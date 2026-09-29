"""Cross-cutting acceptance tests for the local wave seam.

Every other production-core test file owns one module.  These prove the
guarantees that only hold when the modules act together:

* the seam validates local records and never contacts a storage client, so a
  valid PLAN manifest advances a wave whose staged URLs do not exist;
* a wrong manifest leaves ``state.json`` byte-identical, because validation
  happens before any durable write;
* every later consumer revalidates what it downloaded: a size or per-block
  event-count mismatch fails before the shower starts;
* reclaim deletes exactly the recorded references under the run prefix, treats
  a missing object as an idempotent retry, and never invents a sidecar URL;
* the processing chain is MIX -> MINIAOD -> ARCHIVE -> RECLAIM and the terminal
  cleanup wave is prepared before ``.complete`` is written;
* a crash after any durable write replays into exactly the ledger and state an
  uninterrupted run produces, and an inventory that grows by two orders of
  magnitude does not grow ``state.json``.

The fixture is hermetic: staging is XRootD spelling, worker results are
hand-written from the frozen task JSON, and no container or transfer ever runs.
"""
import gzip
import io
import os
from contextlib import ExitStack, contextmanager
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.application import production
from full_mc.application.ledger import (archive_path, group_path, inventory_path,
                                        load_archives, load_groups, load_inventory,
                                        load_wave, sample_key, wave_path)
from full_mc.application.production import (advance_wave, plan_run,
                                            restore_archived_sources)
from full_mc.domain.allocation import allocate_wave
from full_mc.domain.inventory import SourceBlock
from full_mc.io import atomic_json, read_json
from full_mc.storage import archive as storage_archive
from full_mc.storage import reclaim as storage_reclaim
from full_mc.storage.transfer import TransferClient, _remote_parts, materialize_input
from full_mc.workers.chain import execute_chain
from full_mc.workers.reclaim import execute as execute_reclaim
from tests.generate_synthetic_lhe import generate

RELEASE = "acceptance-release"
RUN_ID = "acceptance"
CAMPAIGN = "C"
PROFILE = "run3_2022_legacy_premix_v1"
SAMPLE = sample_key(CAMPAIGN, PROFILE)
STAGING = "root://eosuser.cern.ch///eos/user/c/chiw/staging"
PREFIX = STAGING + "/" + RUN_ID
BLOCKS = 3
BLOCK_EVENTS = 5
SOURCE_EVENTS = BLOCKS * BLOCK_EVENTS
STATE_KEYS = {"state_schema", "status", "next_step", "pending", "pool_cursors",
              "job_offsets", "merge_buffers", "merge_group_counts"}


def transfer_operations():
    """Every public TransferClient operation, discovered rather than listed."""
    return sorted(name for name in dir(TransferClient)
                  if not name.startswith("_") and callable(getattr(TransferClient, name)))


def remote_io_blocked():
    """Every TransferClient operation raises; the seam must not call any of them."""
    stack = ExitStack()
    for name in transfer_operations():
        stack.enter_context(patch.object(
            TransferClient, name,
            side_effect=AssertionError("the seam touched a storage client")))
    return stack


@contextmanager
def no_container_launch():
    """A utilities container launch is a remote operation by another name."""
    with patch("full_mc.runtime.profiles.run_profile",
               side_effect=AssertionError("the seam launched a container")), \
            patch("full_mc.storage.reclaim.run_profile",
                  side_effect=AssertionError("the seam launched a container")):
        yield


def offline_seam():
    """No transfer client, no container: the seam is local-only."""
    stack = ExitStack()
    stack.enter_context(remote_io_blocked())
    stack.enter_context(no_container_launch())
    return stack


class FoldCrash(Exception):
    """A simulated loss of the submit host at a durable-write boundary."""


@contextmanager
def fold_crash(run, target):
    """Crash one wave fold at ``target``, after the write it names has landed.

    ``target`` is one of ``("record", relative_path)`` after that immutable
    record, ``("receipt",)`` after the wave receipt, ``("cleanup",)`` after the
    raw result fragments are removed, or ``("commit",)`` after ``state.json``.
    Records are named relative to the run root because the crash target is
    computed once and replayed for several temporary runs.
    """
    run = Path(run)
    real = {name: getattr(production, name) for name in
            ("write_inventory", "write_archive", "write_group", "write_wave",
             "atomic_json")}

    def record(name):
        def wrapper(*args, **kwargs):
            written = Path(real[name](*args, **kwargs))
            if target[0] == "record" and written == run / target[1]:
                raise FoldCrash(str(written))
            return written
        return wrapper

    def receipt(*args, **kwargs):
        written = real["write_wave"](*args, **kwargs)
        if target[0] == "receipt":
            raise FoldCrash(str(written))
        return written

    def commit(path, value):
        if Path(path).name == "state.json":
            if target[0] == "cleanup":
                for task_id in read_json(run / "state.json")["pending"]["task_ids"]:
                    if (run / "results" / (task_id + ".json")).exists():
                        raise AssertionError("raw results survived into the state commit")
                raise FoldCrash("cleanup")
            if target[0] == "commit":
                real["atomic_json"](path, value)
                raise FoldCrash("commit")
        return real["atomic_json"](path, value)

    with patch.object(production, "write_inventory", record("write_inventory")), \
            patch.object(production, "write_archive", record("write_archive")), \
            patch.object(production, "write_group", record("write_group")), \
            patch.object(production, "write_wave", receipt), \
            patch.object(production, "atomic_json", commit):
        yield


def snapshot(base):
    """Every durable file of one run, with its own base directory folded out.

    Two runs built from the same fixture differ only by where the temporary
    directory landed, so a normalized snapshot is an exact replay comparison.
    """
    base = Path(base).resolve()
    files = {}
    for path in sorted(base.rglob("*")):
        if not path.is_file() or str(path.relative_to(base)).startswith("run/logs/"):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        files[str(path.relative_to(base))] = text.replace(str(base), "<BASE>")
    return files


class AcceptanceFixture(unittest.TestCase):
    """One hermetic run whose staging is XRootD spelling and whose workers are simulated."""

    def fixture(self, base, *, blocks=BLOCKS, block_events=BLOCK_EVENTS,
                source_events=None, staging_bytes=100000):
        base = Path(base)
        base.mkdir(parents=True, exist_ok=True)
        source_events = blocks * block_events if source_events is None else source_events
        source = base / "input" / "input.lhe"
        source.parent.mkdir(parents=True, exist_ok=True)
        # The seam reads the counted event count from the run spec, never the
        # file, so the synthetic source only has to exist and be nonempty.
        generate(str(source), min(source_events, 20))
        runtime = base / "runtime"
        (runtime / "controller/core/src/full_mc/runtime").mkdir(parents=True)
        (runtime / "controller/core/mcprod").write_text("#!/bin/sh\n", encoding="utf-8")
        (runtime / "controller/core/src/full_mc/runtime/bootstrap.py").write_text(
            "# bootstrap\n", encoding="utf-8")
        for name in ("core.tgz", "utilities.tgz", "pythia.tgz", "cmssw12.tgz"):
            (runtime / name).write_bytes(b"bundle")
        atomic_json(runtime / "runtime.json", {
            "runtime_release": RELEASE, "site": "cern", "git_revision": "deadbeef",
            "source_dirty": False,
            "profiles": ["utilities-el9", "pythia-el8", "cmssw12-sim-el8"],
            "bundles": {"core": "core.tgz", "utilities-el9": "utilities.tgz",
                        "pythia-el8": "pythia.tgz", "cmssw12-sim-el8": "cmssw12.tgz"}})
        spec = {
            "schema_version": 2, "run_id": RUN_ID, "runtime_release": RELEASE,
            "runtime_dir": str(runtime), "block_events": block_events,
            "storage": {"cern_staging": STAGING,
                        "ihep_products": "root://eosuser.cern.ch///eos/user/c/chiw/products",
                        "ihep_archive": "root://eosuser.cern.ch///eos/user/c/chiw/archive",
                        "staging_bytes": staging_bytes, "reserve_bytes": 1000},
            "limits": {"source_lhe_bytes": 1000, "source_block_bytes": 1000,
                       "mixed_hepmc_bytes": 1000, "source_files_per_step": 1,
                       "processing_nodes_per_wave": 1},
            "scheduler": {"maxjobs": {"lhe": 3, "lhe_planning": 3,
                                      "block_processing": 2, "processing": 2,
                                      "lhe_coordination": 1}},
            "sources": [{"pool": "pool", "source_id": "s1", "seed": 19,
                         "url": str(source), "actual_events": source_events,
                         "size_bytes": source.stat().st_size,
                         "planned_events": source_events}],
            "campaigns": [{"name": CAMPAIGN, "analysis": "JJP", "target_mixed_events": 1,
                           "max_processing_nodes": 1, "merge_events": 1,
                           "detector_variants": [PROFILE],
                           "sources": [{"pool": "pool", "mode": "normal", "lhe_budget": 5},
                                       {"pool": "pool", "mode": "normal", "lhe_budget": 5}]}]}
        spec_path = base / "spec.json"
        atomic_json(spec_path, spec)
        return spec_path, base / "run"

    def plan_result(self, run, *, blocks=BLOCKS, block_events=BLOCK_EVENTS,
                    source_events=None):
        """A valid PLAN manifest whose staged URLs were never materialized here."""
        source_events = blocks * block_events if source_events is None else source_events
        pending = read_json(run / "state.json")["pending"]
        plan_id = pending["records"][0]["plan_task"]
        task = read_json(run / "tasks" / (plan_id + ".json"))
        directory = str(task["parameters"]["block_output_dir"]).rstrip("/")
        original = dict(task["parameters"]["original_output"],
                        events=source_events, size_bytes=900)
        outputs = [{"artifact_id": "BLOCK:pool:s1:%06d" % index, "kind": "lhe-block",
                    "events": block_events, "size_bytes": 900,
                    "url": directory + "/block%d.lhe.gz" % index}
                   for index in range(blocks)]
        atomic_json(run / "results" / (plan_id + ".json"), {
            "task_id": plan_id, "stage": "plan", "status": "ok", "runtime_release": RELEASE,
            "outputs": outputs, "originals": [original],
            "planned_events": blocks * block_events, "actual_input_events": source_events})
        return plan_id

    def plan_result_path(self, run):
        pending = read_json(run / "state.json")["pending"]
        return run / "results" / (pending["records"][0]["plan_task"] + ".json")

    def processing_results(self, run):
        """A component MiniAOD per detector task plus one archive receipt."""
        pending = read_json(run / "state.json")["pending"]
        archive = read_json(run / "tasks" / (pending["archive_task"] + ".json"))
        for item in pending["miniaods"]:
            task = read_json(run / "tasks" / (item["task_id"] + ".json"))
            output = dict(task["outputs"][0], events=1, count_source="edmFileUtil")
            atomic_json(run / "results" / (item["task_id"] + ".json"), {
                "task_id": item["task_id"], "stage": "miniaod", "status": "ok",
                "runtime_release": RELEASE, "outputs": [output]})
        members = []
        for index, ref in enumerate(archive["inputs"]):
            member = {"artifact_id": ref["artifact_id"],
                      "member": "%06d_%s" % (index, Path(ref["url"]).name),
                      "url": ref["url"], "kind": ref["kind"],
                      "size_bytes": int(ref.get("size_bytes") or 900)}
            if "events" in ref:
                member["events"] = int(ref["events"])
            members.append(member)
        atomic_json(run / "results" / (pending["archive_task"] + ".json"), {
            "task_id": pending["archive_task"], "stage": "archive", "status": "ok",
            "runtime_release": RELEASE, "outputs": [dict(archive["outputs"][0])],
            "members": members})
        return pending

    def reclaim_result(self, run, *, drop=None, disposition="removed"):
        """The reclaim worker envelope for the wave the state is currently holding."""
        pending = read_json(run / "state.json")["pending"]
        task = read_json(run / "tasks" / (pending["reclaim_task"] + ".json"))
        entries = []
        for ref in task["inputs"]:
            if ref["artifact_id"] == drop:
                continue
            objects = [{"kind": "payload", "url": ref["url"], "disposition": disposition}]
            if "sidecar_url" in ref:
                objects.append({"kind": "sidecar", "url": ref["sidecar_url"],
                                "disposition": "already-missing"})
            entries.append({"artifact_id": ref["artifact_id"], "objects": objects})
        return {"task_id": task["task_id"], "stage": "reclaim", "status": "ok",
                "runtime_release": RELEASE, "outputs": [], "reclaimed": entries}

    def write_reclaim_result(self, run, **kwargs):
        outcome = self.reclaim_result(run, **kwargs)
        atomic_json(run / "results" / (outcome["task_id"] + ".json"), outcome)
        return outcome

    def drive(self, base, *, blocks=BLOCKS, block_events=BLOCK_EVENTS,
              source_events=None, staging_bytes=100000, crash=None, observe=None):
        """The whole lifecycle; ``crash`` names one fold write to lose the host at."""
        spec, run = self.fixture(base, blocks=blocks, block_events=block_events,
                                 source_events=source_events,
                                 staging_bytes=staging_bytes)
        plan_run(spec, run)
        self.plan_result(run, blocks=blocks, block_events=block_events,
                         source_events=source_events)
        if crash and crash[0] == "sources":
            self.lose_process(run, 1, crash[1])
            if observe:
                observe(run, crash[0], crash[1])
        advance_wave(run, 1)
        self.processing_results(run)
        # A processing wave is only consumed once its own reclaim result is
        # present, so the cursors it gates stay put until the deletion is real.
        self.write_reclaim_result(run)
        if crash and crash[0] == "processing":
            self.lose_process(run, 2, crash[1])
            if observe:
                observe(run, crash[0], crash[1])
        self.assertEqual(advance_wave(run, 2)["phase"], "reclaim")
        self.write_reclaim_result(run)
        self.assertEqual(advance_wave(run, 3)["status"], "complete")
        return run

    def lose_process(self, run, step, target):
        with fold_crash(run, target):
            with self.assertRaises(FoldCrash):
                advance_wave(run, step)


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class SeamLocality(AcceptanceFixture):
    def test_valid_plan_advances_when_its_test_urls_do_not_exist(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            with offline_seam():
                spec, run = self.fixture(base)
                plan_run(spec, run)
                self.plan_result(run)
                # Nothing under the staging prefix was ever created; the seam
                # still accepts the manifest because the URLs stay opaque.
                processing = advance_wave(run, 1)
            self.assertEqual(processing["phase"], "processing")
            state = read_json(run / "state.json")
            self.assertEqual(state["next_step"], 1)
            self.assertEqual(state["pending"]["phase"], "processing")
            record = read_json(inventory_path(run, "pool", "s1"))
            self.assertEqual(record["accepted_step"], 0)
            self.assertEqual(len(record["producer_result"]["outputs"]), BLOCKS)
            self.assertEqual(record["producer_result"]["originals"][0]["events"],
                             SOURCE_EVENTS)

    def test_no_transfer_operation_is_reachable_from_a_whole_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            # plan_run, both wave folds, and the terminal reclaim all happen
            # with every public transfer operation raising.
            with offline_seam():
                run = self.drive(Path(temporary))
            state = read_json(run / "state.json")
            self.assertEqual(state["status"], "complete")
            self.assertEqual(state["next_step"], 3)
            self.assertEqual(state["pool_cursors"], {"pool": 2})
            self.assertEqual(state["merge_group_counts"], {SAMPLE: 1})
            self.assertTrue((run / ".complete").is_file())


class LocalStructuralValidation(AcceptanceFixture):
    """Every structural disagreement is found before the first durable write."""

    def refuse(self, mutate, pattern):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            spec, run = self.fixture(base)
            plan_run(spec, run)
            self.plan_result(run)
            before = (run / "state.json").read_bytes()
            result_path = self.plan_result_path(run)
            result = read_json(result_path)
            mutate(result)
            atomic_json(result_path, result)
            with self.assertRaisesRegex(RuntimeError, pattern):
                advance_wave(run, 1)
            self.assertEqual((run / "state.json").read_bytes(), before)
            self.assertEqual(read_json(run / "state.json")["next_step"], 0)
            self.assertFalse((run / "ledger").exists())

    def test_wrong_task_metadata_leaves_state_unchanged(self):
        self.refuse(lambda result: result.update(task_id="PLAN_999999"),
                    "wrong task ID")
        self.refuse(lambda result: result.update(runtime_release="other-release"),
                    "wrong runtime release")

    def test_wrong_block_ids_leave_state_unchanged(self):
        self.refuse(lambda result: result["outputs"][1].update(
            artifact_id="BLOCK:pool:other:000001"), "identities")

    def test_wrong_url_prefix_leaves_state_unchanged(self):
        def mutate(result):
            result["outputs"][2]["url"] = (
                "root://eosuser.cern.ch///eos/user/c/chiw/elsewhere/block2.lhe.gz")
        self.refuse(mutate, "outside its output directory")

    def test_wrong_block_order_leaves_state_unchanged(self):
        def mutate(result):
            result["outputs"][0], result["outputs"][1] = (
                result["outputs"][1], result["outputs"][0])
        self.refuse(mutate, "identities")

    def test_wrong_per_block_counts_leave_state_unchanged(self):
        self.refuse(lambda result: result["outputs"][1].update(events=BLOCK_EVENTS - 1),
                    "only the final planner block may be partial")

    def test_wrong_declared_sizes_leave_state_unchanged(self):
        self.refuse(lambda result: result["outputs"][0].update(size_bytes=0),
                    "no declared byte size")


class DownloadingClient:
    """A TransferClient stand-in that copies one prepared local download."""

    def __init__(self, source):
        self.source = Path(source)

    def copy_in(self, url, destination):
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.source, destination)
        return destination


class ConsumptionTimeValidation(AcceptanceFixture):
    """A producer manifest is not proof that a later copy is intact."""

    def mix_task(self, root, references, *, rng_seed=7, mixing_rng_seed=11):
        return {"task_id": "MIX_C_JOB000000_BLOCK000000", "stage": "mix",
                "runtime": "pythia-el8", "runtime_release": RELEASE,
                "inputs": references,
                "parameters": {"analysis": "JJP", "campaign": CAMPAIGN,
                               "sources": [{"slot": 0, "mode": "normal",
                                            "inputs": [ref["artifact_id"]
                                                       for ref in references],
                                            "rng_seed": rng_seed,
                                            "max_lhe_events": 10,
                                            "target_hepmc_events": 1}],
                               "mixing_rng_seed": mixing_rng_seed}}

    def test_downloaded_size_mismatch_removes_the_copy_and_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            download = root / "downloaded.lhe.gz"
            with gzip.open(download, "wb") as stream:
                stream.write(b"<LesHouchesEvents>\n</LesHouchesEvents>\n")
            reference = {"artifact_id": "blk",
                         "url": "root://eosuser.cern.ch///eos/x/block.lhe.gz",
                         "kind": "lhe-block", "events": 1,
                         "size_bytes": download.stat().st_size + 5}
            work = root / "work"
            client = lambda: DownloadingClient(download)
            with patch("full_mc.storage.transfer.TransferClient", client):
                with self.assertRaisesRegex(RuntimeError,
                                            "downloaded size disagrees with blk"):
                    materialize_input(reference, work)
            self.assertFalse((work / "inputs" / "blk" / "block.lhe.gz").exists())

    def test_size_mismatch_stops_the_mix_chain_before_any_shower(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            download = root / "downloaded.lhe.gz"
            generate(str(download), 5)
            reference = {"artifact_id": "blk",
                         "url": "root://eosuser.cern.ch///eos/x/block.lhe.gz",
                         "kind": "lhe-block", "events": 5,
                         "size_bytes": download.stat().st_size + 5}
            work = root / "work"
            client = lambda: DownloadingClient(download)
            with patch("full_mc.storage.transfer.TransferClient", client), \
                    patch("full_mc.workers.chain.native_tree") as tree, \
                    patch("full_mc.workers.chain.run") as run:
                with self.assertRaisesRegex(RuntimeError,
                                            "downloaded size disagrees with blk"):
                    execute_chain(self.mix_task(root, [reference]), work)
            self.assertFalse(run.called)
            self.assertFalse(tree.called)
            self.assertFalse((work / "chain.json").exists())
            self.assertFalse((work / "inputs" / "blk" / "block.lhe.gz").exists())

    def test_per_block_event_count_mismatch_fails_before_showering(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            blocks = []
            for index, (counted, declared) in enumerate(((5, 5), (5, 6))):
                path = root / ("block%d.lhe" % index)
                generate(str(path), counted)
                blocks.append({"artifact_id": "BLOCK:pool:s1:%06d" % index,
                               "url": str(path), "kind": "lhe-block",
                               "events": declared})
            work = root / "work"
            with patch("full_mc.workers.chain.native_tree") as tree, \
                    patch("full_mc.workers.chain.run") as run:
                with self.assertRaisesRegex(
                        RuntimeError, "BLOCK:pool:s1:000001: read 5 LHE events, expected 6"):
                    execute_chain(self.mix_task(root, blocks), work)
            self.assertFalse(run.called)
            self.assertFalse(tree.called)
            self.assertFalse((work / "chain.json").exists())


XRDFS_STUB = """#!/bin/bash
lfn="$3"
if grep -qxF "$lfn" "$RECLAIM_MISSING"; then
    echo "[ERROR] Server responded with an error: [3011] Unable to open file $lfn" >&2
    exit 1
fi
if grep -qxF "$lfn" "$RECLAIM_FATAL"; then
    echo "[ERROR] Server responded with an error: [3010] Permission denied" >&2
    exit 1
fi
exit 0
"""


class ReclaimAcceptance(unittest.TestCase):
    """Deletion happens in a worker, against exactly the recorded references."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.work = self.root / "work"
        binary = self.root / "bin" / "xrdfs"
        binary.parent.mkdir(parents=True)
        binary.write_text(XRDFS_STUB, encoding="utf-8")
        binary.chmod(0o755)
        self.binary = binary.parent
        self.missing = self.root / "missing"
        self.fatal = self.root / "fatal"

    def tearDown(self):
        self.temporary.cleanup()

    def task(self, references):
        return {"task_id": "RECLAIM_000001", "stage": "reclaim",
                "runtime": "utilities-el9", "runtime_release": RELEASE,
                "inputs": references, "outputs": [],
                "parameters": {"staging_prefix": PREFIX}}

    def reference(self, identity, name, *, sidecar=False):
        ref = {"artifact_id": identity, "kind": "lhe-block",
               "url": PREFIX + "/blocks/" + name}
        if sidecar:
            ref["sidecar_url"] = ref["url"] + ".json"
        return ref

    def execute(self, references, *, missing=()):
        """Run the reclaim worker against a stub xrdfs, never a container."""
        self.missing.write_text("\n".join(missing) + ("\n" if missing else ""),
                                encoding="utf-8")
        self.fatal.write_text("", encoding="utf-8")
        launches = []

        def fake_run_profile(profile, command, **kwargs):
            self.assertEqual(profile, "utilities-el9")
            launches.append(command)
            environment = dict(
                os.environ,
                PATH=str(self.binary) + ":" + os.environ.get("PATH", ""),
                RECLAIM_MISSING=str(self.missing), RECLAIM_FATAL=str(self.fatal))
            return subprocess.run(command, env=environment, **kwargs)

        with patch.object(storage_reclaim, "run_profile", side_effect=fake_run_profile):
            outcome = execute_reclaim(self.task(references), self.work)
        return outcome, launches

    def test_reclaim_rejects_a_path_outside_the_run_prefix(self):
        outside = {"artifact_id": "A", "kind": "lhe-block",
                   "url": "root://eosuser.cern.ch///eos/user/c/chiw/other/block.lhe.gz"}
        with patch.object(storage_reclaim, "run_profile") as run:
            with self.assertRaisesRegex(ValueError, "refusing to reclaim outside run staging"):
                execute_reclaim(self.task([outside]), self.work)
        run.assert_not_called()

    def test_missing_payload_and_sidecar_are_successful_retries(self):
        ref = self.reference("B1", "block_000000.lhe.gz", sidecar=True)
        outcome, launches = self.execute(
            [ref], missing=[_remote_parts(ref["url"])[1],
                            _remote_parts(ref["sidecar_url"])[1]])
        self.assertEqual(len(launches), 1)
        self.assertEqual(outcome["status"], "ok")
        objects = outcome["reclaimed"][0]["objects"]
        self.assertEqual([item["kind"] for item in objects], ["payload", "sidecar"])
        self.assertEqual([item["disposition"] for item in objects],
                         ["already-missing", "already-missing"])

    def test_absent_sidecar_never_manufactures_a_deletion(self):
        ref = self.reference("C1", "block_000001.lhe.gz")
        outcome, launches = self.execute([ref])
        self.assertEqual(outcome["reclaimed"][0]["objects"],
                         [{"kind": "payload", "url": ref["url"],
                           "disposition": "removed"}])
        command = launches[0]
        script = command[2] if command[1] == "-c" else Path(command[1]).read_text()
        self.assertNotIn(".json", script)
        self.assertEqual(script.count("remove "), 1)


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class ProcessingChainAndCleanup(AcceptanceFixture):
    def test_processing_dag_chains_mix_miniaod_archive_reclaim(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            spec, run = self.fixture(base)
            plan_run(spec, run)
            self.plan_result(run)
            pending = advance_wave(run, 1)
            self.assertEqual(pending["phase"], "processing")
            state = read_json(run / "state.json")["pending"]
            dag = Path(state["dag"]).read_text()
            mix_id = state["miniaods"][0]["mix_task_id"]
            mini_id = state["miniaods"][0]["task_id"]
            self.assertIn("PARENT %s CHILD %s\n" % (mix_id, mini_id), dag)
            self.assertIn("PARENT %s CHILD %s\n" % (mini_id, state["archive_task"]), dag)
            self.assertIn("PARENT %s CHILD %s\n" % (state["archive_task"],
                                                    state["reclaim_task"]), dag)
            order = [dag.index("JOB " + name) for name in
                     (mix_id, mini_id, state["archive_task"], state["reclaim_task"])]
            self.assertEqual(order, sorted(order))

    def test_terminal_cleanup_wave_is_prepared_before_complete_is_written(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.drive(Path(temporary) / "uninterrupted")
            self.assertTrue((run / ".complete").is_file())
            state = read_json(run / "state.json")
            self.assertEqual(state["next_step"], 3)
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            spec, run = self.fixture(base)
            plan_run(spec, run)
            self.plan_result(run)
            advance_wave(run, 1)
            self.processing_results(run)
            self.write_reclaim_result(run)
            prepared = advance_wave(run, 2)
            # Completion is committed only after the spare slot's reclaim wave
            # has folded, so the sentinel cannot appear ahead of the deletion.
            self.assertEqual(prepared["phase"], "reclaim")
            self.assertFalse((run / ".complete").exists())
            state = read_json(run / "state.json")
            self.assertEqual(state["status"], "running")
            self.assertEqual(state["next_step"], 2)
            self.assertEqual(state["pending"]["step"], 2)
            task = read_json(run / "tasks" / (state["pending"]["reclaim_task"] + ".json"))
            self.assertEqual([ref["artifact_id"] for ref in task["inputs"]],
                             ["BLOCK:pool:s1:000002"])
            self.assertEqual(task["parameters"], {"staging_prefix": PREFIX})
            self.assertTrue(Path(state["pending"]["dag"]).is_file())

            self.write_reclaim_result(run)
            done = advance_wave(run, 3)
            self.assertEqual(done["status"], "complete")
            self.assertTrue((run / ".complete").is_file())
            self.assertEqual(read_json(run / "state.json")["pending"], {})

    def test_already_missing_reclaim_still_advances_the_cursors(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            spec, run = self.fixture(base)
            plan_run(spec, run)
            self.plan_result(run)
            advance_wave(run, 1)
            self.processing_results(run)
            self.write_reclaim_result(run, disposition="already-missing")
            self.assertEqual(advance_wave(run, 2)["phase"], "reclaim")
            self.assertEqual(read_json(run / "state.json")["pool_cursors"], {"pool": 2})


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class CrashSafety(AcceptanceFixture):
    """A lost submit host must replay into the uninterrupted run's artefacts."""

    def crash_targets(self, run):
        """One target per durable write the two folds make, run-relative."""
        relative = lambda path: str(Path(path).relative_to(run))
        return {
            "sources": [("record", relative(inventory_path(run, "pool", "s1"))),
                        ("receipt",), ("cleanup",), ("commit",)],
            "processing": [("record", relative(archive_path(run, 1))),
                           ("record", relative(group_path(run, CAMPAIGN, PROFILE, 0))),
                           ("receipt",), ("cleanup",), ("commit",)],
        }

    def consumed_task_ids(self, run, phase):
        """The worker results one fold reads and removes, not every wave task."""
        pending = read_json(run / "state.json")["pending"]
        if phase == "sources":
            return [item["plan_task"] for item in pending["records"]]
        identifiers = [pending["archive_task"]] + [item["task_id"]
                                                   for item in pending["miniaods"]]
        if pending.get("reclaim_task"):
            identifiers.append(pending["reclaim_task"])
        return identifiers

    def observe(self, run, phase, target):
        """The record the crash landed on is durable but not yet committed."""
        spec = read_json(run / "run.json")
        state = read_json(run / "state.json")
        step = 0 if phase == "sources" else 1
        if target[0] == "record":
            written = run / target[1]
            self.assertTrue(written.exists())
            if written == inventory_path(run, "pool", "s1"):
                # Readers, not writers, decide committedness.
                self.assertEqual(load_inventory(run, spec, state["next_step"]), [])
                self.assertEqual(state["next_step"], 0)
            elif written == archive_path(run, 1):
                self.assertEqual(load_archives(run, spec, state["next_step"]), [])
                self.assertEqual(state["next_step"], 1)
            else:
                self.assertEqual(load_groups(run, spec, state["merge_group_counts"]), [])
                self.assertEqual(state["next_step"], 1)
        if target[0] == "receipt":
            self.assertTrue(wave_path(run, step).exists())
            self.assertEqual(load_wave(run, step)["pending"]["step"], step)
            self.assertEqual(state["next_step"], step)
            # The receipt precedes cleanup, so every consumed fragment is still
            # on disk and a retry can read either copy.
            for task_id in self.consumed_task_ids(run, phase):
                self.assertTrue((run / "results" / (task_id + ".json")).exists())
        if target[0] == "cleanup":
            self.assertEqual(state["next_step"], step)
            for task_id in self.consumed_task_ids(run, phase):
                self.assertFalse((run / "results" / (task_id + ".json")).exists())
        if target[0] == "commit":
            self.assertEqual(state["next_step"], step + 1)

    def test_every_durable_write_replays_without_duplicating_or_losing_a_transition(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            reference = self.drive(base / "reference")
            expected = snapshot(base / "reference")
            self.assertEqual(read_json(reference / "state.json")["next_step"], 3)
            self.assertTrue((reference / ".complete").is_file())
            targets = self.crash_targets(self.fixture(base / "probe")[1])
            for phase in ("sources", "processing"):
                for target in targets[phase]:
                    with self.subTest(phase=phase, target=target):
                        with tempfile.TemporaryDirectory() as case:
                            run = self.drive(
                                Path(case) / "crashed", crash=(phase, target),
                                observe=lambda crashed, phase, target:
                                self.observe_interrupted(crashed, phase, target,
                                                         expected))
                            self.assertEqual(snapshot(Path(case) / "crashed"), expected)
                            self.assertTrue((run / ".complete").is_file())

    def observe_interrupted(self, run, phase, target, expected):
        """The crash left a real gap the retry has to close, and nothing else."""
        self.observe(run, phase, target)
        self.assertNotEqual(snapshot(run.parent), expected,
                            "the crash did not interrupt the fold")

    def test_retried_fold_keeps_every_transition_exactly_once(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            spec, run = self.fixture(base)
            plan_run(spec, run)
            self.plan_result(run)
            self.lose_process(run, 1, ("commit",))
            self.assertEqual(advance_wave(run, 1)["phase"], "processing")
            # A committed fold replays as a no-op, not as a second transition.
            self.assertEqual(read_json(run / "state.json")["next_step"], 1)
            self.assertEqual(len(load_inventory(run, read_json(run / "run.json"), 1)), 1)
            self.processing_results(run)
            self.write_reclaim_result(run)
            self.lose_process(run, 2, ("cleanup",))
            self.assertEqual(advance_wave(run, 2)["phase"], "reclaim")
            self.write_reclaim_result(run)
            self.assertEqual(advance_wave(run, 3)["status"], "complete")
            state = read_json(run / "state.json")
            self.assertEqual(state["next_step"], 3)
            self.assertEqual(state["merge_group_counts"], {SAMPLE: 1})
            spec_payload = read_json(run / "run.json")
            self.assertEqual(len(load_archives(run, spec_payload, 3)), 1)
            self.assertEqual(len(load_groups(run, spec_payload,
                                             state["merge_group_counts"])), 1)
            self.assertFalse((run / "merge_groups.json").exists())


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class InventoryScaling(AcceptanceFixture):
    """Block count must not reach state.json, and must not move the allocation."""

    def allocated(self, base, *, blocks):
        # Every block is a byte, so the ceiling only has to cover 50k of them;
        # the point of the test is the record shape, not the byte budget.
        spec, run = self.fixture(base, blocks=blocks, block_events=1,
                                 source_events=blocks, staging_bytes=10 ** 9)
        plan_run(spec, run)
        self.plan_result(run, blocks=blocks, block_events=1, source_events=blocks)
        self.assertEqual(advance_wave(run, 1)["phase"], "processing")
        return run

    def reference_blocks(self, count):
        return [SourceBlock("BLOCK:pool:s1:%06d" % index, "pool", "s1", index, 1, "url")
                for index in range(count)]

    def test_50k_blocks_allocate_identically_and_keep_state_bounded(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            small = self.allocated(base / "small", blocks=500)
            large = self.allocated(base / "large", blocks=50000)
            small_state = read_json(small / "state.json")
            large_state = read_json(large / "state.json")
            # The allocation is the domain allocator's, unchanged, and it reads
            # only the cursors: 100x more blocks cannot move the first wave.
            campaigns = production._campaigns(read_json(small / "run.json"))
            expected = allocate_wave(campaigns, {"pool": self.reference_blocks(500)},
                                     {}, 1, {})
            self.assertEqual(small_state["pending"]["allocation"], expected)
            self.assertEqual(large_state["pending"]["allocation"],
                             expected)
            self.assertEqual(
                [(node["job_id"], slot["source_rng_seed"], node["mixing_rng_seed"])
                 for node in small_state["pending"]["allocation"]["campaigns"][0]["nodes"]
                 for slot in node["sources"]],
                [(node["job_id"], slot["source_rng_seed"], node["mixing_rng_seed"])
                 for node in large_state["pending"]["allocation"]["campaigns"][0]["nodes"]
                 for slot in node["sources"]])
            # state.json holds cursors and the pending wave, never the inventory:
            # 100x the blocks land in one immutable 9 MB ledger record instead.
            small_size = (small / "state.json").stat().st_size
            large_size = (large / "state.json").stat().st_size
            self.assertLessEqual(abs(small_size - large_size), 64)
            self.assertLess(large_size, 16 * 1024)
            self.assertNotIn("BLOCK:pool:s1:004999",
                             (large / "state.json").read_text())
            self.assertEqual(set(large_state), STATE_KEYS)
            stored = read_json(inventory_path(large, "pool", "s1"))
            self.assertEqual(len(stored["producer_result"]["outputs"]), 50000)


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class ArchiveRestoration(AcceptanceFixture):
    def test_restoration_ignores_uncommitted_archive_records(self):
        def make_tar(name, payload):
            """A one-member archive, so restoration reads real tar bytes."""
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as archive:
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
            return buffer.getvalue()

        class FakeTransfer:
            """A local stand-in for the recorded archive URL only."""

            def copy_in(self, source, destination):
                Path(destination).parent.mkdir(parents=True, exist_ok=True)
                Path(destination).write_bytes(self.tars[source])
                return Path(destination)

        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            run = self.drive(base)
            committed = load_archives(run, read_json(run / "run.json"), 3)
            self.assertEqual([item["step"] for item in committed], [1])
            record = committed[0]
            members = [item for item in record["members"] if item["kind"] == "lhe"]
            self.assertEqual([item["artifact_id"] for item in members], ["LHE_000000"])
            # A committed receipt restores its own recorded bytes.
            committed_bytes = b"A" * int(members[0]["size_bytes"])
            uncommitted_bytes = b"B" * int(members[0]["size_bytes"])
            fake = FakeTransfer()
            fake.tars = {record["outputs"][0]["url"]:
                         make_tar(members[0]["member"], committed_bytes)}
            # A later receipt that the state has not accepted claims the same
            # member with different bytes; it must stay invisible.
            atomic_json(archive_path(run, 99), {
                "schema": "mcprod-archive-receipt-v1", "step": 99,
                "task_id": "ARCHIVE_000099",
                "outputs": [{"artifact_id": "ARCHIVE_000099", "url": PREFIX + "/wave.t",
                             "kind": "upstream-archive"}],
                "members": [dict(members[0], size_bytes=len(uncommitted_bytes))]})
            fake.tars[PREFIX + "/wave.t"] = make_tar(members[0]["member"],
                                                     uncommitted_bytes)
            self.assertEqual([item["step"] for item in
                              load_archives(run, read_json(run / "run.json"), 3)], [1])
            with patch.object(storage_archive, "TransferClient", lambda: fake):
                with self.assertRaisesRegex(ValueError, "unknown verified archive"):
                    restore_archived_sources(run, "ARCHIVE_000099", run / "uncommitted")
                restored = restore_archived_sources(
                    run, record["outputs"][0]["artifact_id"], run / "recovered")
            self.assertEqual(len(restored["restored"]), 1)
            restored_path = Path(restored["restored"][0]["url"])
            self.assertEqual(restored_path.read_bytes(), committed_bytes)


if __name__ == "__main__":
    unittest.main()
