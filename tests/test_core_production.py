"""Offline lifecycle contracts for the lazy CERN production controller."""
from contextlib import ExitStack
import io
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import production
from full_mc.application.ledger import (archive_path, group_path, inventory_path,
                                        load_archives, load_groups, load_wave, sample_key)
from full_mc.application.production import advance_wave, plan_run, restore_archived_sources
from full_mc.io import atomic_json, read_json
from full_mc.planning.graph import PRE_SCRIPT_TIMEOUT_SECONDS, write_root_dag
from full_mc.storage.transfer import TransferClient
from tests.generate_synthetic_lhe import generate

STATE_KEYS = {"state_schema", "status", "next_step", "pending", "pool_cursors",
              "job_offsets", "merge_buffers", "merge_group_counts"}
REMOTE_METHODS = ("size", "mkdir_parent", "mkdir", "read_json_if_exists",
                  "directory_has_entries", "copy_json_if_absent", "copy_out",
                  "copy_in", "read_json", "remove")
SAMPLE = sample_key("C", "run3_2022_legacy_premix_v1")


def sidecar(ref, *, events=None, count_source=None, expected_events=None, partial=None):
    path = Path(ref["url"])
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(b"root-like-product\n")
    result = dict(ref, size_bytes=path.stat().st_size, status="ok", complete=True,
                  runtime_release="test-release", task_id="fake")
    if events is not None:
        result["events"] = events
    if count_source:
        result["count_source"] = count_source
    if expected_events is not None:
        result["expected_events"] = expected_events
    if partial is not None:
        result["partial"] = partial
    atomic_json(str(path) + ".json", result)
    return result


def remote_io_blocked():
    """Every TransferClient operation raises; the seam must not call any of them."""
    stack = ExitStack()
    for name in REMOTE_METHODS:
        stack.enter_context(patch.object(
            TransferClient, name,
            side_effect=AssertionError("seam touched remote storage")))
    return stack


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class ProductionLifecycle(unittest.TestCase):
    def fixture(self, root):
        source = root / "input" / "input.lhe"
        source.parent.mkdir(parents=True)
        generate(str(source), 15)
        runtime = root / "runtime"
        (runtime / "controller/core/src/full_mc/runtime").mkdir(parents=True)
        (runtime / "controller/core/mcprod").write_text("#!/bin/sh\n", encoding="utf-8")
        (runtime / "controller/core/src/full_mc/runtime/bootstrap.py").write_text("# bootstrap\n", encoding="utf-8")
        for name in ("core.tgz", "utilities.tgz", "pythia.tgz", "cmssw12.tgz"):
            (runtime / name).write_bytes(b"bundle")
        atomic_json(runtime / "runtime.json", {
            "runtime_release": "test-release", "site": "cern", "git_revision": "deadbeef",
            "source_dirty": False,
            "profiles": ["utilities-el9", "pythia-el8", "cmssw12-sim-el8"],
            "bundles": {"core": "core.tgz", "utilities-el9": "utilities.tgz",
                        "pythia-el8": "pythia.tgz", "cmssw12-sim-el8": "cmssw12.tgz"}})
        spec = {
            "schema_version": 2, "run_id": "offline", "runtime_release": "test-release",
            "runtime_dir": str(runtime),
            "block_events": 5,
            "storage": {"cern_staging": "root://eosuser.cern.ch///eos/user/c/chiw/staging",
                        "ihep_products": str(root / "ihep/products"),
                        "ihep_archive": str(root / "ihep/archive"),
                        "staging_bytes": 100000, "reserve_bytes": 1000},
            "limits": {"source_lhe_bytes": 1000, "source_block_bytes": 1000,
                       "mixed_hepmc_bytes": 1000, "source_files_per_step": 1,
                       "processing_nodes_per_wave": 1},
            "scheduler": {"maxjobs": {"lhe": 3, "lhe_planning": 3,
                                        "block_processing": 2, "processing": 2,
                                        "lhe_coordination": 1}},
            "sources": [{"pool": "pool", "source_id": "s1", "seed": 19,
                         "url": str(source), "actual_events": 15,
                         "size_bytes": source.stat().st_size}],
            "campaigns": [{"name": "C", "analysis": "JJP", "target_mixed_events": 1,
                           "max_processing_nodes": 1, "merge_events": 1,
                           "detector_variants": ["run3_2022_legacy_premix_v1"],
                           "sources": [{"pool": "pool", "mode": "normal", "lhe_budget": 5},
                                       {"pool": "pool", "mode": "normal", "lhe_budget": 5}]}]}
        spec_path = root / "spec.json"
        atomic_json(spec_path, spec)
        return spec_path, root / "run"

    def planner_results(self, run, staged=True):
        pending = read_json(run / "state.json")["pending"]
        plan_id = pending["records"][0]["plan_task"]
        task = read_json(run / "tasks" / (plan_id + ".json"))
        original = task["parameters"]["original_output"]
        blocks = []
        # A valid manifest whose XRootD URLs are never materialized here: the
        # seam validates the record, not the remote objects.
        original = dict(original, events=15, size_bytes=999)
        directory = str(task["parameters"]["block_output_dir"]).rstrip("/")
        blocks = [{"artifact_id": "BLOCK:pool:s1:%06d" % index, "kind": "lhe-block",
                   "events": 5, "size_bytes": 999,
                   "url": directory + "/block%d.lhe.gz" % index}
                  for index in range(2)]
        atomic_json(run / "results" / (plan_id + ".json"), {"task_id": plan_id, "stage": "plan",
                    "status": "ok", "runtime_release": "test-release", "outputs": blocks,
                    "originals": [original], "planned_events": 10,
                    "actual_input_events": 15})

    def processing_results(self, run):
        pending = read_json(run / "state.json")["pending"]
        archive_task = read_json(run / "tasks" / (pending["archive_task"] + ".json"))
        for item in pending["miniaods"]:
            task = read_json(run / "tasks" / (item["task_id"] + ".json"))
            output = sidecar(task["outputs"][0], events=1, count_source="edmFileUtil",
                             expected_events=1, partial=False)
            atomic_json(run / "results" / (item["task_id"] + ".json"), {"task_id": item["task_id"],
                        "stage": "miniaod", "status": "ok", "runtime_release": "test-release",
                        "outputs": [output], "partial": False})
        members = []
        for index, ref in enumerate(archive_task["inputs"]):
            member = {"artifact_id": ref["artifact_id"],
                      "member": "%06d_%s" % (index, Path(ref["url"]).name),
                      "url": ref["url"], "kind": ref["kind"],
                      "size_bytes": int(ref.get("size_bytes") or 999)}
            if "events" in ref:
                member["events"] = int(ref["events"])
            elif ref["kind"] == "hepmc":
                member["events"] = 1
            members.append(member)
        outcome = {"task_id": pending["archive_task"], "stage": "archive", "status": "ok",
                   "runtime_release": "test-release", "outputs": [dict(archive_task["outputs"][0])],
                   "members": members}
        archive_path = Path(archive_task["outputs"][0]["url"])
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive_path, "w") as stream:
            for item in members:
                info = tarfile.TarInfo(item["member"])
                info.size = int(item["size_bytes"])
                stream.addfile(info, io.BytesIO(b"x" * info.size))
        atomic_json(run / "results" / (pending["archive_task"] + ".json"), outcome)

    def reclaim_result(self, run):
        pending = read_json(run / "state.json")["pending"]
        task = read_json(run / "tasks" / (pending["reclaim_task"] + ".json"))
        reclaimed = [{"artifact_id": ref["artifact_id"],
                      "objects": [{"kind": "payload", "url": ref["url"],
                                   "disposition": "removed"}]}
                     for ref in task["inputs"]]
        atomic_json(run / "results" / (pending["reclaim_task"] + ".json"), {
            "task_id": pending["reclaim_task"], "stage": "reclaim", "status": "ok",
            "runtime_release": "test-release", "outputs": [], "reclaimed": reclaimed,
        })

    def test_lazy_lifecycle_replay_and_reclaim_are_idempotent(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            planned = plan_run(spec, run)
            self.assertEqual(planned["first_phase"], "sources")
            root_dag = (run / "run.dag").read_text()
            self.assertIn("PRE_SKIP WAVE_000000 99", root_dag)
            self.assertIn("SCRIPT PRE WAVE_000000", root_dag)
            advance_script = (run / "advance.sh").read_text()
            self.assertIn("DAGMan inherits the submitter PATH", advance_script)
            self.assertIn("export PATH=/usr/bin:/bin", advance_script)
            self.assertIn("export X509_USER_PROXY=/afs/cern.ch/user/test/x509_user_proxy",
                          advance_script)
            self.assertIn("export X509_CERT_DIR=/cvmfs/grid.cern.ch/etc/grid-security/certificates",
                          advance_script)
            self.assertNotIn('[[ ! -s "$X509_USER_PROXY"', advance_script)
            self.assertIn(f"exec timeout {PRE_SCRIPT_TIMEOUT_SECONDS} ", advance_script)
            subprocess.run(["bash", "-n", str(run / "advance.sh")], check=True)
            state = read_json(run / "state.json")
            self.assertEqual(set(state), STATE_KEYS)
            self.assertEqual(state["state_schema"], "mcprod-state-v2")
            self.assertEqual(state["merge_group_counts"], {})
            source_dag = Path(state["pending"]["dag"]).read_text()
            self.assertIn("CONFIG " + str(run / "dagman.config"), source_dag)
            self.assertIn("CATEGORY", source_dag)
            self.assertIn("MAXJOBS lhe", source_dag)
            for submit_path in (run / "submit").glob("*.sub"):
                submit = submit_path.read_text()
                self.assertIn("should_transfer_files = YES", submit)
                self.assertIn("x509userproxy = /afs/cern.ch/user/test/x509_user_proxy", submit)
                self.assertNotIn("x509up_u", submit)
            self.assertNotIn("ENV SET X509_USER_PROXY=", root_dag)
            self.assertFalse((run / "submit/merge.sub").exists())
            self.assertTrue((run / "submit/reclaim.sub").exists())
            replay = advance_wave(run, 0)
            self.assertEqual(replay["phase"], "sources")
            self.planner_results(run)
            processing = advance_wave(run, 1)
            self.assertEqual(processing["phase"], "processing")
            process_dag = Path(processing["dag"]).read_text()
            self.assertRegex(process_dag, r"PARENT MIX_[^ ]+ CHILD MINIAOD_[^ ]+")
            self.assertRegex(process_dag, r"PARENT MINIAOD_[^ ]+ CHILD ARCHIVE_")
            pending = read_json(run / "state.json")["pending"]
            self.assertEqual([item["artifact_id"] for item in pending["reclaim"]],
                             ["LHE_000000", "BLOCK:pool:s1:000000", "BLOCK:pool:s1:000001",
                              "HEPMC_C_JOB000000_BLOCK000000"])
            self.processing_results(run)
            self.reclaim_result(run)
            with remote_io_blocked():
                done = advance_wave(run, 2)
            self.assertEqual(done["status"], "complete")
            state = read_json(run / "state.json")
            self.assertEqual(set(state), STATE_KEYS)
            self.assertEqual(state["status"], "complete")
            self.assertEqual(state["next_step"], 2)
            self.assertEqual(state["pool_cursors"], {"pool": 2})
            self.assertEqual(state["merge_buffers"][SAMPLE], [])
            self.assertEqual(state["merge_group_counts"], {SAMPLE: 1})
            # Nothing that grows with the run comes back into state.json; the
            # seam does not materialize an aggregate snapshot.
            self.assertNotIn("terminal_cleanup", state)
            self.assertFalse((run / "merge_groups.json").exists())
            # Committed ledger records, and only those, describe the run.
            inventory = read_json(inventory_path(run, "pool", "s1"))
            self.assertEqual(inventory["accepted_step"], 0)
            self.assertEqual(len(inventory["producer_result"]["outputs"]), 2)
            self.assertEqual([item["step"] for item in
                              load_archives(run, read_json(run / "run.json"), 2)], [1])
            self.assertEqual(load_wave(run, 1)["phase"], "processing")
            self.assertEqual(len(load_groups(run, read_json(run / "run.json"),
                                             state["merge_group_counts"])), 1)
            self.assertTrue(group_path(run, "C", "run3_2022_legacy_premix_v1", 0).exists())
            self.assertFalse((run / "merge_groups.json").exists())
            self.assertTrue((run / ".complete").is_file())
            # Completed replays stay no-ops.
            self.assertEqual(advance_wave(run, 2)["status"], "complete")

    def test_state_commit_is_the_retry_safe_fold_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            plan_run(spec, run)
            self.planner_results(run)
            advance_wave(run, 1)
            self.processing_results(run)
            self.reclaim_result(run)
            commit = production.atomic_json

            def interrupted(path, value):
                if Path(path).name == "state.json":
                    raise RuntimeError("state commit interrupted")
                return commit(path, value)

            with patch("full_mc.application.production.atomic_json", side_effect=interrupted):
                with self.assertRaisesRegex(RuntimeError, "state commit interrupted"):
                    advance_wave(run, 2)
            # The fold reached every immutable write; only the marker is missing.
            failed = read_json(run / "state.json")
            self.assertEqual(failed["next_step"], 1)
            self.assertEqual(failed["pending"]["step"], 1)
            self.assertEqual(len(load_archives(run, read_json(run / "run.json"), 2)), 1)
            pending = failed["pending"]
            for task_id in pending["task_ids"]:
                self.assertFalse((run / "results" / (task_id + ".json")).exists())
            # The wave receipt alone replays the interrupted fold.
            with remote_io_blocked():
                retried = advance_wave(run, 2)
            self.assertEqual(retried["status"], "complete")
            state = read_json(run / "state.json")
            self.assertEqual(state["merge_group_counts"], {SAMPLE: 1})
            self.assertEqual(len(load_groups(run, read_json(run / "run.json"),
                                             state["merge_group_counts"])), 1)
            self.assertFalse((run / "merge_groups.json").exists())

    def test_valid_planner_result_advances_with_unreachable_urls(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            plan_run(spec, run)
            self.planner_results(run, staged=False)
            with remote_io_blocked():
                processing = advance_wave(run, 1)
            self.assertEqual(processing["phase"], "processing")
            inventory = read_json(inventory_path(run, "pool", "s1"))
            self.assertEqual(inventory["accepted_step"], 0)

    def test_prefetch_batches_sources_and_keeps_failed_child_pending(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec_path, run = self.fixture(Path(temporary))
            spec = read_json(spec_path)
            spec["limits"].update(source_selection_mode="prefetch", source_files_per_step=4,
                                  processing_nodes_per_wave=3)
            spec["resources"] = {"archive": {"disk_bytes": 21_000}}
            spec["campaigns"][0]["max_processing_nodes"] = 5
            for index in (1, 2, 3):
                source = dict(spec["sources"][0], source_id=f"s{index + 1}", seed=19 + index)
                spec["sources"].append(source)
            atomic_json(spec_path, spec)

            planned = plan_run(spec_path, run)
            self.assertEqual(planned["lazy_steps"], 5)
            self.assertEqual((run / "run.dag").read_text().count("SUBDAG EXTERNAL"), 5)
            pending = read_json(run / "state.json")["pending"]
            self.assertEqual([item["source_index"] for item in pending["records"]], [0, 1, 2, 3])
            self.assertEqual(Path(pending["dag"]).read_text().count("CATEGORY PLAN_"), 4)
            before = (run / "state.json").read_bytes()
            self.assertEqual(advance_wave(run, 0)["status"], "prepared")
            self.assertEqual((run / "state.json").read_bytes(), before)

            for item in pending["records"][:3]:
                index = item["source_index"]
                task = read_json(run / "tasks" / (item["plan_task"] + ".json"))
                original = dict(task["parameters"]["original_output"], events=15,
                                size_bytes=999)
                output_dir = task["parameters"]["block_output_dir"]
                blocks = [{"artifact_id": f"BLOCK:pool:s{index + 1}:{block:06d}",
                           "kind": "lhe-block", "events": 5, "size_bytes": 900,
                           "url": f"{output_dir}/block{block}.lhe.gz"}
                          for block in range(3)]
                atomic_json(run / "results" / (item["plan_task"] + ".json"), {
                    "task_id": item["plan_task"], "stage": "plan", "status": "ok",
                    "runtime_release": "test-release", "outputs": blocks,
                    "originals": [original], "planned_events": 15,
                    "actual_input_events": 15})
            with self.assertRaisesRegex(RuntimeError, "wave results are incomplete"):
                advance_wave(run, 1)
            self.assertEqual((run / "state.json").read_bytes(), before)
            self.assertFalse((run / "ledger").exists())

    def test_existing_specs_keep_demand_driven_source_selection(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec_path, run = self.fixture(Path(temporary))
            spec = read_json(spec_path)
            spec["limits"]["source_files_per_step"] = 2
            spec["sources"].append(dict(spec["sources"][0], source_id="s2", seed=20))
            atomic_json(spec_path, spec)
            plan_run(spec_path, run)
            self.assertEqual(read_json(run / "run.json")["limits"]["source_selection_mode"],
                             "demand")
            self.assertEqual([item["source_index"] for item in
                              read_json(run / "state.json")["pending"]["records"]], [0])

    def test_archive_disk_caps_processing_and_counts_archive_inputs_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec_path, run = self.fixture(Path(temporary))
            spec = read_json(spec_path)
            spec["resources"] = {"archive": {"disk_bytes": 6_000}}
            atomic_json(spec_path, spec)
            plan_run(spec_path, run)
            self.planner_results(run)
            processing = advance_wave(run, 1)
            archive_id = read_json(run / "state.json")["pending"]["archive_task"]
            archive = read_json(run / "tasks" / (archive_id + ".json"))
            self.assertEqual(processing["phase"], "processing")
            self.assertEqual(archive["parameters"]["scratch_bytes"], 5_997)

    def test_pilot_byte_ceilings_allow_five_root_slots_without_disk_probe(self):
        spec = {"sources": [{} for _ in range(200)],
                "limits": {"source_selection_mode": "prefetch",
                           "source_files_per_step": 200,
                           "processing_nodes_per_wave": 1_000,
                           "source_lhe_bytes": 100_000_000,
                           "source_block_bytes": 100_000_000,
                           "mixed_hepmc_bytes": 64_000_000},
                "storage": {"staging_bytes": 100_000_000_000,
                            "reserve_bytes": 10_000_000_000},
                "resources": {"archive": {"disk_bytes": 120_000_000_000}}}
        campaigns = [SimpleNamespace(max_processing_nodes=500)]
        self.assertEqual(production._root_step_count(spec, campaigns), 5)
        spec["limits"]["source_selection_mode"] = "demand"
        self.assertEqual(production._root_step_count(spec, campaigns), 702)

    def test_archive_disk_exhaustion_does_not_prepare_processing(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec_path, run = self.fixture(Path(temporary))
            spec = read_json(spec_path)
            spec["resources"] = {"archive": {"disk_bytes": 5_996}}
            atomic_json(spec_path, spec)
            plan_run(spec_path, run)
            self.planner_results(run)
            with self.assertRaisesRegex(RuntimeError, "archive disk budgets"):
                advance_wave(run, 1)
            state = read_json(run / "state.json")
            self.assertEqual(state["next_step"], 1)
            self.assertEqual(state["job_offsets"], {})
            self.assertFalse((run / "steps" / "000001.dag").exists())

    def test_miniaod_count_must_match_archived_hepmc_or_report_partial(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec_path, run = self.fixture(Path(temporary))
            plan_run(spec_path, run)
            self.planner_results(run)
            advance_wave(run, 1)
            self.processing_results(run)
            self.reclaim_result(run)
            pending = read_json(run / "state.json")["pending"]
            item = pending["miniaods"][0]
            mini_path = run / "results" / (item["task_id"] + ".json")
            mini = read_json(mini_path)
            mini["outputs"][0]["expected_events"] = 2
            atomic_json(mini_path, mini)
            with self.assertRaisesRegex(RuntimeError, "event count disagrees"):
                advance_wave(run, 2)
            self.assertEqual(read_json(run / "state.json")["next_step"], 1)
            self.assertFalse(archive_path(run, 1).exists())

            archive_path_result = run / "results" / (pending["archive_task"] + ".json")
            archive = read_json(archive_path_result)
            for member in archive["members"]:
                if member["kind"] == "hepmc":
                    member["events"] = 2
            atomic_json(archive_path_result, archive)
            mini["outputs"][0].update(events=1, expected_events=2, partial=True)
            mini["partial"] = True
            atomic_json(mini_path, mini)
            self.assertEqual(advance_wave(run, 2)["status"], "complete")

    def test_older_state_format_is_refused(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            plan_run(spec, run)
            state = read_json(run / "state.json")
            state["state_schema"] = "mcprod-state-v1"
            atomic_json(run / "state.json", state)
            with self.assertRaisesRegex(RuntimeError, "mcprod-state-v2"):
                advance_wave(run, 1)

    def test_pre_timeout_fails_and_complete_sentinel_still_skips(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            core = root / "core"
            core.mkdir()
            cli = core / "mcprod"
            cli.write_text("#!/bin/bash\n/usr/bin/sleep 1\n", encoding="utf-8")
            cli.chmod(0o755)
            proxy = root / "proxy"
            proxy.write_text("test-proxy\n", encoding="utf-8")
            with patch("full_mc.planning.graph.PRE_SCRIPT_TIMEOUT_SECONDS", 0.01):
                write_root_dag(root, {"core_root": str(core)}, 1,
                               str(proxy))
            pre = root / "advance.sh"
            subprocess.run(["bash", "-n", str(pre)], check=True)
            timed_out = subprocess.run([str(pre), "0"], check=False)
            self.assertEqual(timed_out.returncode, 124)
            self.assertIn("PRE_SKIP WAVE_000000 99", (root / "run.dag").read_text())

            (root / ".complete").touch()
            skipped = subprocess.run([str(pre), "0"], check=False)
            self.assertEqual(skipped.returncode, 99)

    def test_pre_script_exports_auth_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            core = root / "core"
            core.mkdir()
            env_dump = root / "pre-env.txt"
            proxy = root / "proxy"
            proxy.write_text("test-proxy\n", encoding="utf-8")
            cli = core / "mcprod"
            cli.write_text(
                "#!/bin/bash\n"
                "printf 'X509_USER_PROXY=%s\\nX509_CERT_DIR=%s\\n' "
                '"$X509_USER_PROXY" "$X509_CERT_DIR" > "' + str(env_dump) + '"\n',
                encoding="utf-8",
            )
            cli.chmod(0o755)
            write_root_dag(root, {"core_root": str(core)}, 1, str(proxy))
            outcome = subprocess.run([str(root / "advance.sh"), "0"], check=False)

            self.assertEqual(outcome.returncode, 0)
            self.assertEqual(
                env_dump.read_text(encoding="utf-8"),
                "X509_USER_PROXY=" + str(proxy.resolve()) + "\n"
                "X509_CERT_DIR=/cvmfs/grid.cern.ch/etc/grid-security/certificates\n",
            )
            self.assertNotIn("ENV SET X509_USER_PROXY=", (root / "run.dag").read_text())

    def test_proxy_resolution_blocks_artifact_generation(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            with patch("full_mc.application.production.resolve_proxy_path",
                       side_effect=RuntimeError("missing persistent proxy")):
                with self.assertRaisesRegex(RuntimeError, "missing persistent proxy"):
                    plan_run(spec, run)
            self.assertFalse(run.exists())

    def test_missing_referenced_runtime_asset_blocks_planning(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            payload = read_json(spec)
            runtime = Path(payload["runtime_dir"])
            (runtime / "core.tgz").unlink()
            with self.assertRaisesRegex(FileNotFoundError, "missing runtime asset"):
                plan_run(spec, run)


    def test_planner_result_must_match_the_frozen_source_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            plan_run(spec, run)
            self.planner_results(run)
            pending = read_json(run / "state.json")["pending"]
            result_path = run / "results" / (pending["records"][0]["plan_task"] + ".json")
            result = read_json(result_path)
            result["outputs"][0]["artifact_id"] = "BLOCK:pool:other:000000"
            atomic_json(result_path, result)
            with self.assertRaisesRegex(RuntimeError, "identities"):
                advance_wave(run, 1)
            self.assertEqual(read_json(run / "state.json")["next_step"], 0)
            self.assertFalse((run / "ledger").exists())

    def test_restores_only_committed_recorded_original(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            plan_run(spec, run)
            self.planner_results(run)
            advance_wave(run, 1)
            self.processing_results(run)
            self.reclaim_result(run)
            self.assertEqual(advance_wave(run, 2)["status"], "complete")
            receipts = load_archives(run, read_json(run / "run.json"), 2)
            archive_id = receipts[0]["outputs"][0]["artifact_id"]
            restored = restore_archived_sources(run, archive_id, run / "recovered")
            self.assertEqual([item["artifact_id"] for item in restored["restored"]], ["LHE_000000"])
            self.assertTrue(Path(restored["restored"][0]["url"]).is_file())
            with self.assertRaises(FileExistsError):
                restore_archived_sources(run, archive_id, run / "recovered")
            # Receipts the state has not accepted are not restoration material.
            atomic_json(archive_path(run, 99), {
                "schema": "mcprod-archive-receipt-v1", "step": 99, "task_id": "ARCHIVE_000099",
                "outputs": [{"artifact_id": "ARCHIVE_000099", "url": "root://elsewhere/x.tar",
                             "kind": "upstream-archive"}],
                "members": [{"artifact_id": "LHE_000000", "member": "000000_source.lhe.gz",
                             "url": "root://elsewhere/source.lhe.gz", "kind": "lhe",
                             "events": 15, "size_bytes": 1}]})
            with self.assertRaisesRegex(ValueError, "unknown verified archive"):
                restore_archived_sources(run, "ARCHIVE_000099", run / "recovered")

    def test_rejects_invalid_source_caps_and_revision_expectations(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec_path, run = self.fixture(Path(temporary))
            spec = read_json(spec_path)
            spec["sources"][0]["planned_events"] = 16
            atomic_json(spec_path, spec)
            with self.assertRaisesRegex(ValueError, "planned_events"):
                plan_run(spec_path, run)

            spec["sources"][0].pop("planned_events")
            spec["core_revision"] = "not-the-runtime"
            atomic_json(spec_path, spec)
            with self.assertRaisesRegex(ValueError, "core_revision"):
                plan_run(spec_path, run)

            spec["core_revision"] = ""
            spec["resources"] = {"miniaod": {"cpus": 1}}
            atomic_json(spec_path, spec)
            with self.assertRaisesRegex(ValueError, "cpus"):
                plan_run(spec_path, run)

            spec["resources"] = {}
            spec["limits"]["source_selection_mode"] = "prefetch"
            atomic_json(spec_path, spec)
            plan_run(spec_path, run)
            planned_spec = read_json(run / "run.json")
            self.assertEqual(planned_spec["resources"]["archive"]["disk_bytes"],
                             200_000_000_000)

            spec["limits"]["source_selection_mode"] = "unknown"
            atomic_json(spec_path, spec)
            with self.assertRaisesRegex(ValueError, "source_selection_mode"):
                plan_run(spec_path, run)


if __name__ == "__main__":
    unittest.main()
