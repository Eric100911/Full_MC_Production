"""Controller wiring for the reclaim worker: DAG chain, cursor gate, terminal wave."""
from contextlib import ExitStack
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application.production import advance_wave, plan_run
from full_mc.io import atomic_json, read_json
from full_mc.storage import reclaim as storage_reclaim
from full_mc.storage.transfer import TransferClient
from full_mc.workers.reclaim import execute as execute_reclaim
from tests.generate_synthetic_lhe import generate

RELEASE = "test-release"
STAGING = "root://eosuser.cern.ch///eos/user/c/chiw/staging"
RUN_ID = "wired"
PREFIX = STAGING + "/" + RUN_ID
REMOTE_METHODS = ("size", "mkdir_parent", "mkdir", "read_json_if_exists",
                  "directory_has_entries", "copy_json_if_absent", "copy_out",
                  "copy_in", "read_json", "remove")


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
class ReclaimWiring(unittest.TestCase):
    def fixture(self, root, blocks=3):
        """A hermetic run whose staging root is real XRootD spelling.

        Everything under the staging prefix stays virtual: the seam reads only
        the local ledger, so the worker results below are hand-written.
        """
        source = root / "input" / "input.lhe"
        source.parent.mkdir(parents=True)
        generate(str(source), 15)
        runtime = root / "runtime"
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
            "runtime_dir": str(runtime),
            "block_events": 5,
            "storage": {"cern_staging": STAGING,
                        "ihep_products": "root://eosuser.cern.ch///eos/user/c/chiw/products",
                        "ihep_archive": "root://eosuser.cern.ch///eos/user/c/chiw/archive",
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
        return spec_path, root / "run", blocks

    def planner_results(self, run, blocks):
        """A valid PLAN manifest whose staged URLs were never materialized here."""
        pending = read_json(run / "state.json")["pending"]
        plan_id = pending["records"][0]["plan_task"]
        task = read_json(run / "tasks" / (plan_id + ".json"))
        original = dict(task["parameters"]["original_output"], events=15, size_bytes=900)
        directory = str(task["parameters"]["block_output_dir"]).rstrip("/")
        outputs = [{"artifact_id": "BLOCK:pool:s1:%06d" % index, "kind": "lhe-block",
                    "events": 5, "size_bytes": 900,
                    "url": directory + "/block%d.lhe.gz" % index}
                   for index in range(blocks)]
        atomic_json(run / "results" / (plan_id + ".json"), {
            "task_id": plan_id, "stage": "plan", "status": "ok", "runtime_release": RELEASE,
            "outputs": outputs, "originals": [original],
            "planned_events": 5 * blocks, "actual_input_events": 15})

    def processing_results(self, run):
        """Component MiniAOD and archive receipts; the reclaim result stays unwritten."""
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

    def reclaim_result(self, run, *, drop=None):
        """The reclaim worker envelope for the wave the state is currently holding."""
        pending = read_json(run / "state.json")["pending"]
        task = read_json(run / "tasks" / (pending["reclaim_task"] + ".json"))
        entries = []
        for ref in task["inputs"]:
            if ref["artifact_id"] == drop:
                continue
            objects = [{"kind": "payload", "url": ref["url"], "disposition": "removed"}]
            if "sidecar_url" in ref:
                objects.append({"kind": "sidecar", "url": ref["sidecar_url"],
                                "disposition": "already-missing"})
            entries.append({"artifact_id": ref["artifact_id"], "objects": objects})
        return {"task_id": task["task_id"], "stage": "reclaim", "status": "ok",
                "runtime_release": RELEASE, "outputs": [], "reclaimed": entries}

    def write_reclaim_result(self, run, **kwargs):
        outcome = self.reclaim_result(run, **kwargs)
        atomic_json(run / "results" / (outcome["task_id"] + ".json"), outcome)

    def staged_run(self, root, blocks=3):
        """Run up to the prepared processing wave, with its PLAN fold committed."""
        spec, run, blocks = self.fixture(root, blocks)
        plan_run(spec, run)
        self.planner_results(run, blocks)
        self.assertEqual(advance_wave(run, 1)["phase"], "processing")
        return run

    def test_processing_dag_chains_mix_miniaod_archive_reclaim(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.staged_run(Path(temporary))
            pending = read_json(run / "state.json")["pending"]
            dag = Path(pending["dag"]).read_text()
            task_id = pending["reclaim_task"]
            self.assertRegex(dag, r"PARENT MIX_[^ ]+ CHILD MINIAOD_[^ ]+")
            self.assertRegex(dag, r"PARENT MINIAOD_[^ ]+ CHILD ARCHIVE_")
            self.assertIn("PARENT " + pending["archive_task"] + " CHILD " + task_id, dag)
            self.assertIn("CATEGORY " + task_id + " lhe_coordination", dag)
            self.assertIn("MAXJOBS lhe_coordination 1", dag)
            self.assertIn(str(run / "submit" / "reclaim.sub"), dag)
            self.assertTrue((run / "submit" / "reclaim.sub").is_file())

    def test_reclaim_task_carries_exact_references_and_staging_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.staged_run(Path(temporary))
            pending = read_json(run / "state.json")["pending"]
            task = read_json(run / "tasks" / (pending["reclaim_task"] + ".json"))
            self.assertEqual(task["stage"], "reclaim")
            self.assertEqual(task["runtime"], "utilities-el9")
            self.assertEqual(task["runtime_release"], RELEASE)
            self.assertEqual(task["outputs"], [])
            self.assertEqual(task["parameters"], {"staging_prefix": PREFIX})
            self.assertEqual([ref["artifact_id"] for ref in task["inputs"]],
                             ["LHE_000000", "BLOCK:pool:s1:000000", "BLOCK:pool:s1:000001",
                              "HEPMC_C_JOB000000_BLOCK000000"])
            for ref in task["inputs"]:
                self.assertTrue(ref["url"].startswith(PREFIX + "/"), ref["url"])
                # Only a recorded sidecar travels; none of these declare one.
                self.assertNotIn("sidecar_url", ref)

    def test_cursors_advance_only_after_a_valid_reclaim_result(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.staged_run(Path(temporary))
            self.processing_results(run)
            with self.assertRaises(RuntimeError):
                advance_wave(run, 2)
            state = read_json(run / "state.json")
            self.assertEqual(state["next_step"], 1)
            self.assertEqual(state["pending"]["step"], 1)
            self.assertEqual(state["pool_cursors"], {})

            pending = state["pending"]
            first = read_json(run / "tasks" / (pending["reclaim_task"] + ".json"))["inputs"][0]
            self.write_reclaim_result(run, drop=first["artifact_id"])
            with self.assertRaisesRegex(RuntimeError, "does not account for every declared reference"):
                advance_wave(run, 2)
            state = read_json(run / "state.json")
            self.assertEqual(state["next_step"], 1)
            self.assertEqual(state["pool_cursors"], {})

            self.write_reclaim_result(run)
            # The cursor gate is what advanced; completion itself then waits for
            # the terminal reclaim of the block this wave never consumed.
            self.assertEqual(advance_wave(run, 2)["phase"], "reclaim")
            self.assertEqual(read_json(run / "state.json")["pool_cursors"], {"pool": 2})

    def test_terminal_reclaim_runs_before_complete_is_written(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.staged_run(Path(temporary))
            self.processing_results(run)
            self.write_reclaim_result(run)
            prepared = advance_wave(run, 2)
            self.assertEqual(prepared["phase"], "reclaim")
            self.assertFalse((run / ".complete").exists())
            pending = read_json(run / "state.json")["pending"]
            self.assertEqual(pending["step"], 2)
            task = read_json(run / "tasks" / (pending["reclaim_task"] + ".json"))
            self.assertEqual([ref["artifact_id"] for ref in task["inputs"]],
                             ["BLOCK:pool:s1:000002"])
            self.assertEqual(task["parameters"], {"staging_prefix": PREFIX})
            dag = Path(pending["dag"]).read_text()
            self.assertIn("CATEGORY " + pending["reclaim_task"] + " lhe_coordination", dag)

            self.write_reclaim_result(run)
            done = advance_wave(run, 3)
            self.assertEqual(done["status"], "complete")
            self.assertTrue((run / ".complete").is_file())
            self.assertEqual(read_json(run / "state.json")["pending"], {})

    def test_completion_without_leftovers_creates_no_empty_subdag(self):
        with tempfile.TemporaryDirectory() as temporary:
            # Two planned blocks with a 5+5 budget consume the whole source, so
            # there is nothing for a terminal cleanup task to remove.
            run = self.staged_run(Path(temporary), blocks=2)
            self.processing_results(run)
            self.write_reclaim_result(run)
            self.assertEqual(advance_wave(run, 2)["status"], "complete")
            state = read_json(run / "state.json")
            self.assertEqual(state["status"], "complete")
            self.assertEqual(state["pending"], {})
            self.assertEqual(state["pool_cursors"], {"pool": 2})
            self.assertTrue((run / ".complete").is_file())
            self.assertFalse((run / "steps" / "000002.dag").exists())

    def test_no_seam_step_launches_a_container(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.staged_run(Path(temporary))
            self.processing_results(run)
            self.write_reclaim_result(run)
            with patch.object(storage_reclaim, "run_profile",
                              side_effect=AssertionError("seam launched a container")):
                with remote_io_blocked():
                    self.assertEqual(advance_wave(run, 2)["phase"], "reclaim")
                    self.write_reclaim_result(run)
                    self.assertEqual(advance_wave(run, 3)["status"], "complete")
            self.assertTrue((run / ".complete").is_file())

    def test_reclaim_worker_refuses_a_reference_outside_the_run_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = {"task_id": "RECLAIM_000001", "stage": "reclaim",
                    "runtime": "utilities-el9", "runtime_release": RELEASE,
                    "inputs": [{"artifact_id": "A", "kind": "lhe-block",
                                "url": "root://eosuser.cern.ch///eos/user/c/chiw/other/b.lhe.gz"}],
                    "outputs": [], "parameters": {"staging_prefix": PREFIX}}
            with patch.object(storage_reclaim, "run_profile") as run:
                with self.assertRaisesRegex(ValueError, "refusing to reclaim outside run staging"):
                    execute_reclaim(task, root / "work")
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
