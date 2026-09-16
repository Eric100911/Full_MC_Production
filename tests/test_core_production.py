"""Offline lifecycle contracts for the lazy CERN production controller."""
import gzip
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application.production import advance_wave, plan_run, restore_archived_sources
from full_mc.io import atomic_json, read_json
from full_mc.storage.transfer import artifact_ref, TransferClient
from full_mc.workers.archive import execute as archive_execute
from tests.generate_synthetic_lhe import generate


def sidecar(ref, *, events=None, count_source=None):
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
    atomic_json(str(path) + ".json", result)
    return result


class ProductionLifecycle(unittest.TestCase):
    def fixture(self, root):
        source = root / "input.lhe"
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
            "profiles": ["utilities-el9", "pythia-el8", "cmssw12-el8"],
            "bundles": {"core": "core.tgz", "utilities-el9": "utilities.tgz",
                        "pythia-el8": "pythia.tgz", "cmssw12-el8": "cmssw12.tgz"}})
        spec = {
            "run_id": "offline", "runtime_release": "test-release", "runtime_dir": str(runtime),
            "block_events": 5,
            "storage": {"cern_staging": str(root / "cern"),
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
                         "url": str(source), "actual_events": 15}],
            "campaigns": [{"name": "C", "analysis": "JJP", "target_mixed_events": 1,
                           "max_processing_nodes": 1, "merge_events": 1,
                           "sources": [{"pool": "pool", "mode": "normal", "lhe_budget": 5},
                                       {"pool": "pool", "mode": "normal", "lhe_budget": 5}]}]}
        spec_path = root / "spec.json"
        atomic_json(spec_path, spec)
        return spec_path, root / "run"

    def planner_results(self, run):
        pending = read_json(run / "state.json")["pending"]
        plan_id = pending["records"][0]["plan_task"]
        task = read_json(run / "tasks" / (plan_id + ".json"))
        original = task["parameters"]["original_output"]
        original_path = Path(original["url"])
        original_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(original_path, "wb") as stream:
            stream.write(b"<LesHouchesEvents>\n<event>\n1\n</event>\n</LesHouchesEvents>\n")
        original = artifact_ref(original_path, original["artifact_id"], original["kind"], 15, original["url"])
        blocks = []
        for index in range(3):
            path = original_path.parent / "blocks" / ("block%d.lhe.gz" % index)
            path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(path, "wb") as stream:
                stream.write(b"<event>\n%d\n</event>\n" % index)
            blocks.append(artifact_ref(path, "BLOCK:pool:s1:%06d" % index, "lhe-block", 5, str(path)))
        atomic_json(run / "results" / (plan_id + ".json"), {"task_id": plan_id, "stage": "plan",
                    "status": "ok", "runtime_release": "test-release", "outputs": blocks,
                    "originals": [original], "planned_events": 15,
                    "actual_input_events": 15})

    def processing_results(self, run):
        pending = read_json(run / "state.json")["pending"]
        archive_task = read_json(run / "tasks" / (pending["archive_task"] + ".json"))
        for mixed in pending["mixed"]:
            mixed_path = Path(mixed["url"])
            mixed_path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(mixed_path, "wb") as stream:
                stream.write(b"HepMC::Version 3\n")
        for item in pending["miniaods"]:
            task = read_json(run / "tasks" / (item["task_id"] + ".json"))
            output = sidecar(task["outputs"][0], events=1, count_source="edmFileUtil")
            atomic_json(run / "results" / (item["task_id"] + ".json"), {"task_id": item["task_id"],
                        "stage": "miniaod", "status": "ok", "runtime_release": "test-release",
                        "outputs": [output]})
        # Archive is a real local worker execution; MiniAOD gates use the sidecars above.
        outcome = archive_execute(archive_task, run / "archive-work")
        atomic_json(run / "results" / (pending["archive_task"] + ".json"), outcome)

    def test_lazy_lifecycle_replay_and_reclaim_are_idempotent(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            planned = plan_run(spec, run)
            self.assertEqual(planned["first_phase"], "sources")
            root_dag = (run / "run.dag").read_text()
            self.assertIn("PRE_SKIP WAVE_000000 99", root_dag)
            self.assertIn("SCRIPT PRE WAVE_000000", root_dag)
            source_dag = Path(read_json(run / "state.json")["pending"]["dag"]).read_text()
            self.assertIn("CONFIG " + str(run / "dagman.config"), source_dag)
            self.assertIn("CATEGORY", source_dag)
            self.assertIn("MAXJOBS lhe", source_dag)
            self.assertIn("should_transfer_files = YES", (run / "submit/plan.sub").read_text())
            self.assertFalse((run / "submit/merge.sub").exists())
            replay = advance_wave(run, 0)
            self.assertEqual(replay["phase"], "sources")
            self.planner_results(run)
            processing = advance_wave(run, 1)
            self.assertEqual(processing["phase"], "processing")
            process_dag = Path(processing["dag"]).read_text()
            self.assertRegex(process_dag, r"PARENT MIX_[^ ]+ CHILD MINIAOD_[^ ]+")
            self.assertRegex(process_dag, r"PARENT MINIAOD_[^ ]+ CHILD ARCHIVE_")
            self.processing_results(run)
            with patch.object(TransferClient, "remove", side_effect=RuntimeError("cleanup interruption")):
                with self.assertRaisesRegex(RuntimeError, "cleanup interruption"):
                    advance_wave(run, 2)
            failed = read_json(run / "state.json")
            self.assertEqual(failed["pool_cursors"], {})
            self.assertEqual(failed["merge_buffers"], {})
            done = advance_wave(run, 2)
            self.assertEqual(done["status"], "complete")
            state = read_json(run / "state.json")
            self.assertEqual(state["pool_cursors"], {"pool": 2})
            self.assertEqual(len(state["merge_buffers"]["C"]), 0)
            self.assertEqual(len(state["groups"]), 1)
            self.assertEqual(len(state["archives"]), 1)
            self.assertEqual(state["terminal_cleanup"], ["BLOCK:pool:s1:000002"])
            self.assertFalse((run / "cern/offline/sources/pool/s1/blocks/block2.lhe.gz").exists())

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

    def test_restores_only_recorded_original_after_terminal_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            plan_run(spec, run)
            self.planner_results(run)
            advance_wave(run, 1)
            self.processing_results(run)
            self.assertEqual(advance_wave(run, 2)["status"], "complete")
            archive_id = read_json(run / "state.json")["archives"][0]["outputs"][0]["artifact_id"]
            restored = restore_archived_sources(run, archive_id, run / "recovered")
            self.assertEqual([item["artifact_id"] for item in restored["restored"]], ["LHE_000000"])
            self.assertTrue(Path(restored["restored"][0]["url"]).is_file())
            with self.assertRaises(FileExistsError):
                restore_archived_sources(run, archive_id, run / "recovered")

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


if __name__ == "__main__":
    unittest.main()
