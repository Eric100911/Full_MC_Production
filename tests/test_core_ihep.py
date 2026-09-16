"""Offline IHEP preparation and exact-attempt submission tests."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.backends.hepjob import (prepare_batch, resume_batch, SAFE_NTUPLE_NODES,
                                     status_batch, submit_batch)
from full_mc.cli import build_parser, dispatch
from full_mc.io import atomic_json, read_json


def handoff(root, count=1):
    tasks = []
    for index in range(count):
        for stage in ("merge", "ntuple", "selection", "efficiency"):
            tasks.append({"task_id": f"g{index}:{stage}", "stage": stage,
                          "runtime_release": "r1",
                          "parameters": {"merge_group": f"g{index}", "analysis": "JJP"},
                          "inputs": [], "outputs": []})
    path = root / "handoff.json"
    atomic_json(path, {"schema": "full-mc-handoff-v1", "run_id": "r", "tasks": tasks})
    return path


def runtime(root, release="r1", revision="core1"):
    root.mkdir()
    (root / "core.tgz").write_bytes(b"core")
    controller = root / "controller/core/src/full_mc/runtime"
    controller.mkdir(parents=True)
    (root / "controller/core/mcprod").write_text("#!/bin/sh\n", encoding="utf-8")
    (controller / "bootstrap.py").write_text("# bootstrap\n", encoding="utf-8")
    atomic_json(root / "runtime.json", {"runtime_release": release, "site": "ihep",
                                         "git_revision": revision,
                                         "profiles": ["cmssw12-el8", "cmssw15-el9", "analyzer-el9"],
                                         "bundles": {"core": "core.tgz"}})
    return root


class IhepContracts(unittest.TestCase):
    def test_prepared_wrappers_are_private_and_nodes_round_robin(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root, 12), root / "batch", proxy="/proxy path")
            self.assertEqual(len(batch["jobs"]), 12)
            self.assertEqual(batch["jobs"][0]["node"], SAFE_NTUPLE_NODES[0] + ".ihep.ac.cn")
            self.assertEqual(batch["jobs"][11]["node"], batch["jobs"][0]["node"])
            checkpoints = set()
            for job in batch["jobs"]:
                wrapper = Path(job["wrapper"])
                subprocess.run(["bash", "-n", str(wrapper)], check=True)
                text = wrapper.read_text()
                self.assertIn("mktemp -d /tmp/chiw/", text)
                self.assertIn("credentials/x509_user_proxy", text)
                self.assertNotIn("x509up_u", text)
                self.assertIn("proxy_source='/proxy path'", text)
                checkpoints.add(read_json(job["task_path"])["checkpoint_dir"])
            self.assertEqual(len(checkpoints), 12)
            with self.assertRaises(FileExistsError):
                prepare_batch(root / "handoff.json", root / "batch")

    def test_ambiguous_attempt_is_not_resubmitted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root), root / "batch")
            failed = Mock(returncode=1, stdout="", stderr="network cut after submit")
            submit_batch(batch["manifest_path"], runner=Mock(return_value=failed))
            updated = read_json(batch["manifest_path"])
            self.assertEqual(updated["jobs"][0]["state"], "unknown")
            no_submit = Mock(side_effect=AssertionError("must not retry unknown attempt"))
            resume_batch(batch["manifest_path"], runner=no_submit)
            no_submit.assert_not_called()

    def test_crash_after_attempt_record_does_not_duplicate_submission(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root), root / "batch")
            with self.assertRaises(KeyboardInterrupt):
                submit_batch(batch["manifest_path"], runner=Mock(side_effect=KeyboardInterrupt))
            self.assertEqual(read_json(batch["manifest_path"])["jobs"][0]["state"], "submitting")
            no_submit = Mock(side_effect=AssertionError("must reconcile crashed attempt"))
            resume_batch(batch["manifest_path"], runner=no_submit)
            no_submit.assert_not_called()

    def test_status_and_explicit_recovery_use_only_the_recorded_job(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root), root / "batch")
            submit_batch(batch["manifest_path"], runner=Mock(return_value=Mock(
                returncode=0, stdout="submitted to cluster 27", stderr="")))
            active = Mock(return_value=Mock(returncode=0, stdout="27 running", stderr=""))
            status_batch(batch["manifest_path"], runner=active)
            self.assertEqual(active.call_args.args[0], ["hep_q", "27"])
            self.assertEqual(read_json(batch["manifest_path"])["jobs"][0]["state"], "submitted")

            failed = Mock(return_value=Mock(returncode=0, stdout="27 failed", stderr=""))
            status_batch(batch["manifest_path"], runner=failed)
            self.assertEqual(read_json(batch["manifest_path"])["jobs"][0]["state"], "failed")
            recovered = resume_batch(batch["manifest_path"], runner=Mock(return_value=Mock(
                returncode=0, stdout="submitted to cluster 28", stderr="")))
            job = recovered["jobs"][0]
            self.assertEqual((job["state"], job["job_id"], job["recovery_of"]),
                             ("submitted", "28", "27"))
            self.assertEqual(len(job["attempts"]), 2)

    def test_pinned_handoff_requires_matching_ihep_runtime_controller(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = read_json(handoff(root))
            payload.update(runtime_release="r1", core_revision="core1")
            atomic_json(root / "handoff.json", payload)
            with self.assertRaisesRegex(ValueError, "runtime-dir"):
                prepare_batch(root / "handoff.json", root / "missing")
            release = runtime(root / "runtime")
            batch = prepare_batch(root / "handoff.json", root / "batch", runtime_dir=release)
            task = read_json(batch["jobs"][0]["task_path"])
            self.assertEqual(task["subtasks"]["merge"]["parameters"]["core_root"],
                             str(release / "controller/core"))

    def test_cli_routes_batch_status_and_runtime_directory(self):
        parser = build_parser()
        prepared = parser.parse_args(["batch", "prepare", "--handoff", "handoff.json",
                                      "--output-dir", "batch", "--runtime-dir", "runtime"])
        self.assertEqual(prepared.runtime_dir, Path("runtime"))
        status = parser.parse_args(["batch", "status", "--manifest", "batch_manifest.json"])
        with patch("full_mc.backends.hepjob.status_batch", return_value={"checked": True}) as checked:
            self.assertEqual(dispatch(status), {"checked": True})
        checked.assert_called_once_with(Path("batch_manifest.json").resolve())

    def test_cli_forwards_log_level_to_batch_and_aggregation_preparation(self):
        parser = build_parser()
        batch = parser.parse_args(["batch", "prepare", "--handoff", "handoff.json",
                                   "--output-dir", "batch", "--log-level", "warning"])
        with patch("full_mc.backends.hepjob.prepare_batch", return_value={"prepared": True}) as prepared:
            self.assertEqual(dispatch(batch), {"prepared": True})
        self.assertEqual(prepared.call_args.kwargs["log_level"], "warning")

        aggregate = parser.parse_args(["analysis", "aggregate", "--run", "run", "--prepare",
                                       "--handoff", "handoff.json", "--output", "aggregate.json",
                                       "--log-level", "error"])
        with patch("full_mc.application.analysis.prepare_aggregate",
                   return_value={"prepared": True}) as prepared_aggregate:
            self.assertEqual(dispatch(aggregate), {"prepared": True})
        self.assertEqual(prepared_aggregate.call_args.kwargs["log_level"], "error")


if __name__ == "__main__":
    unittest.main()
