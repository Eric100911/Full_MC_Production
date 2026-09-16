import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from full_mc.workers import execute_task
from full_mc.application.resume import run_combined_task


class InputRecordTests(unittest.TestCase):
    def test_hydrates_dynamic_inputs_before_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = root / "mini.root"
            payload.write_bytes(b"payload")
            (root / "mini.root.json").write_text(
                "{\"status\":\"ok\",\"complete\":true,\"artifact_id\":\"a1\",\"url\":\"%s\",\"kind\":\"miniaod\",\"events\":7,\"size_bytes\":7,\"runtime_release\":\"r1\",\"count_source\":\"edmFileUtil\"}" % payload
            )
            task = {"stage":"miniaod", "runtime":"cmssw12-el8",
                    "runtime_release":"r1", "inputs":[{"artifact_id":"a1",
                    "url":str(payload), "kind":"miniaod"}]}
            with patch("full_mc.workers.import_module") as loader:
                loader.return_value.execute.return_value = {"status":"ok"}
                execute_task(task, root / "work")
            self.assertEqual(task["inputs"][0]["events"], 7)
            self.assertEqual(task["inputs"][0]["count_source"], "edmFileUtil")

    def test_plan_keeps_authoritative_lhe_and_block_refs_without_sidecars(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            refs = [{"artifact_id":"lhe", "url":str(root / "x.lhe.gz"),
                     "kind":"lhe", "events":11},
                    {"artifact_id":"block", "url":"BLOCK:p:g:000001",
                     "kind":"lhe-block", "events":5}]
            task = {"stage":"plan", "runtime":"utilities-el9",
                    "runtime_release":"r1", "inputs":refs}
            with patch("full_mc.workers.import_module") as loader:
                loader.return_value.execute.return_value = {"status":"ok"}
                execute_task(task, root / "work")
            self.assertEqual(refs[0]["events"], 11)
            self.assertNotIn("size_bytes", refs[1])

    def test_jup_combined_checkpoint_has_only_merge_and_ntuple(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def stage(name, output):
                return {"parameters": {"analysis": "JUP"}, "outputs": [{"artifact_id": output, "url": str(root / output), "kind": "raw-ntuple" if name == "ntuple" else "merged-miniaod"}]}
            task = {"task_id":"jup-1", "stage":"combined", "runtime_release":"r1",
                    "checkpoint_dir":str(root / "checkpoints"),
                    "subtasks":{"merge":stage("merge", "merged"), "ntuple":stage("ntuple", "raw")}}
            def execute(current, work):
                return {"task_id":current["task_id"], "stage":current["stage"], "status":"ok",
                        "runtime_release":"r1", "outputs":current["outputs"]}
            # Use the task JSON path to exercise the public recovery entry point.
            (root / "task.json").write_text(__import__("json").dumps(task))
            with patch("full_mc.workers.execute_task", execute), patch("full_mc.application.resume.artifact_ready", return_value=True):
                result = run_combined_task(root / "task.json", root / "work")
            self.assertEqual([item["stage"] for item in result["stages"]], ["merge", "ntuple"])
            self.assertEqual([item["artifact_id"] for item in result["outputs"]], ["raw"])


if __name__ == "__main__":
    unittest.main()
