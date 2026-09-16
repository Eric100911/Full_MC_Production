from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.application.analysis import prepare_aggregate, run_aggregate_task
from full_mc.backends.hepjob import prepare_batch
from full_mc.io import atomic_json, read_json


def artifact(path, artifact_id, kind):
    path = Path(path)
    return {"artifact_id": artifact_id, "kind": kind, "url": str(path),
            "size_bytes": 1}


def aggregation_handoff(root, groups=1):
    tasks = []
    for index in range(groups):
        sample = "JJP_DPS%d" % (index + 1)
        selected = artifact(root / ("selected-%d.root" % index),
                            "selected-%d" % index, "selected-ntuple")
        shard = artifact(root / ("shard-%d.tar" % index),
                         "shard-%d" % index, "efficiency-shard")
        stages = {
            "merge": {"task_id": "g%d:merge" % index, "stage": "merge",
                      "parameters": {"analysis": "JJP", "merge_group": "g%d" % index},
                      "outputs": []},
            "selection": {"task_id": "g%d:selection" % index, "stage": "selection",
                          "outputs": [selected]},
            "efficiency": {"task_id": "g%d:efficiency" % index, "stage": "efficiency",
                           "parameters": {"campaign": sample},
                           "outputs": [shard]},
        }
        tasks.append({"task_id": "g%d" % index, "stage": "combined",
                      "subtasks": stages})
    handoff = {"schema": "full-mc-handoff-v1", "run_id": "run-1",
               "runtime_release": "release-1", "tasks": tasks}
    path = root / "handoff.json"
    atomic_json(path, handoff)
    return path


class AggregationContracts(unittest.TestCase):
    def test_prepare_records_pairs_outputs_and_requested_log_level(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            handoff = aggregation_handoff(root, 2)
            with patch("full_mc.application.analysis.artifact_ready", return_value=True):
                summary = prepare_aggregate(handoff, root / "aggregation.json",
                                            log_level="warning")
            task = read_json(root / "aggregation.json")
            self.assertEqual(summary["input_count"], 4)
            self.assertEqual(summary["output_count"], 4)
            self.assertEqual(task["log_level"], "warning")
            self.assertEqual(task["runtime"], "analyzer-el9")
            self.assertEqual(list(task["groups"]), ["JJP_DPS1", "JJP_DPS2"])
            self.assertEqual([out["kind"] for out in task["outputs"]],
                             ["selected-ntuple", "efficiency-maps"] * 2)

    def test_prepare_rejects_unverified_pair_member(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            handoff = aggregation_handoff(root)
            with patch("full_mc.application.analysis.artifact_ready",
                       side_effect=[True, False]):
                with self.assertRaisesRegex(ValueError, "unverified aggregation input"):
                    prepare_aggregate(handoff, root / "aggregation.json")

    def test_worker_merges_selected_inputs_and_all_shards_with_log_level(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            selected = root / "selected.root"
            selected.write_bytes(b"selected")
            shard_archive = root / "efficiency.tar"
            source = root / "source.parquet"
            source.write_bytes(b"shard")
            with tarfile.open(shard_archive, "w") as archive:
                archive.add(source, arcname="shard_0/JJP_DPS1/events.parquet")
            selected_ref = artifact(selected, "selected-0", "selected-ntuple")
            shard_ref = artifact(shard_archive, "shard-0", "efficiency-shard")
            task = {
                "schema": "full-mc-aggregate-v1", "task_id": "run-1:aggregate",
                "stage": "aggregate",
                "runtime": "analyzer-el9", "runtime_release": "release-1",
                "groups": {"JJP_DPS1": [{"selection": selected_ref,
                                          "efficiency": shard_ref}]},
                "outputs": [
                    artifact(root / "published-selected.root",
                             "aggregate:JJP_DPS1:selected", "selected-ntuple"),
                    artifact(root / "published-efficiency.tar",
                             "aggregate:JJP_DPS1:efficiency", "efficiency-maps"),
                ],
                "log_level": "info",
            }
            task_path = root / "aggregation.json"
            atomic_json(task_path, task)
            calls = []

            def fake_run(run_task, runtime, command, work_dir):
                calls.append((run_task, runtime, list(map(str, command))))
                if "merge_efficiency_shards.py" in str(command):
                    output_dir = Path(work_dir) / "efficiency" / "JJP_DPS1"
                    output_dir.mkdir(parents=True)
                    (output_dir / "efficiency_maps.parquet").write_bytes(b"maps")

            def fake_publish(path, run_task, artifact_id):
                declared = next(ref for ref in run_task["outputs"]
                                if ref["artifact_id"] == artifact_id)
                return dict(declared, sidecar_url=str(declared["url"]) + ".json")

            def fake_materialize(ref, work_dir):
                return Path(ref["url"])

            with (
                patch("full_mc.application.analysis.artifact_ready",
                      return_value=True),
                patch("full_mc.application.analysis.materialize_input",
                      side_effect=fake_materialize),
                patch("full_mc.application.analysis.run", side_effect=fake_run),
                patch("full_mc.application.analysis.publish",
                      side_effect=fake_publish),
            ):
                outcome = run_aggregate_task(task_path, root / "work",
                                              log_level="warning")

            self.assertEqual(outcome["status"], "ok")
            self.assertEqual(outcome["output_count"], 2)
            self.assertEqual(len(calls), 3)
            self.assertTrue(all(call[0]["log_level"] == "warning" for call in calls))
            self.assertIn("selected-inputs.json", calls[0][2][2])
            self.assertIn("--sample", calls[1][2])
            self.assertIn("--skip-plots", calls[2][2])
            self.assertEqual(read_json(root / "work" / "result.json")["status"], "ok")

    def test_log_level_is_validated_for_prepare_and_worker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            handoff = aggregation_handoff(root)
            with patch("full_mc.application.analysis.artifact_ready",
                       return_value=True):
                with self.assertRaisesRegex(ValueError, "log_level"):
                    prepare_aggregate(handoff, root / "bad.json",
                                     log_level="quiet")
            task = {"schema": "full-mc-aggregate-v1", "task_id": "a",
                    "groups": {}, "outputs": [], "log_level": "quiet"}
            atomic_json(root / "bad-task.json", task)
            with self.assertRaisesRegex(ValueError, "log_level"):
                run_aggregate_task(root / "bad-task.json", root / "work")


class BatchLoggingContracts(unittest.TestCase):
    def test_batch_preparation_propagates_level_to_combined_subtasks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            handoff = aggregation_handoff(root)
            with patch("full_mc.backends.hepjob._combined_tasks",
                       return_value=iter([{
                           "task_id": "combined:g0", "stage": "combined",
                           "runtime_release": "release-1", "subtasks": {
                               stage: {"task_id": stage, "stage": stage}
                               for stage in ("merge", "ntuple", "selection", "efficiency")
                           }}])):
                batch = prepare_batch(root / "handoff.json", root / "batch",
                                      proxy="/proxy", log_level="error")
            prepared = read_json(batch["jobs"][0]["task_path"])
            self.assertEqual(prepared["log_level"], "error")
            self.assertEqual({subtask["log_level"]
                              for subtask in prepared["subtasks"].values()}, {"error"})


if __name__ == "__main__":
    unittest.main()
