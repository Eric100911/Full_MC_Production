"""Offline handoff tests: gate future IHEP work on its component MiniAODs."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application.handoff import export_handoff
from full_mc.application.ledger import STATE_SCHEMA, sample_key, write_group
from full_mc.io import atomic_json, read_json

CAMPAIGN = "C"
PROFILE = "run3_2022_v1"
SAMPLE = sample_key(CAMPAIGN, PROFILE)
STAGES = ("merge", "ntuple", "selection", "efficiency")


def combined(task_id, root, inputs):
    return {"task_id": task_id, "stage": "combined", "subtasks": {
        stage: {"task_id": stage, "stage": stage,
                "inputs": inputs if stage == "merge" else [],
                "outputs": [{"artifact_id": stage, "kind": stage,
                             "url": str(root / (stage + ".root"))}],
                "parameters": {"core_root": "/cern/checkout"}}
        for stage in STAGES}}


def run_dir(root, groups, committed, analysis_output=None):
    """A run whose committed ledger ordinals are the only handoff source.

    ``groups`` is written in ordinal order; only the first ``committed`` of them
    are below the state counter and therefore committed.
    """
    run_spec = {
        "run_id": "r", "runtime_release": "r1", "core_revision": "abc",
        "campaigns": [{"name": CAMPAIGN,
                       "resolved_detector_variants": [{"profile_id": PROFILE}]}]}
    if analysis_output is not None:
        run_spec["analysis_output"] = analysis_output
    atomic_json(root / "run.json", run_spec)
    atomic_json(root / "state.json",
                {"state_schema": STATE_SCHEMA, "status": "complete", "next_step": 1,
                 "pending": {}, "pool_cursors": {}, "job_offsets": {},
                 "merge_buffers": {}, "merge_group_counts": {SAMPLE: committed}})
    for ordinal, group in enumerate(groups):
        write_group(root, CAMPAIGN, PROFILE, ordinal, group)


def mini_ref(root, identity="mini"):
    path = root / (identity + ".root")
    path.write_bytes(b"fixture")
    ref = {"artifact_id": identity, "kind": "miniaod", "url": str(path)}
    atomic_json(str(path) + ".json", dict(ref, status="ok", complete=True,
                events=17, count_source="edmFileUtil", size_bytes=7))
    return ref


class HandoffContracts(unittest.TestCase):
    def test_exports_components_before_downstream_outputs_exist(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ref = mini_ref(root)
            run_dir(root, [combined("group", root, [ref])], committed=1)
            payload = export_handoff(root)
            self.assertEqual(len(payload["tasks"]), 1)
            merge = payload["tasks"][0]["subtasks"]["merge"]
            self.assertEqual(merge["inputs"][0]["events"], 17)
            self.assertNotIn("core_root", merge["parameters"])
            self.assertFalse((root / "merge.root").exists())
            (root / "mini.root").unlink()
            self.assertEqual(export_handoff(root)["tasks"], [])

    def test_committed_ordinals_come_from_the_ledger_not_the_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ref = mini_ref(root)
            run_dir(root, [combined("group", root, [ref])], committed=1)
            # A stale snapshot, as the old coordinator would have left it.
            atomic_json(root / "merge_groups.json", {
                "run_id": "r", "runtime_release": "r1",
                "groups": [combined("snapshot-only", root, [ref])]})
            payload = export_handoff(root)
            self.assertEqual([task["task_id"] for task in payload["tasks"]],
                             ["group"])

    def test_uncommitted_group_ordinal_is_never_exported(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ref = mini_ref(root)
            groups = [combined("committed", root, [ref]),
                      combined("pending", root, [ref])]
            # The second ordinal already exists on disk but the state counter
            # has not reached it, so it is not an accepted transition.
            run_dir(root, groups, committed=1)
            payload = export_handoff(root)
            self.assertEqual([task["task_id"] for task in payload["tasks"]],
                             ["committed"])
            snapshot = read_json(root / "merge_groups.json")
            self.assertEqual([group["task_id"] for group in snapshot["groups"]],
                             ["committed"])

    def test_snapshot_metadata_identifies_the_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ref = mini_ref(root)
            analysis_output = {"url": "root://cceos.ihep.ac.cn:1094///store/analysis.tar"}
            run_dir(root, [combined("group", root, [ref])], committed=1,
                    analysis_output=analysis_output)
            payload = export_handoff(root)
            self.assertEqual(payload["schema"], "full-mc-handoff-v1")
            self.assertEqual(payload["run_id"], "r")
            self.assertEqual(payload["runtime_release"], "r1")
            self.assertEqual(payload["core_revision"], "abc")
            self.assertEqual(payload["tasks"][0]["runtime_release"], "r1")
            self.assertEqual(payload["rejected"], [])
            self.assertEqual(read_json(root / "merge_groups.json")["analysis_output"],
                             analysis_output)
            self.assertEqual(payload["analysis_output"], analysis_output)

    def test_ready_only_false_rejects_unready_groups(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ref = mini_ref(root)
            run_dir(root, [combined("group", root, [ref])], committed=1)
            (root / "mini.root").unlink()
            with self.assertRaises(ValueError):
                export_handoff(root, ready_only=False)
            self.assertEqual(
                [item["task_id"] for item in export_handoff(root)["rejected"]],
                ["group"])


if __name__ == "__main__":
    unittest.main()
