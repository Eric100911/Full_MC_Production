"""Read-only monitoring of a frozen v3 production attempt."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from full_mc.application.staging import FUSED_STATE_SCHEMA
from full_mc.io import atomic_json
from tools.mcprod_snapshot import build_snapshot, query_queue


class Snapshot(unittest.TestCase):
    def fixture(self, root):
        atomic_json(root / "run.json", {"state_schema": FUSED_STATE_SCHEMA, "run_id": "pilot"})
        atomic_json(root / "state.json", {
            "state_schema": FUSED_STATE_SCHEMA, "status": "running",
            "planner": {"sources": {"0": "completed", "1": "skipped"}},
            "allocations": {"SHARD_C_000000": {"completed": False}},
            "accepted": {"PLAN_000000": {"stage": "plan"}},
        })
        atomic_json(root / "submission.json", {
            "run_dir": str(root), "dag": str(root / "run.dag"),
            "current_attempt": 2, "scheduler": "test-schedd", "root_cluster_id": "42",
            "attempts": [{"number": 1, "scheduler": "test-schedd", "root_cluster_id": "41"},
                         {"number": 2, "scheduler": "test-schedd", "root_cluster_id": "42"}],
        })
        (root / "run.dag").write_text(
            "SUBDAG EXTERNAL SOURCE_000000 source.dag\n"
            "SUBDAG EXTERNAL SOURCE_000001 unrendered.dag\n"
            "SUBDAG EXTERNAL SHARD_C_000000 shard.dag\n"
            "JOB CLEANUP_p_000000 reclaim.sub\nJOB FINALIZE noop.sub NOOP\n")
        (root / "source.dag").write_text("JOB PLAN_000000 plan.sub\n")
        (root / "shard.dag").write_text("JOB PROCESS_C_JOB000000_BLOCK000000 processing.sub\n")

    def test_v3_counts_use_committed_state_and_live_workers(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as tmp:
            root = Path(tmp)
            self.fixture(root)
            before = sorted(root.iterdir())
            reader = Mock(return_value=[
                {"DAGNodeName": "PROCESS_C_JOB000000_BLOCK000000", "JobStatus": 2}])
            result = build_snapshot(root, queue_reader=reader)
            reader.assert_called_once_with("test-schedd", 42)
            self.assertEqual(result["stages"]["source"], {"done": 1, "skipped": 1})
            self.assertEqual(result["stages"]["plan"], {"done": 1})
            self.assertEqual(result["stages"]["processing"], {"running": 1})
            self.assertEqual(result["stages"]["cleanup"], {"pending": 1})
            self.assertEqual(sorted(root.iterdir()), before)

    def test_queue_failure_preserves_local_records_and_reports_error(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as tmp:
            root = Path(tmp)
            self.fixture(root)
            reader = Mock(side_effect=OSError("schedd unavailable"))
            result = build_snapshot(root, queue_reader=reader)
            self.assertEqual(result["queue"]["status"], "error")
            self.assertEqual(result["stages"]["plan"]["done"], 1)
            with self.assertRaisesRegex(ValueError, "conflicts"):
                build_snapshot(root, cluster=41, queue_reader=reader)
            self.assertEqual(reader.call_count, 1)

    def test_queue_query_uses_exact_recorded_root(self):
        with patch("tools.mcprod_snapshot.subprocess.run",
                   return_value=Mock(stdout="[]")) as run:
            self.assertEqual(query_queue("test-schedd", 42), [])
        self.assertEqual(run.call_args.args[0],
                         ["condor_q", "-name", "test-schedd", "-dag", "42", "-json"])
