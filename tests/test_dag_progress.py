#!/usr/bin/env python3
"""Unit tests for tools/dag_progress.py."""

import tempfile
import unittest
from pathlib import Path

from tools.dag_progress import (
    completed_records_from_logs,
    format_age,
    latest_node_records,
    parse_dag_tree,
    progress_bar,
    record_status,
)


class DagProgressTest(unittest.TestCase):
    def test_parses_nested_dag_stages(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "root.dag"
            child = Path(tmp) / "child.dag"
            root.write_text(
                "\n".join(
                    (
                        "JOB PLAN_POOL_1 plan.sub",
                        "JOB COORD_CAMPAIGN_1 coord.sub",
                        "SUBDAG EXTERNAL MIX_CAMPAIGN_1 child.dag",
                    )
                ),
                encoding="utf-8",
            )
            child.write_text(
                "\n".join(
                    (
                        "JOB MIX_CAMPAIGN_1_BLOCK000000 processing.sub",
                        "JOB MERGE_CAMPAIGN_1_GROUP000000 merge.sub",
                        "JOB NTUPLE_CAMPAIGN_1_MERGE000000 ntuple.sub",
                        "FINAL FINAL_CAMPAIGN_1 final.sub",
                    )
                ),
                encoding="utf-8",
            )
            stages = {
                name: node.stage for name, node in parse_dag_tree(root).items()
            }

        self.assertEqual(stages["PLAN_POOL_1"], "plan")
        self.assertEqual(stages["COORD_CAMPAIGN_1"], "coordinate")
        self.assertEqual(stages["MIX_CAMPAIGN_1"], "subdag")
        self.assertEqual(stages["MIX_CAMPAIGN_1_BLOCK000000"], "processing")
        self.assertEqual(stages["MERGE_CAMPAIGN_1_GROUP000000"], "merge")
        self.assertEqual(stages["NTUPLE_CAMPAIGN_1_MERGE000000"], "ntuple")
        self.assertEqual(stages["FINAL_CAMPAIGN_1"], "final")

    def test_live_retry_wins_over_failed_history_attempt(self):
        history = [
            {
                "DAGNodeName": "MERGE_X",
                "JobStatus": 4,
                "ExitCode": 1,
                "CompletionDate": 100,
            }
        ]
        live = [
            {
                "DAGNodeName": "MERGE_X",
                "JobStatus": 2,
                "EnteredCurrentStatus": 101,
            }
        ]
        selected = latest_node_records(live, history)
        self.assertEqual(record_status(selected["MERGE_X"]), "running")

    def test_reads_latest_terminal_state_from_dagman_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            dag = Path(tmp) / "root.dag"
            dag.write_text("JOB PLAN_POOL_1 plan.sub\n", encoding="utf-8")
            Path(f"{dag}.dagman.out").write_text(
                "\n".join(
                    (
                        "Node PLAN_POOL_1 job proc (10.0.0) failed.",
                        "Node PLAN_POOL_1 job proc (11.0.0) completed successfully.",
                    )
                ),
                encoding="utf-8",
            )
            planned = parse_dag_tree(dag)
            records = completed_records_from_logs(planned)

        self.assertEqual(len(records), 1)
        self.assertEqual(record_status(records[0]), "done")

    def test_nonzero_completed_exit_is_failed(self):
        self.assertEqual(
            record_status({"JobStatus": 4, "ExitCode": 17}),
            "failed",
        )
        self.assertEqual(record_status({"JobStatus": 4, "ExitCode": 0}), "done")

    def test_display_helpers(self):
        self.assertEqual(format_age(3661), "1h01m")
        self.assertEqual(progress_bar(1, 2, width=4), "██░░")


if __name__ == "__main__":
    unittest.main()
