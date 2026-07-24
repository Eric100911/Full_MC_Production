#!/usr/bin/env python3
"""Unit tests for tools/dag_progress.py."""

import tempfile
import unittest
from pathlib import Path

from tools.dag_progress import (
    DagGraph,
    PlannedNode,
    completed_records_from_logs,
    flatten_graph,
    format_age,
    latest_node_records,
    node_stage,
    parse_dag_graph,
    parse_dag_tree,
    progress_bar,
    record_status,
    render,
    render_structure,
    resolve_color_mode,
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

    def test_parses_dependencies_categories_limits_and_missing_subdag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "root.dag"
            missing = Path(tmp) / "not_generated.dag"
            root.write_text(
                "\n".join(
                    (
                        "MAXJOBS lhe 3",
                        "JOB LHE_POOL_0 lhe.sub",
                        "CATEGORY LHE_POOL_0 lhe",
                        "JOB PLAN_POOL_0 plan.sub",
                        "PARENT LHE_POOL_0 CHILD PLAN_POOL_0",
                        f"SUBDAG EXTERNAL MIX_CAMPAIGN_0 {missing}",
                        "PARENT PLAN_POOL_0 CHILD MIX_CAMPAIGN_0",
                    )
                ),
                encoding="utf-8",
            )
            graph = parse_dag_graph(root)

        self.assertEqual(graph.maxjobs, {"lhe": 3})
        self.assertEqual(graph.nodes["LHE_POOL_0"].stage, "lhe")
        self.assertEqual(graph.nodes["LHE_POOL_0"].category, "lhe")
        self.assertEqual(
            graph.edges,
            [
                ("LHE_POOL_0", "PLAN_POOL_0"),
                ("PLAN_POOL_0", "MIX_CAMPAIGN_0"),
            ],
        )
        self.assertIsNone(graph.children["MIX_CAMPAIGN_0"])
        self.assertEqual(
            graph.nodes["MIX_CAMPAIGN_0"].child_dag,
            missing.resolve(),
        )

    def test_parent_child_cartesian_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "root.dag"
            root.write_text(
                "\n".join(
                    (
                        "JOB PLAN_A_0 plan.sub",
                        "JOB PLAN_B_0 plan.sub",
                        "JOB COORD_A_0 coord.sub",
                        "JOB COORD_B_0 coord.sub",
                        "PARENT PLAN_A_0 PLAN_B_0 CHILD COORD_A_0 COORD_B_0",
                    )
                ),
                encoding="utf-8",
            )
            graph = parse_dag_graph(root)

        self.assertEqual(
            set(graph.edges),
            {
                ("PLAN_A_0", "COORD_A_0"),
                ("PLAN_A_0", "COORD_B_0"),
                ("PLAN_B_0", "COORD_A_0"),
                ("PLAN_B_0", "COORD_B_0"),
            },
        )

    def test_full_structure_shows_layers_parents_final_and_pending_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "root.dag"
            root.write_text(
                "\n".join(
                    (
                        "JOB "
                        "PLAN_A_VERY_LONG_POOL_NAME_THAT_EXCEEDS_THE_TERMINAL_WIDTH_0 "
                        "plan.sub",
                        "JOB "
                        "PLAN_B_VERY_LONG_POOL_NAME_THAT_EXCEEDS_THE_TERMINAL_WIDTH_0 "
                        "plan.sub",
                        "JOB COORD_CAMPAIGN_WITH_A_LONG_NAME_0 coord.sub",
                        "PARENT "
                        "PLAN_A_VERY_LONG_POOL_NAME_THAT_EXCEEDS_THE_TERMINAL_WIDTH_0 "
                        "PLAN_B_VERY_LONG_POOL_NAME_THAT_EXCEEDS_THE_TERMINAL_WIDTH_0 "
                        "CHILD COORD_CAMPAIGN_WITH_A_LONG_NAME_0",
                        "SUBDAG EXTERNAL MIX_CAMPAIGN_WITH_A_LONG_NAME_0 "
                        "missing.dag",
                        "PARENT COORD_CAMPAIGN_WITH_A_LONG_NAME_0 "
                        "CHILD MIX_CAMPAIGN_WITH_A_LONG_NAME_0",
                        "FINAL SUMMARY summary.sub",
                    )
                ),
                encoding="utf-8",
            )
            graph = parse_dag_graph(root)
            output = "\n".join(
                render_structure(graph, {}, "full", 60, False)
            )

        self.assertIn("DAG structure (full):", output)
        self.assertIn("Layer 1:", output)
        self.assertIn("← PLAN_A_", output)
        self.assertIn("child DAG pending:", output)
        self.assertIn("[FINAL: after DAG termination]", output)
        self.assertIn("Shortened node names:", output)

    def test_collapsed_structure_preserves_shared_plan_fanout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "root.dag"
            root.write_text(
                "\n".join(
                    (
                        "JOB LHE_SOURCE_0 lhe.sub",
                        "JOB PLAN_SHARED_0 plan.sub",
                        "JOB COORD_CAMPAIGN_A_0 coord.sub",
                        "JOB COORD_CAMPAIGN_B_0 coord.sub",
                        "PARENT LHE_SOURCE_0 CHILD PLAN_SHARED_0",
                        "PARENT PLAN_SHARED_0 CHILD COORD_CAMPAIGN_A_0",
                        "PARENT PLAN_SHARED_0 CHILD COORD_CAMPAIGN_B_0",
                        "SUBDAG EXTERNAL MIX_CAMPAIGN_A_0 missing_a.dag",
                        "SUBDAG EXTERNAL MIX_CAMPAIGN_B_0 missing_b.dag",
                        "PARENT COORD_CAMPAIGN_A_0 CHILD MIX_CAMPAIGN_A_0",
                        "PARENT COORD_CAMPAIGN_B_0 CHILD MIX_CAMPAIGN_B_0",
                    )
                ),
                encoding="utf-8",
            )
            graph = parse_dag_graph(root)
            output = "\n".join(
                render_structure(graph, {}, "collapsed", 100, False)
            )

        self.assertIn("LHE_SOURCE ×1", output)
        self.assertIn("→ PLAN_SHARED", output)
        self.assertIn("PLAN_SHARED ×1", output)
        self.assertIn("→ CAMPAIGN_A, CAMPAIGN_B", output)
        self.assertIn("Campaign CAMPAIGN_A (1 source jobs):", output)
        self.assertIn("Child DAG pending: 1 controller(s)", output)

    def test_auto_structure_switches_after_150_nodes(self):
        def graph_with_nodes(count):
            graph = DagGraph(Path("/tmp/test.dag"))
            for index in range(count):
                name = f"LHE_POOL_{index}"
                graph.nodes[name] = PlannedNode(
                    name,
                    "lhe",
                    graph.dag_file,
                )
            return graph

        full = render_structure(graph_with_nodes(150), {}, "auto", 80, False)
        collapsed = render_structure(
            graph_with_nodes(151),
            {},
            "auto",
            80,
            False,
        )
        self.assertIn("DAG structure (full):", full)
        self.assertIn("DAG structure (campaign-collapsed):", collapsed)

    def test_color_modes_and_status_ansi(self):
        self.assertTrue(resolve_color_mode("always", False, {"NO_COLOR": "1"}))
        self.assertFalse(resolve_color_mode("never", True, {}))
        self.assertTrue(resolve_color_mode("auto", True, {}))
        self.assertFalse(resolve_color_mode("auto", True, {"NO_COLOR": ""}))
        self.assertFalse(resolve_color_mode("auto", False, {}))

        graph = DagGraph(Path("/tmp/test.dag"))
        graph.nodes["LHE_POOL_0"] = PlannedNode(
            "LHE_POOL_0",
            "lhe",
            graph.dag_file,
        )
        records = {"LHE_POOL_0": {"JobStatus": 2}}
        colored = "\n".join(
            render_structure(graph, records, "full", 80, True)
        )
        plain = "\n".join(
            render_structure(graph, records, "full", 80, False)
        )
        self.assertIn("\033[36m", colored)
        self.assertNotIn("\033[", plain)

    def test_rejects_unsupported_structure_names(self):
        graph = DagGraph(Path("/tmp/test.dag"))
        graph.nodes["UNRELATED_NODE"] = PlannedNode(
            "UNRELATED_NODE",
            "processing",
            graph.dag_file,
        )
        with self.assertRaisesRegex(RuntimeError, "unsupported"):
            render_structure(graph, {}, "full", 80, False)

    def test_lhe_stage_and_flat_workflow_names(self):
        self.assertEqual(node_stage("LHE_POOL_0", "JOB"), "lhe")
        self.assertEqual(node_stage("PROC_CAMPAIGN_0", "JOB"), "processing")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "flat.dag"
            root.write_text(
                "\n".join(
                    (
                        "JOB LHE_POOL_0 lhe.sub",
                        "JOB PROC_CAMPAIGN_0 processing.sub",
                        "JOB NTUPLE_CAMPAIGN_0 ntuple.sub",
                        "PARENT LHE_POOL_0 CHILD PROC_CAMPAIGN_0",
                        "PARENT PROC_CAMPAIGN_0 CHILD NTUPLE_CAMPAIGN_0",
                    )
                ),
                encoding="utf-8",
            )
            graph = parse_dag_graph(root)
            collapsed = "\n".join(
                render_structure(graph, {}, "collapsed", 80, False)
            )
            dashboard = render(
                "test-schedd",
                1,
                root,
                [
                    {
                        "DAGNodeName": "LHE_POOL_0",
                        "JobStatus": 2,
                        "ClusterId": 2,
                        "ProcId": 0,
                    }
                ],
                False,
                color_enabled=True,
            )

        self.assertEqual(len(flatten_graph(graph)), 3)
        self.assertIn("Campaign CAMPAIGN (1 source jobs):", collapsed)
        self.assertIn("Process ─1→ Ntuple", collapsed)
        self.assertIn("LHE → Plan", dashboard)
        self.assertIn("\033[36m", dashboard)


if __name__ == "__main__":
    unittest.main()
