#!/usr/bin/env python3
"""Offline regression tests for the read-only mcprod snapshot."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tools.mcprod_snapshot import build_snapshot, collect_artifacts, query_queue


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


class McprodSnapshotTest(unittest.TestCase):
    def test_queue_query_uses_only_the_explicit_schedd_and_root(self):
        result = SimpleNamespace(stdout="[]\n", stderr="")
        with patch("tools.mcprod_snapshot.subprocess.run", return_value=result) as run:
            self.assertEqual(query_queue("bigbird26.cern.ch", 12758251), [])

        run.assert_called_once_with(
            [
                "condor_q",
                "-name",
                "bigbird26.cern.ch",
                "-dag",
                "12758251",
                "-json",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def make_run(self, root: Path, *, current_log: bool = True) -> Path:
        run = root / "run"
        (run / "steps").mkdir(parents=True)
        (run / "tasks").mkdir()
        (run / "logs").mkdir()
        (run / "ledger" / "waves").mkdir(parents=True)
        (run / "ledger" / "archives").mkdir()

        dag = "\n".join(
            [
                "SUBDAG EXTERNAL WAVE_000000 steps/000000.dag",
                "PRE_SKIP WAVE_000000 99",
                "SUBDAG EXTERNAL WAVE_000001 steps/000001.dag",
                "PRE_SKIP WAVE_000001 99",
                "SUBDAG EXTERNAL WAVE_000002 steps/000002.dag",
                "PRE_SKIP WAVE_000002 99",
                "SUBDAG EXTERNAL WAVE_000003 steps/000003.dag",
                "PRE_SKIP WAVE_000003 99",
                "SUBDAG EXTERNAL WAVE_000004 steps/000004.dag",
                "PRE_SKIP WAVE_000004 99",
            ]
        )
        (run / "run.dag").write_text(dag + "\n", encoding="utf-8")
        (run / "steps" / "000000.dag").write_text(
            "JOB PLAN_000000 plan.sub\n", encoding="utf-8"
        )
        (run / "steps" / "000001.dag").write_text(
            "\n".join(
                (
                    "JOB MIX_JOB000000_BLOCK000000 mix.sub",
                    "JOB MINIAOD_JOB000000_BLOCK000000 miniaod.sub",
                    "JOB ARCHIVE_000001 archive.sub",
                    "JOB RECLAIM_000001 reclaim.sub",
                    "PARENT MIX_JOB000000_BLOCK000000 "
                    "CHILD MINIAOD_JOB000000_BLOCK000000",
                    "PARENT MINIAOD_JOB000000_BLOCK000000 CHILD ARCHIVE_000001",
                    "PARENT ARCHIVE_000001 CHILD RECLAIM_000001",
                )
            )
            + "\n",
            encoding="utf-8",
        )
        for name, stage in (
            ("PLAN_000000", "plan"),
            ("MIX_JOB000000_BLOCK000000", "mix"),
            ("MINIAOD_JOB000000_BLOCK000000", "miniaod"),
            ("ARCHIVE_000001", "archive"),
            ("RECLAIM_000001", "reclaim"),
        ):
            write_json(
                run / "tasks" / f"{name}.json",
                {"task_id": name, "stage": stage},
            )

        root_lines = [
            "09/27/26 ** condor_scheduniv_exec.100.0 (CONDOR_DAGMAN) STARTING UP",
            "Node WAVE_000000 job proc (90.0.0) failed.",
        ]
        if current_log:
            root_lines.extend(
                (
                    "09/28/26 ** condor_scheduniv_exec.200.0 "
                    "(CONDOR_DAGMAN) STARTING UP",
                    "Event: ULOG_SUBMIT for HTCondor Node WAVE_000000 (201.0.0)",
                    "Event: ULOG_EXECUTE for HTCondor Node WAVE_000000 (201.0.0)",
                    "Node WAVE_000000 job proc (201.0.0) completed successfully.",
                    "Event: ULOG_SUBMIT for HTCondor Node WAVE_000001 (202.0.0)",
                    "Event: ULOG_EXECUTE for HTCondor Node WAVE_000001 (202.0.0)",
                    "Node WAVE_000001 job proc (202.0.0) completed successfully.",
                )
            )
            for index in (2, 3, 4):
                name = f"WAVE_{index:06d}"
                root_lines.extend(
                    (
                        f"PRE Script of Node {name} failed with status 99",
                        "PRE_SKIP return value 99 indicates we are done "
                        f"(successfully) with node {name}",
                        f"Event: ULOG_PRESKIP for HTCondor Node {name} (0.1.{index})",
                    )
                )
            root_lines.append(
                "**** condor_scheduniv_exec.200.0 "
                "(condor_DAGMAN) EXITING WITH STATUS 0"
            )
        (run / "run.dag.dagman.out").write_text(
            "\n".join(root_lines) + "\n", encoding="utf-8"
        )
        (run / "run.dag.dagman.log").write_text(
            "000 (100.000.000) old attempt\n005 (100.000.000) Job terminated.\n"
            "000 (200.000.000) current attempt\n005 (200.000.000) Job terminated.\n"
            "    (1) Normal termination (return value 0)\n",
            encoding="utf-8",
        )
        write_json(
            run / "run.dag.metrics",
            {"dagman_id": "200", "exitcode": 0, "DagStatus": 0,
             "dag_nodes": 5, "dag_nodes_succeeded": 5, "dag_nodes_failed": 0},
        )

        (run / "steps" / "000000.dag.dagman.out").write_text(
            "\n".join(
                (
                    "09/28/26 ** condor_scheduniv_exec.201.0 "
                    "(CONDOR_DAGMAN) STARTING UP",
                    "Event: ULOG_SUBMIT for HTCondor Node PLAN_000000 (301.0.0)",
                    "Event: ULOG_EXECUTE for HTCondor Node PLAN_000000 (301.0.0)",
                    "Node PLAN_000000 job proc (301.0.0) completed successfully.",
                    "**** condor_scheduniv_exec.201.0 "
                    "(condor_DAGMAN) EXITING WITH STATUS 0",
                )
            ) + "\n",
            encoding="utf-8",
        )
        (run / "steps" / "000001.dag.dagman.out").write_text(
            "\n".join(
                (
                    "09/28/26 ** condor_scheduniv_exec.202.0 "
                    "(CONDOR_DAGMAN) STARTING UP",
                    "Event: ULOG_SUBMIT for HTCondor Node "
                    "MIX_JOB000000_BLOCK000000 (302.0.0)",
                    "Node MIX_JOB000000_BLOCK000000 job proc "
                    "(302.0.0) completed successfully.",
                    "Event: ULOG_SUBMIT for HTCondor Node "
                    "MINIAOD_JOB000000_BLOCK000000 (303.0.0)",
                    "Node MINIAOD_JOB000000_BLOCK000000 job proc "
                    "(303.0.0) completed successfully.",
                    "Event: ULOG_SUBMIT for HTCondor Node ARCHIVE_000001 (304.0.0)",
                    "Node ARCHIVE_000001 job proc (304.0.0) completed successfully.",
                    "Event: ULOG_SUBMIT for HTCondor Node RECLAIM_000001 (305.0.0)",
                    "Node RECLAIM_000001 job proc (305.0.0) completed successfully.",
                    "**** condor_scheduniv_exec.202.0 "
                    "(condor_DAGMAN) EXITING WITH STATUS 0",
                )
            ) + "\n",
            encoding="utf-8",
        )

        miniaod = {
            "artifact_id": "MINIAOD_JOB000000_BLOCK000000",
            "kind": "miniaod",
            "events": 5,
            "size_bytes": 2048,
            "url": "root://products/run/miniaod.root",
            "sidecar_url": "root://products/run/miniaod.root.json",
        }
        archive = {
            "artifact_id": "ARCHIVE_000001",
            "kind": "upstream-archive",
            "size_bytes": 8192,
            "url": "root://archives/run/wave_000001.tar",
            "sidecar_url": "root://archives/run/wave_000001.tar.json",
        }
        block = {
            "artifact_id": "BLOCK:pool:source:000000",
            "kind": "lhe-block",
            "events": 5,
            "size_bytes": 1000,
            "url": "root://staging/run/block.lhe.gz",
        }
        write_json(
            run / "ledger" / "waves" / "000000.json",
            {
                "step": 0,
                "phase": "sources",
                "results": {
                    "PLAN_000000": {
                        "task_id": "PLAN_000000",
                        "stage": "plan",
                        "status": "ok",
                        "outputs": [block],
                        "originals": [],
                    }
                },
            },
        )
        write_json(
            run / "ledger" / "waves" / "000001.json",
            {
                "step": 1,
                "phase": "processing",
                "results": {
                    "MINIAOD_JOB000000_BLOCK000000": {
                        "task_id": "MINIAOD_JOB000000_BLOCK000000",
                        "stage": "miniaod",
                        "status": "ok",
                        "outputs": [miniaod],
                    },
                    "ARCHIVE_000001": {
                        "task_id": "ARCHIVE_000001",
                        "stage": "archive",
                        "status": "ok",
                        "outputs": [archive],
                        "members": [
                            {
                                "artifact_id": "HEPMC_JOB000000",
                                "kind": "hepmc",
                                "events": 5,
                                "size_bytes": 500,
                                "url": "root://staging/run/mix.hepmc.gz",
                                "member": "000001_mix.hepmc.gz",
                            }
                        ],
                    },
                    "RECLAIM_000001": {
                        "task_id": "RECLAIM_000001",
                        "stage": "reclaim",
                        "status": "ok",
                        "outputs": [],
                        "reclaimed": [
                            {
                                "artifact_id": block["artifact_id"],
                                "objects": [
                                    {
                                        "kind": "payload",
                                        "url": block["url"],
                                        "disposition": "removed",
                                    }
                                ],
                            }
                        ],
                    },
                },
            },
        )
        write_json(
            run / "ledger" / "archives" / "000001.json",
            {"step": 1, "task_id": "ARCHIVE_000001", "outputs": [archive],
             "members": [{"artifact_id": "HEPMC_JOB000000", "kind": "hepmc",
                          "events": 5, "size_bytes": 500,
                          "url": "root://staging/run/mix.hepmc.gz",
                          "member": "000001_mix.hepmc.gz"}]},
        )

        write_json(run / "run.json", {"run_id": "sample"})
        write_json(
            run / "state.json",
            {"status": "complete", "next_step": 2, "pending": {}},
        )
        (run / ".complete").write_text("", encoding="utf-8")
        dag_path = str((run / "run.dag").resolve())
        write_json(
            run / "submission.json",
            {
                "current_attempt": 2,
                "scheduler": "bigbird26.cern.ch",
                "root_cluster_id": "200",
                "run_dir": str(run.resolve()),
                "dag": dag_path,
                "attempts": [
                    {
                        "number": 1,
                        "scheduler": "bigbird26.cern.ch",
                        "root_cluster_id": "100",
                    },
                    {
                        "number": 2,
                        "scheduler": "bigbird26.cern.ch",
                        "root_cluster_id": "200",
                    },
                ],
                "last_status": {
                    "scheduler": "bigbird26.cern.ch",
                    "root_cluster_id": "100",
                },
            },
        )
        return run

    def test_current_attempt_queue_empty_uses_logs_and_archive_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            snapshot = build_snapshot(run, queue_query=lambda _schedd, _cluster: [])

        self.assertEqual((snapshot["attempt"], snapshot["cluster"]), (2, 200))
        self.assertEqual(snapshot["overall"], "complete")
        self.assertEqual(snapshot["queue"]["status"], "empty")
        self.assertEqual(snapshot["root"]["total"], 5)
        self.assertEqual(snapshot["root"]["completed"], 5)
        self.assertEqual(snapshot["root"]["counts"]["done"], 2)
        self.assertEqual(snapshot["root"]["counts"]["skipped"], 3)
        self.assertEqual(snapshot["root"]["counts"]["failed"], 0)
        self.assertEqual(
            [wave["status"] for wave in snapshot["waves"]],
            ["done", "done", "skipped", "skipped", "skipped"],
        )
        self.assertEqual(snapshot["waves"][0]["stages"]["plan"]["counts"]["done"], 1)
        self.assertTrue(
            snapshot["waves"][0]["summary"]["key_nodes"][0]["last_observed"]
        )
        self.assertEqual(snapshot["waves"][1]["stages"]["miniaod"]["counts"]["done"], 1)
        self.assertIn("archive", snapshot["stages"])
        self.assertIn("reclaim", snapshot["stages"])
        self.assertFalse(snapshot["remote_stat_checked"])

        artifacts = snapshot["artifacts"]["committed"]
        archive_outputs = [
            item for item in artifacts
            if item["artifact_id"] == "ARCHIVE_000001"
        ]
        self.assertEqual(len(archive_outputs), 1)
        self.assertEqual(
            archive_outputs[0]["sidecar_url"],
            "root://archives/run/wave_000001.tar.json",
        )
        member = next(
            item for item in artifacts
            if item["artifact_id"] == "HEPMC_JOB000000"
        )
        self.assertTrue(member["archive_member"])
        self.assertEqual(member["remote_presence"], "not_checked")
        self.assertEqual(member["sidecar_url"], "")
        mini = next(
            item for item in artifacts
            if item["artifact_id"].startswith("MINIAOD_")
        )
        self.assertEqual((mini["events"], mini["size_bytes"]), (5, 2048))
        self.assertTrue(mini["sidecar_url"].endswith(".root.json"))
        block = next(
            item for item in artifacts
            if item["artifact_id"].startswith("BLOCK:")
        )
        self.assertTrue(block["reclaimed_staging"])

    def test_htcondor_25_workflow_batch_id_restores_completed_stage_states(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            root_log = run / "run.dag.dagman.out"
            root_text = root_log.read_text(encoding="utf-8")
            root_text = root_text.replace(
                "09/28/26 ** condor_scheduniv_exec.200.0 "
                "(CONDOR_DAGMAN) STARTING UP",
                "09/28/26 Workflow batch-id: <200.0>",
            )
            root_log.write_text(root_text, encoding="utf-8")
            for step, cluster in ((0, 201), (1, 202)):
                child_log = run / "steps" / f"{step:06d}.dag.dagman.out"
                child_text = child_log.read_text(encoding="utf-8")
                child_text = child_text.replace(
                    f"09/28/26 ** condor_scheduniv_exec.{cluster}.0 "
                    "(CONDOR_DAGMAN) STARTING UP",
                    "09/28/26 Workflow batch-id: <200.0>",
                )
                child_log.write_text(child_text, encoding="utf-8")
            snapshot = build_snapshot(run, queue_query=lambda _schedd, _cluster: [])

        self.assertEqual(snapshot["overall"], "complete")
        self.assertTrue(snapshot["evidence"]["dagman_out_segment"])
        self.assertEqual(snapshot["root"]["counts"]["done"], 2)
        self.assertEqual(snapshot["root"]["counts"]["skipped"], 3)
        wave = next(item for item in snapshot["waves"] if item["name"] == "WAVE_000001")
        self.assertTrue(wave["child_log_matched"])
        self.assertEqual(wave["stages"]["mix"]["counts"]["done"], 1)
        self.assertEqual(wave["stages"]["miniaod"]["counts"]["done"], 1)

    def test_queue_failure_falls_back_to_persistent_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))

            def fail_query(_schedd, _cluster):
                raise subprocess.CalledProcessError(
                    1,
                    ["condor_q"],
                    stderr="unavailable",
                )

            snapshot = build_snapshot(run, queue_query=fail_query)

        self.assertEqual(snapshot["overall"], "complete")
        self.assertEqual(snapshot["queue"]["status"], "error")
        self.assertIn("unavailable", snapshot["queue"]["error"])

    def test_live_held_queue_state_overrides_persisted_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            live = [{
                "DAGNodeName": "MINIAOD_JOB000000_BLOCK000000",
                "ClusterId": 303,
                "ProcId": 0,
                "JobStatus": 5,
                "HoldReason": "test hold",
            }]
            snapshot = build_snapshot(run, queue_query=lambda *_: live)

        self.assertEqual(snapshot["overall"], "held")
        miniaod = snapshot["stages"]["miniaod"]
        self.assertEqual(miniaod["counts"]["held"], 1)
        held = next(node for node in miniaod["key_nodes"] if node["status"] == "held")
        self.assertEqual(held["source"], "queue")
        self.assertEqual(held["hold_reason"], "test hold")

    def test_conflicting_root_metrics_report_inconsistent(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            metrics_path = run / "run.dag.metrics"
            metrics = json.loads(metrics_path.read_text())
            metrics["DagStatus"] = 1
            write_json(metrics_path, metrics)
            snapshot = build_snapshot(run, queue_query=lambda *_: [])

        self.assertEqual(snapshot["overall"], "inconsistent")

    def test_identity_conflicts_and_ambiguous_current_attempt_are_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            with self.assertRaisesRegex(ValueError, "--schedd conflicts"):
                build_snapshot(run, schedd="other.example", queue_query=lambda *_: [])
            with self.assertRaisesRegex(ValueError, "--cluster conflicts"):
                build_snapshot(run, cluster=100, queue_query=lambda *_: [])
            submission_path = run / "submission.json"
            submission = json.loads(submission_path.read_text())
            submission["attempts"].append(dict(submission["attempts"][1]))
            write_json(submission_path, submission)
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                build_snapshot(run, queue_query=lambda *_: [])

    def test_missing_current_log_segment_does_not_reuse_old_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp), current_log=False)
            snapshot = build_snapshot(run, queue_query=lambda *_: [])

        self.assertEqual(snapshot["overall"], "unknown")
        self.assertFalse(snapshot["evidence"]["dagman_out_segment"])
        self.assertEqual(snapshot["root"]["counts"]["failed"], 0)
        self.assertEqual(snapshot["root"]["counts"]["done"], 0)
        self.assertIn(
            "current root DAGMan output segment", snapshot["evidence"]["missing"]
        )

    def test_missing_current_child_log_does_not_reuse_old_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            child_log = run / "steps" / "000001.dag.dagman.out"
            child_log.write_text(
                "09/27/26 ** condor_scheduniv_exec.190.0 "
                "(CONDOR_DAGMAN) STARTING UP\n"
                "Node MINIAOD_JOB000000_BLOCK000000 job proc "
                "(190.0.0) completed successfully.\n",
                encoding="utf-8",
            )
            snapshot = build_snapshot(run, queue_query=lambda *_: [])

        self.assertEqual(snapshot["overall"], "unknown")
        wave = next(item for item in snapshot["waves"] if item["name"] == "WAVE_000001")
        self.assertFalse(wave["child_log_matched"])
        self.assertEqual(wave["stages"]["miniaod"]["counts"]["done"], 0)
        self.assertIn(
            "current WAVE_000001 SubDAG log segment",
            snapshot["evidence"]["missing"],
        )

    def test_current_results_are_uncommitted_and_snapshot_does_not_write_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            state_path = run / "state.json"
            state = json.loads(state_path.read_text())
            state["status"] = "processing"
            state["pending"] = {"step": 2, "task_ids": ["MINIAOD_CURRENT"]}
            write_json(state_path, state)
            write_json(run / "results" / "MINIAOD_CURRENT.json", {
                "task_id": "MINIAOD_CURRENT", "stage": "miniaod", "status": "ok",
                "outputs": [{"artifact_id": "MINIAOD_CURRENT", "kind": "miniaod",
                             "events": 77, "size_bytes": 9000,
                             "url": "root://products/current.root"}],
            })
            protected = [
                run / "submission.json",
                state_path,
                run / "results" / "MINIAOD_CURRENT.json",
                run / "ledger" / "waves" / "000000.json",
                run / "ledger" / "waves" / "000001.json",
                run / "ledger" / "archives" / "000001.json",
            ]
            before = {
                path: (path.read_bytes(), path.stat().st_mtime_ns)
                for path in protected
            }

            artifacts = collect_artifacts(run, state)

            after = {
                path: (path.read_bytes(), path.stat().st_mtime_ns)
                for path in protected
            }

        self.assertEqual(before, after)
        self.assertEqual(len(artifacts["uncommitted"]), 1)
        self.assertFalse(artifacts["uncommitted"][0]["merged"])
        self.assertEqual(artifacts["uncommitted"][0]["events"], 77)
        self.assertFalse(artifacts["remote_stat_checked"])


if __name__ == "__main__":
    unittest.main()
