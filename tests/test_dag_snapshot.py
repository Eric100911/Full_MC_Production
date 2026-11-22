#!/usr/bin/env python3
"""Tests for the concise legacy DAG snapshot."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools import dag_progress


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "skills/cms-condor-job-operator/scripts/dag_snapshot.py"
)
SPEC = importlib.util.spec_from_file_location("dag_snapshot", SCRIPT)
dag_snapshot = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dag_snapshot)


class DagSnapshotTest(unittest.TestCase):
    def test_explicit_dag_file_counts_preskips_as_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            dag = Path(tmp) / "root.dag"
            lines = []
            log = []
            for index in range(12):
                name = f"PLAN_POOL_{index}"
                lines.append(f"JOB {name} plan.sub")
                if index < 3:
                    lines.append(f"PRE_SKIP {name} 99")
                    log.extend(
                        (
                            f"PRE Script of Node {name} failed with status 99",
                            "Event: ULOG_PRESKIP for HTCondor Node "
                            f"{name} (0.{index}.0)",
                        )
                    )
                else:
                    log.append(
                        f"Node {name} job proc (20.{index}.0) "
                        "completed successfully."
                    )
            dag.write_text("\n".join(lines), encoding="utf-8")
            Path(f"{dag}.dagman.out").write_text("\n".join(log), encoding="utf-8")

            with patch.object(dag_progress, "query_jobs", return_value=[]):
                snapshot = dag_snapshot.build_snapshot(
                    dag_progress, "schedd.example", 42, dag
                )
            rendered = dag_snapshot.render_text(snapshot, max_active=0)

        self.assertEqual(snapshot["assessment"], "complete")
        self.assertEqual(snapshot["logical"]["completed"], 12)
        self.assertEqual(snapshot["logical"]["skipped"], 3)
        self.assertIn("12/12 complete (9 done, 3 skipped)", rendered)


if __name__ == "__main__":
    unittest.main()
