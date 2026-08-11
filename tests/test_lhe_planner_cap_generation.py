#!/usr/bin/env python3
"""Regression test for existing-LHE generate-test planner event caps."""

from __future__ import annotations

import json
import subprocess
import tarfile
import tempfile
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
TMP_ROOT = Path("/tmp/chiw")


def main() -> int:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="planner_cap_generation_", dir=TMP_ROOT) as tmp:
        output_dir = Path(tmp) / "dag"
        log_root = Path(tmp) / "logs"
        command = [
            "python3",
            str(BASE_DIR / "dag_generator.py"),
            "generate-test",
            "--campaign",
            "JJP_DPS1",
            "--jobs",
            "1",
            "--max-events",
            "20",
            "--enable-lhe-block-subdags",
            "--skip-lhe-generation",
            "--no-scan-existing",
            "--lhe-events-per-block",
            "5",
            "--enable-ntuple",
            "--miniaod-merge-events",
            "10",
            "--log-root",
            str(log_root),
            "--output-dir",
            str(output_dir),
            "--output",
            "planner_cap_test.dag",
        ]
        subprocess.run(command, cwd=BASE_DIR, check=True)

        planning_configs = sorted((output_dir / "node_configs" / "planning").glob("*.json"))
        assert planning_configs, "no planner configs generated"
        payload = json.loads(planning_configs[0].read_text(encoding="utf-8"))
        assert payload["events_per_block"] == 5
        assert payload["max_events_per_plan"] == 20

        dag_text = (output_dir / "planner_cap_test.dag").read_text(encoding="utf-8")
        assert "JOB PLAN_" in dag_text
        assert "JOB LHE_" not in dag_text
        assert "SCRIPT POST " not in dag_text
        assert "\nFINAL SUMMARY " in dag_text
        assert 'archive_enabled="true"' in dag_text
        assert f'proxy_bundle_path="{output_dir / "proxy_bundle.tar.gz"}"' in dag_text
        assert f'log_source_root="{log_root}"' in dag_text
        assert "workflow_archive_id=" in dag_text

        metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
        archive_metadata = metadata["log_archive"]
        assert archive_metadata["enabled"] is True
        assert archive_metadata["mechanism"] == "top-level-final-worker"
        assert archive_metadata["source"] == "scan-log-root"
        assert archive_metadata["workflow_archive_id"]

        with tarfile.open(output_dir / "summary_runtime_bundle.tar.gz", "r:gz") as archive:
            members = set(archive.getnames())
        assert "runtime/processing/templates/summary.sh" in members
        assert "runtime/tools/archive_workflow_logs.py" in members
        assert (output_dir / "summary_final.sh").is_file()

    print("[OK] generate-test existing-LHE planner cap auto-follows --max-events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
