#!/usr/bin/env python3
"""Regression test for existing-LHE generate-test planner event caps."""

from __future__ import annotations

import json
import subprocess
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

    print("[OK] generate-test existing-LHE planner cap auto-follows --max-events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
