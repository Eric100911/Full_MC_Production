#!/usr/bin/env python3
"""Regression tests for counted existing-LHE inventory grouping."""

from __future__ import annotations

import gzip
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import sys


BASE_DIR = Path(__file__).resolve().parents[1]
TMP_ROOT = Path("/tmp/chiw")
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR / "tests"))

from dag_generator import pool_storage_name  # noqa: E402
from generate_synthetic_lhe import generate  # noqa: E402


def write_gzipped_lhe(path: Path, n_events: int) -> None:
    plain = path.with_suffix("")
    plain.parent.mkdir(parents=True, exist_ok=True)
    generate(str(plain), n_events=n_events)
    with open(plain, "rb") as source, gzip.open(path, "wb") as target:
        shutil.copyfileobj(source, target)
    plain.unlink()


def run_command(command: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        cwd=BASE_DIR,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def main() -> int:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="existing_lhe_inventory_", dir=TMP_ROOT) as tmp:
        workdir = Path(tmp)
        local_base = workdir / "local"
        pool_dir = local_base / "lhe_pools" / pool_storage_name("pool_2jpsi_cs")
        first = pool_dir / "sample_pool_2jpsi_cs_60100.lhe.gz"
        second = pool_dir / "sample_pool_2jpsi_cs_60101.lhe.gz"
        write_gzipped_lhe(first, 17)
        write_gzipped_lhe(second, 23)

        bounded_inventory_path = workdir / "bounded_inventory.json"
        run_command(
            [
                "python3",
                "dag_generator.py",
                "scan-lhe-inventory",
                "--campaign",
                "JJP_SPS_CS",
                "--local-output-base",
                str(local_base),
                "--output",
                str(bounded_inventory_path),
                "--count-events",
                "--max-files-per-pool",
                "1",
            ]
        )
        bounded_inventory = json.loads(bounded_inventory_path.read_text(encoding="utf-8"))
        bounded_records = bounded_inventory["pools"]["pool_2jpsi_cs"]["files"]
        assert [record["actual_events"] for record in bounded_records] == [17]
        assert [record["seed"] for record in bounded_records] == [60100]

        inventory_path = workdir / "counted_inventory.json"
        run_command(
            [
                "python3",
                "dag_generator.py",
                "scan-lhe-inventory",
                "--campaign",
                "JJP_SPS_CS",
                "--local-output-base",
                str(local_base),
                "--output",
                str(inventory_path),
                "--count-events",
            ]
        )
        inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
        records = inventory["pools"]["pool_2jpsi_cs"]["files"]
        assert [record["actual_events"] for record in records] == [17, 23]
        assert [record["seed"] for record in records] == [60100, 60101]

        output_dir = workdir / "grouped_dag"
        run_command(
            [
                "python3",
                "dag_generator.py",
                "generate-test",
                "--campaign",
                "JJP_SPS_CS",
                "--jobs",
                "1",
                "--max-events",
                "20",
                "--enable-lhe-block-subdags",
                "--skip-lhe-generation",
                "--existing-lhe-inventory",
                str(inventory_path),
                "--lhe-group-min-events",
                "30",
                "--lhe-group-max-events",
                "40",
                "--lhe-group-max-files",
                "2",
                "--lhe-events-per-block",
                "10",
                "--disable-ntuple",
                "--output-dir",
                str(output_dir),
                "--output",
                "grouped_counted.dag",
            ]
        )
        planning_configs = sorted((output_dir / "node_configs" / "planning").glob("*.json"))
        assert len(planning_configs) == 1
        planning = json.loads(planning_configs[0].read_text(encoding="utf-8"))
        assert planning["lhe_paths"] == [str(first), str(second)]
        assert planning["lhe_event_counts"] == [17, 23]

        old_inventory_path = workdir / "old_inventory.json"
        old_inventory_path.write_text(
            json.dumps(
                {
                    "pools": {
                        "pool_2jpsi_cs": {
                            "files": [str(first), str(second)],
                        }
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        failed = run_command(
            [
                "python3",
                "dag_generator.py",
                "generate-test",
                "--campaign",
                "JJP_SPS_CS",
                "--jobs",
                "1",
                "--max-events",
                "20",
                "--enable-lhe-block-subdags",
                "--skip-lhe-generation",
                "--existing-lhe-inventory",
                str(old_inventory_path),
                "--lhe-group-min-events",
                "30",
                "--lhe-events-per-block",
                "10",
                "--disable-ntuple",
                "--output-dir",
                str(workdir / "old_grouped_dag"),
                "--output",
                "old_grouped.dag",
            ],
            check=False,
        )
        assert failed.returncode != 0
        assert "requires positive actual_events" in failed.stderr

    print("[OK] counted existing-LHE inventory controls grouped DAG generation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
