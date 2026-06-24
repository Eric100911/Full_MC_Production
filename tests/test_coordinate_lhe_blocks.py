#!/usr/bin/env python3
"""Regression tests for block coordinator multiplicity and output IDs."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
COORDINATOR = BASE_DIR / "tools" / "coordinate_lhe_blocks.py"
TMP_ROOT = Path("/tmp/chiw")
TARGET_BASE = "root://cceos.ihep.ac.cn:1094//store/user/chiw/JpsiJpsiPhi_MC_Production_v4"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def make_manifest(path: Path, pool: str, group_id: str, n_blocks: int) -> None:
    write_json(
        path,
        {
            "pool": pool,
            "group_id": group_id,
            "primary_seed": 100,
            "seeds": [100],
            "blocks": [
                {
                    "index": index,
                    "n_events": 1000,
                    "path": f"{TARGET_BASE}/lhe_pools/{pool}/lhe_blocks/{group_id}/"
                    f"block_{group_id}_{index:06d}.lhe.gz",
                }
                for index in range(n_blocks)
            ],
        },
    )


def run_coordinator(
    workdir: Path,
    campaign: str,
    job_index: int,
    campaign_inputs: list[str],
    shower_modes: list[str],
    source_infos: list[dict[str, object]],
) -> Path:
    output_dir = workdir / campaign
    subdag_path = output_dir / "blocks_processing.dag"
    command = [
        "python3",
        str(COORDINATOR),
        "--campaign",
        campaign,
        "--job-index",
        str(job_index),
        "--source-manifests",
        json.dumps(source_infos),
        "--shower-modes",
        ",".join(shower_modes),
        "--campaign-inputs",
        ",".join(campaign_inputs),
        "--analysis-type",
        "JJP",
        "--n-sources",
        str(len(campaign_inputs)),
        "--max-events",
        "-1",
        "--enable-ntuple",
        "--cleanup",
        "--output-dir",
        str(output_dir),
        "--processing-sub-template-path",
        "processing.sub",
        "--processing-bundle-path",
        "processing_runtime_bundle.tar.gz",
        "--processing-bundle-name",
        "processing_runtime_bundle.tar.gz",
        "--proxy-bundle-path",
        "proxy_bundle.tar.gz",
        "--proxy-bundle-name",
        "proxy_bundle.tar.gz",
        "--processing-wrapper-path",
        "run_processing.sh",
        "--ntuple-sub-template-path",
        "ntuple.sub",
        "--ntuple-bundle-path",
        "ntuple_runtime_bundle.tar.gz",
        "--ntuple-bundle-name",
        "ntuple_runtime_bundle.tar.gz",
        "--ntuple-wrapper-path",
        "run_ntuple_only.sh",
        "--subdag-output-path",
        str(subdag_path),
        "--target-eos-base",
        TARGET_BASE,
        "--storage-config",
        json.dumps({"target_eos_base": TARGET_BASE}),
    ]
    subprocess.run(command, check=True, cwd=BASE_DIR)
    return subdag_path


def main() -> int:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="coordinate_blocks_", dir=TMP_ROOT) as tmp:
        workdir = Path(tmp)

        jpsi_manifest = workdir / "jpsi_manifest.json"
        gg_manifest = workdir / "gg_manifest.json"
        make_manifest(jpsi_manifest, "pool_jpsi_CSCO_g", "jpsi_group", 6)
        make_manifest(gg_manifest, "pool_gg", "gg_group", 3)

        tps_subdag = run_coordinator(
            workdir,
            "JJP_TPS",
            7,
            ["pool_jpsi_CSCO_g", "pool_jpsi_CSCO_g", "pool_gg"],
            ["normal", "normal", "phi_mpi_off"],
            [
                {
                    "pool": "pool_jpsi_CSCO_g",
                    "group_id": "jpsi_group",
                    "primary_seed": 100,
                    "seeds": [100],
                    "path": str(jpsi_manifest),
                },
                {
                    "pool": "pool_gg",
                    "group_id": "gg_group",
                    "primary_seed": 40100,
                    "seeds": [40100],
                    "path": str(gg_manifest),
                },
            ],
        )
        tps_dag = tps_subdag.read_text(encoding="utf-8")
        assert tps_dag.count("\nJOB MIX_JJP_TPS_7_BLOCK") == 3

        first_config = workdir / "JJP_TPS" / "node_configs" / "processing" / (
            "MIX_JJP_TPS_7_BLOCK000000.json"
        )
        first = json.loads(first_config.read_text(encoding="utf-8"))
        assert first["job_id"] == "JOB000007_BLOCK000000"
        assert first["enable_ntuple"] is False
        assert first["inputs"] == [
            "BLOCK:pool_jpsi_CSCO_g:jpsi_group:000000",
            "BLOCK:pool_jpsi_CSCO_g:jpsi_group:000001",
            "BLOCK:pool_gg:gg_group:000000",
        ]

        ntuple_config = workdir / "JJP_TPS" / "node_configs" / "ntuple" / (
            "NTUPLE_JJP_TPS_7_BLOCK000000.json"
        )
        ntuple = json.loads(ntuple_config.read_text(encoding="utf-8"))
        assert ntuple["miniaod_input"] == (
            f"{TARGET_BASE}/output/JJP_TPS/JOB000007_BLOCK000000/output_MINIAOD.root"
        )

        sps_manifest = workdir / "sps_manifest.json"
        make_manifest(sps_manifest, "pool_2jpsi_cs", "sps_group", 2)
        sps_subdag = run_coordinator(
            workdir,
            "JJP_SPS_CS",
            4,
            ["pool_2jpsi_cs"],
            ["phi_mpi_off"],
            [
                {
                    "pool": "pool_2jpsi_cs",
                    "group_id": "sps_group",
                    "primary_seed": 60100,
                    "seeds": [60100],
                    "path": str(sps_manifest),
                }
            ],
        )
        sps_dag = sps_subdag.read_text(encoding="utf-8")
        assert sps_dag.count("\nJOB MIX_JJP_SPS_CS_4_BLOCK") == 2

    print("[OK] Block coordinator TPS multiplicity and unique output IDs")
    print("[OK] Block coordinator single-source SPS SubDAG generation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
