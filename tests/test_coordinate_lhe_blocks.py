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
TARGET_BASE = "root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4"


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
    miniaod_merge_events: int = 0,
) -> Path:
    output_dir = workdir / campaign
    log_root = workdir / "logs"
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
        "--log-root",
        str(log_root),
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
        "--miniaod-merge-events",
        str(miniaod_merge_events),
        "--miniaod-merge-sub-template-path",
        "miniaod_merge.sub",
        "--miniaod-merge-wrapper-path",
        "run_miniaod_merge.sh",
        "--final-sub-template-path",
        "subdag_final.sub",
        "--final-wrapper-path",
        "run_subdag_final_inventory.sh",
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
        assert "\nFINAL FINAL_JJP_TPS_7 subdag_final.sub" in tps_dag
        processing_log = (
            workdir
            / "logs"
            / "JJP_TPS"
            / "processing"
            / "job_000007"
            / "block_000000"
        )
        ntuple_log = (
            workdir
            / "logs"
            / "JJP_TPS"
            / "ntuple"
            / "job_000007"
            / "block_000000"
        )
        assert processing_log.is_dir()
        assert ntuple_log.is_dir()
        assert f'log_root="{processing_log}"' in tps_dag
        assert f'log_root="{ntuple_log}"' in tps_dag

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

        merge_subdag = run_coordinator(
            workdir,
            "JJP_TPS",
            8,
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
            miniaod_merge_events=5000,
        )
        merge_dag = merge_subdag.read_text(encoding="utf-8")
        assert merge_dag.count("\nJOB MIX_JJP_TPS_8_BLOCK") == 3
        assert merge_dag.count("\nJOB MERGE_JJP_TPS_8_GROUP") == 1
        assert merge_dag.count("\nJOB NTUPLE_JJP_TPS_8_MERGE") == 1
        assert "\nJOB NTUPLE_JJP_TPS_8_BLOCK" not in merge_dag
        merge_config = workdir / "JJP_TPS" / "node_configs" / "miniaod_merge" / (
            "MERGE_JJP_TPS_8_GROUP000000.json"
        )
        merge_payload = json.loads(merge_config.read_text(encoding="utf-8"))
        assert merge_payload["expected_events"] == 3000
        assert [item["job_id"] for item in merge_payload["input_miniaods"]] == [
            "JOB000008_BLOCK000000",
            "JOB000008_BLOCK000001",
            "JOB000008_BLOCK000002",
        ]
        merged_ntuple_config = workdir / "JJP_TPS" / "node_configs" / "ntuple" / (
            "NTUPLE_JJP_TPS_8_MERGE000000.json"
        )
        merged_ntuple = json.loads(merged_ntuple_config.read_text(encoding="utf-8"))
        assert merged_ntuple["miniaod_input"] == (
            f"{TARGET_BASE}/output/JJP_TPS/JOB000008_MERGE000000/output_MINIAOD.root"
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

        sps6_manifest = workdir / "sps6_manifest.json"
        make_manifest(sps6_manifest, "pool_2jpsi_cs", "sps6_group", 6)
        sps6_subdag = run_coordinator(
            workdir,
            "JJP_SPS_CS",
            5,
            ["pool_2jpsi_cs"],
            ["phi_mpi_off"],
            [
                {
                    "pool": "pool_2jpsi_cs",
                    "group_id": "sps6_group",
                    "primary_seed": 60101,
                    "seeds": [60101],
                    "path": str(sps6_manifest),
                }
            ],
            miniaod_merge_events=5000,
        )
        sps6_dag = sps6_subdag.read_text(encoding="utf-8")
        assert sps6_dag.count("\nJOB MERGE_JJP_SPS_CS_5_GROUP") == 2
        first_merge = json.loads(
            (
                workdir / "JJP_SPS_CS" / "node_configs" / "miniaod_merge" /
                "MERGE_JJP_SPS_CS_5_GROUP000000.json"
            ).read_text(encoding="utf-8")
        )
        second_merge = json.loads(
            (
                workdir / "JJP_SPS_CS" / "node_configs" / "miniaod_merge" /
                "MERGE_JJP_SPS_CS_5_GROUP000001.json"
            ).read_text(encoding="utf-8")
        )
        assert len(first_merge["input_miniaods"]) == 5
        assert first_merge["expected_events"] == 5000
        assert len(second_merge["input_miniaods"]) == 1
        assert second_merge["expected_events"] == 1000

    print("[OK] Block coordinator TPS multiplicity and unique output IDs")
    print("[OK] Block coordinator single-source SPS SubDAG generation")
    print("[OK] Block processing and ntuple logs use per-job, per-block directories")
    print("[OK] MiniAOD merge mode emits merge-group ntuple nodes and provenance configs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
