#!/usr/bin/env python3
"""Regression tests for block coordinator multiplicity and output IDs."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
COORDINATOR = BASE_DIR / "tools" / "coordinate_lhe_blocks.py"
TMP_ROOT = Path("/tmp/chiw")
TARGET_BASE = "root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4"

sys.path.insert(0, str(BASE_DIR))
from tools.coordinate_lhe_blocks import (  # noqa: E402
    EDM_EVENT_ID_SCHEME,
    UINT32_MAX,
    edm_luminosity_block,
    validate_edm_event_ids,
)


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
    max_events: int = -1,
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
        str(max_events),
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
    assert edm_luminosity_block(0, 0) == 1
    assert edm_luminosity_block(1, 0) == 2
    assert edm_luminosity_block(0, 1) == 3
    try:
        edm_luminosity_block(UINT32_MAX, 0)
    except OverflowError:
        pass
    else:
        raise AssertionError("uint32 luminosity-block overflow was not rejected")
    validate_edm_event_ids([
        {
            "block_index": 0,
            "edm_event_id": {
                "first_run": 1,
                "first_luminosity_block": 1,
                "first_event": 1,
                "reserved_events": 10,
            },
        },
        {
            "block_index": 1,
            "edm_event_id": {
                "first_run": 1,
                "first_luminosity_block": 2,
                "first_event": 1,
                "reserved_events": 10,
            },
        },
    ])
    try:
        validate_edm_event_ids([
            {
                "block_index": 0,
                "edm_event_id": {
                    "first_run": 1,
                    "first_luminosity_block": 1,
                    "first_event": 1,
                    "reserved_events": 10,
                },
            },
            {
                "block_index": 1,
                "edm_event_id": {
                    "first_run": 1,
                    "first_luminosity_block": 1,
                    "first_event": 5,
                    "reserved_events": 10,
                },
            },
        ])
    except ValueError:
        pass
    else:
        raise AssertionError("overlap within one run/lumi was not rejected")

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
            max_events=5,
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
        assert first["max_events"] == 5
        assert first["target_mixed_events"] == 5
        assert first["event_id_span"] == 5
        assert first["minimum_output_fraction"] == 0.8
        assert first["edm_event_id"] == {
            "first_run": 1,
            "first_luminosity_block": 29,
            "first_event": 1,
            "reserved_events": 5,
            "number_events_in_luminosity_block": 0,
        }
        assert first["inputs"] == [
            "BLOCK:pool_jpsi_CSCO_g:jpsi_group:000000",
            "BLOCK:pool_jpsi_CSCO_g:jpsi_group:000001",
            "BLOCK:pool_gg:gg_group:000000",
        ]
        assert [source["inputs"] for source in first["sources"]] == [
            ["BLOCK:pool_jpsi_CSCO_g:jpsi_group:000000"],
            ["BLOCK:pool_jpsi_CSCO_g:jpsi_group:000001"],
            ["BLOCK:pool_gg:gg_group:000000"],
        ]
        second_config = workdir / "JJP_TPS" / "node_configs" / "processing" / (
            "MIX_JJP_TPS_7_BLOCK000001.json"
        )
        second = json.loads(second_config.read_text(encoding="utf-8"))
        assert second["edm_event_id"]["first_luminosity_block"] == 38
        assert second["edm_event_id"]["first_event"] == 1
        assert second["edm_event_id"]["reserved_events"] == 5

        coord_manifest = json.loads(
            (workdir / "JJP_TPS" / "coord_manifest_JJP_TPS_7.json").read_text(
                encoding="utf-8"
            )
        )
        assert coord_manifest["processing_max_events"] == 5
        assert coord_manifest["target_mixed_events"] == 5
        assert coord_manifest["event_id_span"] == 5
        assert coord_manifest["version"] == "1.1"
        assert coord_manifest["event_id_scheme"] == EDM_EVENT_ID_SCHEME
        assert "events_per_block" not in coord_manifest
        assert coord_manifest["mixed_blocks"][0]["edm_event_id"]["first_event"] == 1
        assert coord_manifest["mixed_blocks"][1]["edm_event_id"]["first_event"] == 1

        final_config = json.loads(
            (
                workdir / "JJP_TPS" / "node_configs" / "final" /
                "FINAL_JJP_TPS_7.json"
            ).read_text(encoding="utf-8")
        )
        assert final_config["event_id_scheme"] == EDM_EVENT_ID_SCHEME

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
        job8_first = json.loads(
            (
                workdir / "JJP_TPS" / "node_configs" / "processing" /
                "MIX_JJP_TPS_8_BLOCK000000.json"
            ).read_text(encoding="utf-8")
        )
        assert job8_first["edm_event_id"]["first_luminosity_block"] == 37
        assert job8_first["edm_event_id"]["first_event"] == 1
        assert (
            first["edm_event_id"]["first_luminosity_block"]
            != job8_first["edm_event_id"]["first_luminosity_block"]
        )
        merge_config = workdir / "JJP_TPS" / "node_configs" / "miniaod_merge" / (
            "MERGE_JJP_TPS_8_GROUP000000.json"
        )
        merge_payload = json.loads(merge_config.read_text(encoding="utf-8"))
        assert merge_payload["expected_events"] == 300
        assert [item["job_id"] for item in merge_payload["input_miniaods"]] == [
            "JOB000008_BLOCK000000",
            "JOB000008_BLOCK000001",
            "JOB000008_BLOCK000002",
        ]
        assert all("manifest_url" in item for item in merge_payload["input_miniaods"])
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
        assert sps6_dag.count("\nJOB MERGE_JJP_SPS_CS_5_GROUP") == 1
        first_merge = json.loads(
            (
                workdir / "JJP_SPS_CS" / "node_configs" / "miniaod_merge" /
                "MERGE_JJP_SPS_CS_5_GROUP000000.json"
            ).read_text(encoding="utf-8")
        )
        assert len(first_merge["input_miniaods"]) == 6
        assert first_merge["expected_events"] == 600

    print("[OK] Block coordinator TPS multiplicity and unique output IDs")
    print("[OK] Block coordinator single-source SPS SubDAG generation")
    print("[OK] Block processing and ntuple logs use per-job, per-block directories")
    print("[OK] MiniAOD merge mode emits merge-group ntuple nodes and provenance configs")
    print("[OK] Block coordinator emits deterministic, non-overlapping EDM EventIDs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
