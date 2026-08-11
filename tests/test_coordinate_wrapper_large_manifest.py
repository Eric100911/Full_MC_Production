#!/usr/bin/env python3
"""Exercise the coordinator wrapper above Linux's single-argument limit."""

from __future__ import annotations

import json
import subprocess
import tarfile
import tempfile
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
WRAPPER = BASE_DIR / "processing/condor_wrappers/run_coordinate_lhe_blocks.sh"
TMP_ROOT = Path("/tmp/chiw")
ENTRY_COUNT = 1900


STUB_COORDINATOR = """#!/usr/bin/env python3
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("--source-manifests-file", required=True)
parser.add_argument("--allocation-manifest", required=True)
parser.add_argument("--allocation-shard-index", required=True, type=int)
args, _ = parser.parse_known_args()
with open(args.source_manifests_file, "r", encoding="utf-8") as handle:
    entries = json.load(handle)
if len(entries) != 1900:
    raise SystemExit(f"wrong manifest count: {len(entries)}")
if args.allocation_shard_index != 7:
    raise SystemExit(f"wrong allocation shard: {args.allocation_shard_index}")
print(f"LARGE_MANIFEST_OK={len(entries)}")
"""


def write_tarball(path: Path, files: dict[str, bytes]) -> None:
    staging = path.parent / (path.stem + "_staging")
    for relative, payload in files.items():
        target = staging / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    with tarfile.open(path, "w:gz") as archive:
        for relative in files:
            archive.add(staging / relative, arcname=relative)


def base_config(source_manifests_path: Path) -> dict[str, object]:
    return {
        "campaign": "JJP_TPS_MC_v4_1",
        "job_index": 0,
        "source_manifests_path": str(source_manifests_path),
        "shower_modes": ["normal", "normal", "phi_mpi_off"],
        "campaign_inputs": [
            "pool_jpsi_CSCO_g",
            "pool_jpsi_CSCO_g",
            "pool_gg",
        ],
        "analysis_type": "JJP",
        "n_sources": 3,
        "max_events": -1,
        "log_root": "/tmp/chiw/logs",
        "request_cpus": "2",
        "request_memory": "8GB",
        "request_disk": "20GB",
        "output_dir": "/tmp/chiw/output",
        "processing_sub_template_path": "processing.sub",
        "processing_bundle_path": "processing_runtime_bundle.tar.gz",
        "processing_bundle_name": "processing_runtime_bundle.tar.gz",
        "proxy_bundle_path": "proxy_bundle.tar.gz",
        "proxy_bundle_name": "proxy_bundle.tar.gz",
        "processing_wrapper_path": "run_processing.sh",
        "subdag_output_path": "/tmp/chiw/output/blocks_processing.dag",
        "max_block_subdag_jobs": 100,
        "miniaod_merge_events": 5000,
        "miniaod_merge_validation": "event-count",
        "max_miniaod_merge_jobs": 50,
        "miniaod_merge_sub_template_path": "miniaod_merge.sub",
        "miniaod_merge_wrapper_path": "run_miniaod_merge.sh",
        "final_sub_template_path": "subdag_final.sub",
        "final_wrapper_path": "run_subdag_final_inventory.sh",
        "source_lhe_budgets": [1000, 1000, 977],
        "pool_start_blocks": {
            "pool_jpsi_CSCO_g": 0,
            "pool_gg": 0,
        },
        "processing_start_index": 0,
        "max_processing_nodes": 2000,
        "allocation_manifest_path": "/afs/example/campaign_shards.json",
        "allocation_shard_index": 7,
    }


def run_wrapper(workdir: Path, config: dict[str, object], name: str) -> None:
    config_path = workdir / name
    config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    result = subprocess.run(
        [
            "bash",
            str(WRAPPER),
            "proxy_bundle.tar.gz",
            "coordinate_runtime_bundle.tar.gz",
            name,
        ],
        cwd=workdir,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if result.returncode != 0 or f"LARGE_MANIFEST_OK={ENTRY_COUNT}" not in result.stdout:
        raise AssertionError(result.stdout)


def main() -> int:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    entries = [
        {
            "pool": "pool_jpsi_CSCO_g",
            "group_id": f"group_{index:06d}",
            "primary_seed": index,
            "path": (
                "/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/"
                f"generated/plans/group_{index:06d}/plan_manifest.json"
            ),
        }
        for index in range(ENTRY_COUNT)
    ]
    with tempfile.TemporaryDirectory(
        prefix="coordinate_wrapper_large_",
        dir=TMP_ROOT,
    ) as tmp:
        workdir = Path(tmp)
        source_path = workdir / "source_manifests.json"
        source_path.write_text(
            json.dumps(entries, indent=2) + "\n",
            encoding="utf-8",
        )
        write_tarball(
            workdir / "proxy_bundle.tar.gz",
            {"credentials/x509_user_proxy": b"test-proxy\n"},
        )
        write_tarball(
            workdir / "coordinate_runtime_bundle.tar.gz",
            {"runtime/tools/coordinate_lhe_blocks.py": STUB_COORDINATOR.encode()},
        )

        file_config = base_config(source_path)
        run_wrapper(workdir, file_config, "file_config.json")

        embedded_config = base_config(source_path)
        embedded_config["source_manifests"] = entries
        run_wrapper(workdir, embedded_config, "embedded_config.json")

    print("[OK] Coordinator wrapper handles 1900 manifests through a JSON file")
    print("[OK] Embedded legacy configs are converted to a worker-scratch file")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
