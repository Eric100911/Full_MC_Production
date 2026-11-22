#!/usr/bin/env python3
"""Apply slim-log mode to existing recovery DAGs in place (no regeneration).

The recovery resubmits already-generated blocks_processing.dag SubDAGs. Workers
get their log verbosity from two places:

  1. run_chain.sh inside the processing runtime bundle reads PROCESSING_LOG_LEVEL.
  2. run_processing.sh maps the per-node config's processing_environment.log_level
     to that env var.

This helper therefore (a) rebuilds each recovery output directory's
processing_runtime_bundle.tar.gz with the updated run_chain.sh, and (b) patches
every MIX_*.json node config to add log_level: "slim" to processing_environment.
The DAGs and their frozen manifests are untouched.

Usage:
    python3 tools/apply_slim_logs_to_recovery.py --dry-run
    python3 tools/apply_slim_logs_to_recovery.py
"""

from __future__ import annotations

import argparse
import json
import os
import tarfile
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Recovery output directories whose bundles/configs should be slimmed.
RECOVERY_OUTPUT_DIRS = [
    BASE_DIR / "generated" / "dps1_split_mix_only_20260813",
    BASE_DIR / "generated" / "JpsiJpsiPhi_MC_Production_v4_3_full_DPS2_CS",
    BASE_DIR / "generated" / "JpsiJpsiPhi_MC_Production_v4_3_full_SPS_CS",
    BASE_DIR / "generated" / "JpsiJpsiPhi_MC_Production_v4_3_full_SPS_G",
]

PROCESSING_BUNDLE_NAME = "processing_runtime_bundle.tar.gz"

# Mirrors dag_generator.py build_bundle processing_items.
PROCESSING_BUNDLE_ITEMS = [
    (BASE_DIR / "processing" / "run_chain.sh", "runtime/processing/run_chain.sh"),
    (BASE_DIR / "processing" / "cmssw_helpers", "runtime/processing/cmssw_helpers"),
    (BASE_DIR / "processing" / "pythia_shower", "runtime/processing/pythia_shower"),
    (BASE_DIR / "common" / "cmssw_configs", "runtime/common/cmssw_configs"),
    (BASE_DIR / "common" / "octet_pdg.py", "runtime/common/octet_pdg.py"),
    (BASE_DIR / "common" / "compression_helpers.sh", "runtime/common/compression_helpers.sh"),
]


def rebuild_processing_bundle(output_dir: Path, dry_run: bool) -> bool:
    bundle_path = output_dir / PROCESSING_BUNDLE_NAME
    if not bundle_path.is_file():
        print(f"  [skip] no bundle at {bundle_path.relative_to(BASE_DIR)}")
        return False
    if dry_run:
        print(f"  [dry-run] would rebuild {bundle_path.relative_to(BASE_DIR)}")
        return True
    temporary = bundle_path.with_suffix(bundle_path.suffix + ".tmp")
    with tarfile.open(temporary, "w:gz") as archive:
        for source_path, arcname in PROCESSING_BUNDLE_ITEMS:
            archive.add(str(source_path), arcname=arcname, recursive=True)
    os.replace(temporary, bundle_path)
    size = bundle_path.stat().st_size
    print(f"  [OK] rebuilt {bundle_path.relative_to(BASE_DIR)} ({size} bytes)")
    return True


def patch_config_log_level(config_path: Path, dry_run: bool) -> bool:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    env = payload.setdefault("processing_environment", {})
    if env.get("log_level") == "slim":
        return False
    env["log_level"] = "slim"
    if dry_run:
        print(f"  [dry-run] would patch {config_path.relative_to(BASE_DIR)}")
        return True
    temporary = config_path.with_suffix(config_path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, config_path)
    return True


def patch_all_configs(output_dir: Path, dry_run: bool) -> int:
    changed = 0
    seen = set()
    for config_path in sorted(output_dir.rglob("node_configs/processing/MIX_*.json")):
        if config_path in seen:
            continue
        seen.add(config_path)
        if patch_config_log_level(config_path, dry_run):
            changed += 1
    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing.")
    args = parser.parse_args()

    print("=== Rebuilding processing runtime bundles (updated run_chain.sh) ===")
    for output_dir in RECOVERY_OUTPUT_DIRS:
        rebuild_processing_bundle(output_dir, args.dry_run)

    print("=== Patching MIX node configs: processing_environment.log_level=slim ===")
    total_changed = 0
    for output_dir in RECOVERY_OUTPUT_DIRS:
        if not output_dir.is_dir():
            continue
        changed = patch_all_configs(output_dir, args.dry_run)
        total_changed += changed
        print(f"  {output_dir.relative_to(BASE_DIR)}: {changed} configs patched")
    print(f"=== Total configs patched: {total_changed} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
