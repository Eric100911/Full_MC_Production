#!/bin/bash
# ==============================================================================
# run_plan_lhe_blocks.sh - Wrapper for the per-pool LHE block planner.
# ==============================================================================
set -euo pipefail

PROXY_BUNDLE="${1:?missing proxy bundle}"
PLAN_BUNDLE="${2:?missing planner bundle}"
CONFIG_NAME="${3:?missing planner config JSON}"
CONFIG_PATH="${PWD}/${CONFIG_NAME}"

if [[ ! -s "${CONFIG_PATH}" ]]; then
    echo "ERROR: Planner config JSON not found or empty: ${CONFIG_PATH}" >&2
    exit 1
fi

echo "=== LHE Block Planner Wrapper ==="
echo "Config: ${CONFIG_NAME}"

echo "Extracting proxy bundle..."
tar -xzf "${PROXY_BUNDLE}"
PROXY_TARGET="/tmp/x509up_u$(id -u)"
install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"

echo "Extracting planner bundle..."
tar -xzf "${PLAN_BUNDLE}"

echo "Running plan_lhe_blocks.py..."
cd runtime/tools
if ! python3 - "${CONFIG_PATH}" <<'PY'
import json
import subprocess
import sys

config_path = sys.argv[1]
with open(config_path, "r", encoding="utf-8") as handle:
    cfg = json.load(handle)

required = [
    "pool_name",
    "group_id",
    "primary_seed",
    "seeds",
    "lhe_paths",
    "output_dir",
    "events_per_block",
    "shuffle_seed",
    "shuffle_mode",
    "n_strata",
    "block_output_dir",
    "manifest_output_path",
]
missing = [key for key in required if key not in cfg or cfg[key] in (None, "")]
if missing:
    raise SystemExit(f"Missing planner config keys: {', '.join(missing)}")
if not isinstance(cfg["lhe_paths"], list) or not cfg["lhe_paths"]:
    raise SystemExit("Planner config key lhe_paths must be a non-empty list")
if not isinstance(cfg["seeds"], list) or not cfg["seeds"]:
    raise SystemExit("Planner config key seeds must be a non-empty list")
if "lhe_event_counts" in cfg and cfg["lhe_event_counts"]:
    if not isinstance(cfg["lhe_event_counts"], list):
        raise SystemExit("Planner config key lhe_event_counts must be a list")
    if len(cfg["lhe_event_counts"]) != len(cfg["lhe_paths"]):
        raise SystemExit("Planner config key lhe_event_counts must match lhe_paths length")

cmd = [
    "python3",
    "plan_lhe_blocks.py",
    "--pool-name", str(cfg["pool_name"]),
    "--group-id", str(cfg["group_id"]),
    "--primary-seed", str(cfg["primary_seed"]),
    "--helac-seeds", ",".join(str(seed) for seed in cfg["seeds"]),
    "--output-dir", str(cfg["output_dir"]),
    "--events-per-block", str(cfg["events_per_block"]),
    "--shuffle-seed", str(cfg["shuffle_seed"]),
    "--shuffle-mode", str(cfg["shuffle_mode"]),
    "--n-strata", str(cfg["n_strata"]),
    "--block-output-dir", str(cfg["block_output_dir"]),
    "--manifest-output-path", str(cfg["manifest_output_path"]),
    "--lhe-shuffle-split-bin", "./lhe_shuffle_split",
]
max_events_per_plan = int(cfg.get("max_events_per_plan", 0) or 0)
if max_events_per_plan > 0:
    cmd.extend(["--max-events-per-plan", str(max_events_per_plan)])
lhe_event_counts = cfg.get("lhe_event_counts", [])
if lhe_event_counts:
    cmd.extend(["--lhe-event-counts", ",".join(str(count) for count in lhe_event_counts)])
for path in cfg["lhe_paths"]:
    cmd.extend(["--lhe-path", str(path)])
if cfg.get("drop_incomplete_last_block", False):
    cmd.append("--drop-incomplete-last-block")
if cfg.get("local_output_base"):
    cmd.extend(["--local-output-base", str(cfg["local_output_base"])])
if cfg.get("reuse_existing_blocks", False):
    cmd.append("--reuse-existing-blocks")

raise SystemExit(subprocess.run(cmd, check=False).returncode)
PY
then
    echo "ERROR: LHE block planning failed" >&2
    exit 1
fi

echo "=== LHE block planning completed successfully ==="
