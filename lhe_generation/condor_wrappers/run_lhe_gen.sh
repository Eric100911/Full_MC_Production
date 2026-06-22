#!/bin/bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <proxy-bundle.tar.gz> <lhe-runtime-bundle.tar.gz> <config.json>" >&2
    exit 64
fi

PROXY_BUNDLE="$1"
LHE_BUNDLE="$2"
CONFIG_NAME="$3"

if [[ ! -f "${CONFIG_NAME}" ]]; then
    echo "ERROR: LHE config JSON not found: ${CONFIG_NAME}" >&2
    exit 66
fi

echo "=== LHE Generation Wrapper ==="
echo "Working directory: $(pwd)"
echo "Config: ${CONFIG_NAME}"
echo "PATH: ${PATH}"
echo ""

if ! command -v tar >/dev/null 2>&1; then
    echo "ERROR: tar command not found" >&2
    echo "Available PATH: ${PATH}" >&2
    exit 1
fi

echo "Extracting proxy bundle..."
tar -xzf "${PROXY_BUNDLE}"

echo "Installing proxy..."
PROXY_TARGET="/tmp/x509up_u$(id -u)"
install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"
echo "X509_USER_PROXY=${X509_USER_PROXY}"
if command -v voms-proxy-info >/dev/null 2>&1; then
    voms-proxy-info --file "${X509_USER_PROXY}" --timeleft || true
fi

echo "Extracting LHE bundle..."
tar -xzf "${LHE_BUNDLE}"

CONFIG_PATH="$(pwd)/${CONFIG_NAME}"
export CONFIG_PATH

echo "Running HELAC generation..."
cd runtime/lhe_generation
if ! python3 - <<'PY'
import json
import os
import subprocess


def bool_text(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "false"}:
            return lowered
    raise SystemExit(f"Expected boolean value, got {value!r}")


config_path = os.environ["CONFIG_PATH"]
with open(config_path, "r", encoding="utf-8") as handle:
    cfg = json.load(handle)

required = [
    "pool",
    "seed",
    "min_pt_conia",
    "min_pt_bonia",
    "min_pt_q",
    "unwevt",
    "test_mode",
    "output_dir",
]
missing = [key for key in required if key not in cfg or cfg[key] in (None, "")]
if missing:
    raise SystemExit(f"Missing LHE config keys: {', '.join(missing)}")

local_output_base = cfg.get("local_output_base")
if local_output_base:
    os.environ["LOCAL_OUTPUT_BASE"] = str(local_output_base)

storage = cfg.get("storage", {})
if isinstance(storage, dict) and storage.get("target_eos_base"):
    os.environ["TARGET_EOS_BASE"] = str(storage["target_eos_base"])

cmd = [
    "bash",
    "run_helac.sh",
    "--pool", str(cfg["pool"]),
    "--seed", str(cfg["seed"]),
    "--min-pt-conia", str(cfg["min_pt_conia"]),
    "--min-pt-bonia", str(cfg["min_pt_bonia"]),
    "--min-pt-q", str(cfg["min_pt_q"]),
    "--unwevt", str(cfg["unwevt"]),
    "--test-mode", bool_text(cfg["test_mode"]),
    "--output-dir", str(cfg["output_dir"]),
]

if cfg.get("compress_lhe", False):
    cmd.extend([
        "--compress-lhe",
        "--lhe-compression-level",
        str(cfg.get("lhe_compression_level", 1)),
    ])

if cfg.get("lhe_shuffle_split", False):
    cmd.extend([
        "--lhe-shuffle-split",
        "--lhe-events-per-block",
        str(cfg.get("lhe_events_per_block", 1000)),
        "--lhe-shuffle-mode",
        str(cfg.get("lhe_shuffle_mode", "stratified")),
        "--lhe-n-strata",
        str(cfg.get("lhe_n_strata", "auto")),
    ])
    if cfg.get("lhe_drop_incomplete_last_block", False):
        cmd.append("--lhe-drop-incomplete-last-block")

raise SystemExit(subprocess.run(cmd, check=False).returncode)
PY
then
    echo "ERROR: HELAC generation failed" >&2
    exit 1
fi

echo "=== LHE generation completed successfully ==="
