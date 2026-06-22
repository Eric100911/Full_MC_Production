#!/bin/bash
# ==============================================================================
# run_ntuple_only.sh - HTCondor wrapper for standalone ntuple re-running.
# ==============================================================================

set -euo pipefail

PROXY_BUNDLE="${1:?missing proxy bundle}"
NTUPLE_BUNDLE="${2:?missing ntuple bundle}"
CONFIG_NAME="${3:?missing ntuple config JSON}"
CONFIG_PATH="${PWD}/${CONFIG_NAME}"

if [[ ! -s "${CONFIG_PATH}" ]]; then
    echo "FATAL: Ntuple config JSON not found or empty: ${CONFIG_PATH}" >&2
    exit 1
fi

echo "=== Ntuple-only Wrapper ==="
echo "Host: $(hostname)"
echo "Date: $(date -Iseconds)"
echo "Working dir: $(pwd)"
echo "Config: ${CONFIG_NAME}"

echo "--- Extracting proxy bundle ---"
tar -xzf "${PROXY_BUNDLE}"

PROXY_TARGET="/tmp/x509up_u$(id -u)"
install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"

echo "--- Extracting ntuple runtime bundle ---"
tar -xzf "${NTUPLE_BUNDLE}"

export LD_LIBRARY_PATH="/usr/lib64:${LD_LIBRARY_PATH:-}"

echo "--- Running ntuple chain ---"
cd runtime/processing

if ! python3 - "${CONFIG_PATH}" <<'PY'
import json
import os
import subprocess
import sys

config_path = sys.argv[1]
with open(config_path, "r", encoding="utf-8") as handle:
    cfg = json.load(handle)

required = [
    "analysis",
    "campaign",
    "job_id",
    "max_events",
    "efficiency_ntuple",
    "cleanup",
    "miniaod_input",
]
missing = [key for key in required if key not in cfg or cfg[key] in (None, "")]
if missing:
    raise SystemExit(f"Missing ntuple config keys: {', '.join(missing)}")

def bool_text(value):
    return "true" if bool(value) else "false"

env = os.environ.copy()
storage = cfg.get("storage", {})
if storage and not isinstance(storage, dict):
    raise SystemExit("Ntuple config key storage must be an object when provided")
for cfg_key, env_key in [
    ("local_output_base", "LOCAL_OUTPUT_BASE"),
    ("custom_output_subpath", "CUSTOM_OUTPUT_SUBPATH"),
    ("custom_ntuple_basename", "CUSTOM_NTUPLE_BASENAME"),
]:
    value = cfg.get(cfg_key)
    if value:
        env[env_key] = str(value)
target_eos_base = cfg.get("target_eos_base") or storage.get("target_eos_base")
if target_eos_base:
    env["TARGET_EOS_BASE"] = str(target_eos_base)

cmd = [
    "bash",
    "run_chain.sh",
    "--inputs", "file:/dev/null",
    "--modes", "normal",
    "--analysis", str(cfg["analysis"]),
    "--campaign", str(cfg["campaign"]),
    "--job-id", str(cfg["job_id"]),
    "--max-events", str(cfg["max_events"]),
    "--enable-ntuple", "true",
    "--efficiency-ntuple", bool_text(cfg["efficiency_ntuple"]),
    "--cleanup", bool_text(cfg["cleanup"]),
    "--skip-to", "ntuple",
    "--miniaod-input", str(cfg["miniaod_input"]),
    "--transfer-miniaod", "false",
]

raise SystemExit(subprocess.run(cmd, env=env, check=False).returncode)
PY
then
    echo "FATAL: run_chain.sh failed" >&2
    exit 1
fi

echo "=== Ntuple-only wrapper completed successfully ==="
