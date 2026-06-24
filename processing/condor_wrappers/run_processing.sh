#!/bin/bash

set -euo pipefail

PROXY_BUNDLE="${1:?missing proxy bundle}"
PROCESSING_BUNDLE="${2:?missing processing bundle}"
CONFIG_NAME="${3:?missing processing config JSON}"
CONFIG_PATH="${PWD}/${CONFIG_NAME}"

if [[ ! -s "${CONFIG_PATH}" ]]; then
    echo "ERROR: Processing config JSON not found or empty: ${CONFIG_PATH}" >&2
    exit 1
fi

echo "=== Processing Chain Wrapper ==="
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

echo "Extracting processing bundle..."
tar -xzf "${PROCESSING_BUNDLE}"

export LD_LIBRARY_PATH="/usr/lib64:${LD_LIBRARY_PATH:-}"

echo "Running processing chain..."
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
    "inputs",
    "modes",
    "analysis",
    "campaign",
    "job_id",
    "max_events",
    "enable_ntuple",
    "efficiency_ntuple",
    "cleanup",
    "shuffle_mixing",
]
missing = [key for key in required if key not in cfg or cfg[key] is None]
if missing:
    raise SystemExit(f"Missing processing config keys: {', '.join(missing)}")
if not isinstance(cfg["inputs"], list) or not cfg["inputs"]:
    raise SystemExit("Processing config key inputs must be a non-empty list")
if not isinstance(cfg["modes"], list) or not cfg["modes"]:
    raise SystemExit("Processing config key modes must be a non-empty list")

def bool_text(value):
    return "true" if bool(value) else "false"

env = os.environ.copy()
storage = cfg.get("storage", {})
if storage and not isinstance(storage, dict):
    raise SystemExit("Processing config key storage must be an object when provided")
for cfg_key, env_key in [
    ("local_output_base", "LOCAL_OUTPUT_BASE"),
]:
    value = cfg.get(cfg_key)
    if value:
        env[env_key] = str(value)
target_eos_base = cfg.get("target_eos_base") or storage.get("target_eos_base")
if target_eos_base:
    env["TARGET_EOS_BASE"] = str(target_eos_base)

processing_env = cfg.get("processing_environment", {})
if not isinstance(processing_env, dict):
    raise SystemExit("Processing config key processing_environment must be an object when provided")
env_mapping = {
    "premix_input_mode": "PREMIX_INPUT_MODE",
    "premix_redirector": "PREMIX_REDIRECTOR",
    "premix_cache_files": "PREMIX_CACHE_FILES",
    "premix_cache_redirector": "PREMIX_CACHE_REDIRECTOR",
    "premix_cache_timeout": "PREMIX_CACHE_TIMEOUT",
    "premix_cache_retries": "PREMIX_CACHE_RETRIES",
    "raw_threads": "RAW_THREADS",
    "raw_streams": "RAW_STREAMS",
    "raw_watchdog_timeout": "RAW_WATCHDOG_TIMEOUT",
    "raw_watchdog_kill_after": "RAW_WATCHDOG_KILL_AFTER",
    "raw_cmsdriver_timeout": "RAW_CMSDRIVER_TIMEOUT",
}
for cfg_key, env_key in env_mapping.items():
    value = processing_env.get(cfg_key, cfg.get(cfg_key))
    if value:
        env[env_key] = str(value)

cmd = [
    "bash",
    "run_chain.sh",
    "--inputs", ",".join(str(item) for item in cfg["inputs"]),
    "--modes", ",".join(str(item) for item in cfg["modes"]),
    "--analysis", str(cfg["analysis"]),
    "--campaign", str(cfg["campaign"]),
    "--job-id", str(cfg["job_id"]),
    "--max-events", str(cfg["max_events"]),
    "--enable-ntuple", bool_text(cfg["enable_ntuple"]),
    "--efficiency-ntuple", bool_text(cfg["efficiency_ntuple"]),
    "--shuffle-mixing", bool_text(cfg["shuffle_mixing"]),
    "--cleanup", bool_text(cfg["cleanup"]),
    "--config", config_path,
]
if cfg.get("skip_to"):
    cmd.extend(["--skip-to", str(cfg["skip_to"])])
if cfg.get("stop_at"):
    cmd.extend(["--stop-at", str(cfg["stop_at"])])

raise SystemExit(subprocess.run(cmd, env=env, check=False).returncode)
PY
then
    echo "ERROR: Processing chain failed" >&2
    exit 1
fi

echo "=== Processing chain completed successfully ==="
