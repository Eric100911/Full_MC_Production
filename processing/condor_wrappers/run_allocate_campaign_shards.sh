#!/bin/bash
# Build authoritative campaign/shard cursors after all LHE planners complete.

set -euo pipefail

ALLOCATION_BUNDLE="${1:?missing allocation bundle}"
CONFIG_NAME="${2:?missing allocation config JSON}"
CONFIG_PATH="${PWD}/${CONFIG_NAME}"

if [[ ! -s "${CONFIG_PATH}" ]]; then
    echo "ERROR: Allocation config JSON not found or empty: ${CONFIG_PATH}" >&2
    exit 1
fi

WRAPPER_LOG_DIR="${PWD}/wrapper_logs"
mkdir -p "${WRAPPER_LOG_DIR}"
if command -v tee >/dev/null 2>&1; then
    exec > >(tee -a "${WRAPPER_LOG_DIR}/wrapper.stdout") \
        2> >(tee -a "${WRAPPER_LOG_DIR}/wrapper.stderr" >&2)
else
    exec >"${WRAPPER_LOG_DIR}/wrapper.stdout" 2>"${WRAPPER_LOG_DIR}/wrapper.stderr"
fi

read_log_level() {
    python3 - "$1" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as handle:
        print(json.load(handle).get("log_level") or "info")
except Exception:
    print("info")
PY
}

# The allocation config carries the workflow log level; export it so this node
# sees the same contract as the LHE/planner/coordinator nodes.
WORKFLOW_LOG_LEVEL="${WORKFLOW_LOG_LEVEL:-$(read_log_level "${CONFIG_PATH}")}"
export WORKFLOW_LOG_LEVEL

tar -xzf "${ALLOCATION_BUNDLE}"
cd runtime/tools
python3 allocate_campaign_shards.py --config "${CONFIG_PATH}"
