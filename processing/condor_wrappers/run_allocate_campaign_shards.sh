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

tar -xzf "${ALLOCATION_BUNDLE}"
cd runtime/tools
python3 allocate_campaign_shards.py --config "${CONFIG_PATH}"
