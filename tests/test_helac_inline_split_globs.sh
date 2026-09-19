#!/bin/bash
# Offline contract for the retained worker's optional inline split stage.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${SCRIPT_DIR}/../lhe_generation/run_helac.sh"

grep -F 'BLOCK_COUNT=$(ls "${SHUFFLE_SPLIT_DIR}/${MY_SEED}_block_"*.lhe' "${SCRIPT}" >/dev/null
grep -F 'cp "${SHUFFLE_SPLIT_DIR}/${MY_SEED}_block_"*.lhe' "${SCRIPT}" >/dev/null
grep -F 'for bf in "${SHUFFLE_SPLIT_DIR}/${MY_SEED}_block_"*.lhe' "${SCRIPT}" >/dev/null

echo "[PASS] inline shuffle-split count, local copy, and remote loop use seed-prefixed blocks"
