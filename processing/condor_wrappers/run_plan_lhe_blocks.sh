#!/bin/bash
# ==============================================================================
# run_plan_lhe_blocks.sh — Wrapper for the per-pool LHE block planner.
#
# Positional args from plan_lhe_blocks.sub:
#   $1  PROXY_BUNDLE
#   $2  PLAN_BUNDLE
#   $3  POOL
#   $4  SEED
#   $5  LHE_PATH
#   $6  OUTPUT_DIR
#   $7  EVENTS_PER_BLOCK
#   $8  SHUFFLE_SEED
#   $9  SHUFFLE_MODE
#   $10 N_STRATA
#   $11 DROP_INCOMPLETE
#   $12 BLOCK_OUTPUT_DIR
#   $13 LOCAL_OUTPUT_BASE
#   $14 REUSE_BLOCKS
#   $15 MANIFEST_OUTPUT_PATH
# ==============================================================================
set -euo pipefail

PROXY_BUNDLE="$1"
PLAN_BUNDLE="$2"
POOL="$3"
SEED="$4"
LHE_PATH="$5"
OUTPUT_DIR="$6"
EVENTS_PER_BLOCK="${7:-1000}"
SHUFFLE_SEED="$8"
SHUFFLE_MODE="${9:-stratified}"
N_STRATA="${10:-auto}"
DROP_INCOMPLETE="${11:-false}"
BLOCK_OUTPUT_DIR="$12"
LOCAL_OUTPUT_BASE="${13:-}"
REUSE_BLOCKS="${14:-false}"
MANIFEST_OUTPUT_PATH="${15:-}"

export LOCAL_OUTPUT_BASE="${LOCAL_OUTPUT_BASE}"

echo "=== LHE Block Planner Wrapper ==="
echo "Pool: ${POOL}  Seed: ${SEED}"
echo "LHE path: ${LHE_PATH}"
echo "Events per block: ${EVENTS_PER_BLOCK}"
echo "Shuffle seed: ${SHUFFLE_SEED}"

# Extract proxy bundle
echo "Extracting proxy bundle..."
tar -xzf "${PROXY_BUNDLE}"
PROXY_TARGET="/tmp/x509up_u$(id -u)"
install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"

# Extract planner bundle
echo "Extracting planner bundle..."
tar -xzf "${PLAN_BUNDLE}"

# Build planner args
PLANNER_ARGS=(
    --pool-name "${POOL}"
    --helac-seed "${SEED}"
    --lhe-path "${LHE_PATH}"
    --output-dir "${OUTPUT_DIR}"
    --events-per-block "${EVENTS_PER_BLOCK}"
    --shuffle-seed "${SHUFFLE_SEED}"
    --shuffle-mode "${SHUFFLE_MODE}"
    --n-strata "${N_STRATA}"
    --block-output-dir "${BLOCK_OUTPUT_DIR}"
    --lhe-shuffle-split-bin ./lhe_shuffle_split
)
if [[ "${DROP_INCOMPLETE}" == "true" ]]; then
    PLANNER_ARGS+=(--drop-incomplete-last-block)
fi
if [[ -n "${LOCAL_OUTPUT_BASE}" ]]; then
    PLANNER_ARGS+=(--local-output-base "${LOCAL_OUTPUT_BASE}")
fi
if [[ "${REUSE_BLOCKS}" == "true" ]]; then
    PLANNER_ARGS+=(--reuse-existing-blocks)
fi
if [[ -n "${MANIFEST_OUTPUT_PATH}" ]]; then
    PLANNER_ARGS+=(--manifest-output-path "${MANIFEST_OUTPUT_PATH}")
fi

# Run the planner
echo "Running plan_lhe_blocks.py..."
cd runtime/tools
if ! python3 plan_lhe_blocks.py "${PLANNER_ARGS[@]}"; then
    echo "ERROR: LHE block planning failed" >&2
    exit 1
fi

echo "=== LHE block planning completed successfully ==="
