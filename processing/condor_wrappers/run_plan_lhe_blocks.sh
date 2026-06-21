#!/bin/bash
# ==============================================================================
# run_plan_lhe_blocks.sh — Wrapper for the per-pool LHE block planner.
#
# Positional args from plan_lhe_blocks.sub:
#   $1  PROXY_BUNDLE
#   $2  PLAN_BUNDLE
#   $3  POOL
#   $4  GROUP_ID
#   $5  PRIMARY_SEED
#   $6  SEEDS
#   $7  LHE_PATHS
#   $8  OUTPUT_DIR
#   $9  EVENTS_PER_BLOCK
#   $10 SHUFFLE_SEED
#   $11 SHUFFLE_MODE
#   $12 N_STRATA
#   $13 DROP_INCOMPLETE
#   $14 BLOCK_OUTPUT_DIR
#   $15 LOCAL_OUTPUT_BASE
#   $16 REUSE_BLOCKS
#   $17 MANIFEST_OUTPUT_PATH
# ==============================================================================
set -euo pipefail

PROXY_BUNDLE="$1"
PLAN_BUNDLE="$2"
POOL="$3"
GROUP_ID="$4"
PRIMARY_SEED="$5"
SEEDS="$6"
LHE_PATHS="$7"
OUTPUT_DIR="$8"
EVENTS_PER_BLOCK="${9:-1000}"
SHUFFLE_SEED="${10}"
SHUFFLE_MODE="${11:-stratified}"
N_STRATA="${12:-auto}"
DROP_INCOMPLETE="${13:-false}"
BLOCK_OUTPUT_DIR="$14"
LOCAL_OUTPUT_BASE="${15:-}"
REUSE_BLOCKS="${16:-false}"
MANIFEST_OUTPUT_PATH="${17:-}"

export LOCAL_OUTPUT_BASE="${LOCAL_OUTPUT_BASE}"

echo "=== LHE Block Planner Wrapper ==="
echo "Pool: ${POOL}  Group: ${GROUP_ID}  Primary seed: ${PRIMARY_SEED}"
echo "Seeds: ${SEEDS}"
echo "LHE paths: ${LHE_PATHS}"
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
    --group-id "${GROUP_ID}"
    --primary-seed "${PRIMARY_SEED}"
    --helac-seeds "${SEEDS}"
    --output-dir "${OUTPUT_DIR}"
    --events-per-block "${EVENTS_PER_BLOCK}"
    --shuffle-seed "${SHUFFLE_SEED}"
    --shuffle-mode "${SHUFFLE_MODE}"
    --n-strata "${N_STRATA}"
    --block-output-dir "${BLOCK_OUTPUT_DIR}"
    --lhe-shuffle-split-bin ./lhe_shuffle_split
)
IFS=',' read -ra PATH_ARRAY <<< "${LHE_PATHS}"
for path in "${PATH_ARRAY[@]}"; do
    PLANNER_ARGS+=(--lhe-path "${path}")
done
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
