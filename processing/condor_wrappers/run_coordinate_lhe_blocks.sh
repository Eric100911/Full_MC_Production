#!/bin/bash
# ==============================================================================
# run_coordinate_lhe_blocks.sh — Wrapper for the multi-source LHE block
# coordinator.
#
# Positional args from coordinate_lhe_blocks.sub:
#   $1  PROXY_BUNDLE
#   $2  COORD_BUNDLE
#   $3  CAMPAIGN
#   $4  JOB_INDEX
#   $5  SOURCE_MANIFESTS           (JSON string)
#   $6  SHOWER_MODES
#   $7  ANALYSIS_TYPE
#   $8  N_SOURCES
#   $9  MAX_EVENTS
#   $10 ENABLE_NTUPLE
#   $11 EFFICIENCY_NTUPLE
#   $12 CLEANUP
#   $13 SHUFFLE_MIXING
#   $14 LOG_ROOT
#   $15 REQUEST_CPUS
#   $16 REQUEST_MEMORY
#   $17 REQUEST_DISK
#   $18 TARGET_MACHINE
#   $19 OUTPUT_DIR
#   $20 PROCESSING_SUB_TEMPLATE_PATH
#   $21 PROCESSING_BUNDLE_PATH
#   $22 PROCESSING_BUNDLE_NAME
#   $23 PROXY_BUNDLE_PATH
#   $24 PROXY_BUNDLE_NAME
#   $25 PROCESSING_WRAPPER_PATH
#   $26 NTUPLE_SUB_TEMPLATE_PATH
#   $27 NTUPLE_BUNDLE_PATH
#   $28 NTUPLE_BUNDLE_NAME
#   $29 NTUPLE_WRAPPER_PATH
#   $30 SUBDAG_OUTPUT_PATH
#   $31 MAX_BLOCK_SUBDAG_JOBS
#   $32 LOCAL_OUTPUT_BASE
# ==============================================================================
set -euo pipefail

PROXY_BUNDLE="$1"
COORD_BUNDLE="$2"
CAMPAIGN="$3"
JOB_INDEX="$4"
SOURCE_MANIFESTS="$5"
SHOWER_MODES="$6"
ANALYSIS_TYPE="$7"
N_SOURCES="$8"
MAX_EVENTS="${9:--1}"
ENABLE_NTUPLE="${10:-false}"
EFFICIENCY_NTUPLE="${11:-false}"
CLEANUP="${12:-false}"
SHUFFLE_MIXING="${13:-false}"
LOG_ROOT="${14:-.}"
REQUEST_CPUS="${15:-8}"
REQUEST_MEMORY="${16:-20GB}"
REQUEST_DISK="${17:-50GB}"
TARGET_MACHINE="${18:-}"
OUTPUT_DIR="$19"
PROCESSING_SUB_TEMPLATE_PATH="$20"
PROCESSING_BUNDLE_PATH="$21"
PROCESSING_BUNDLE_NAME="$22"
PROXY_BUNDLE_PATH="$23"
PROXY_BUNDLE_NAME="$24"
PROCESSING_WRAPPER_PATH="$25"
NTUPLE_SUB_TEMPLATE_PATH="${26:-}"
NTUPLE_BUNDLE_PATH="${27:-}"
NTUPLE_BUNDLE_NAME="${28:-}"
NTUPLE_WRAPPER_PATH="${29:-}"
SUBDAG_OUTPUT_PATH="$30"
MAX_BLOCK_SUBDAG_JOBS="${31:-10}"
LOCAL_OUTPUT_BASE="${32:-}"

export LOCAL_OUTPUT_BASE="${LOCAL_OUTPUT_BASE}"

echo "=== LHE Block Coordinator Wrapper ==="
echo "Campaign: ${CAMPAIGN}  Job index: ${JOB_INDEX}"
echo "N sources: ${N_SOURCES}"

# Extract proxy bundle
echo "Extracting proxy bundle..."
tar -xzf "${PROXY_BUNDLE}"
PROXY_TARGET="/tmp/x509up_u$(id -u)"
install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"

# Extract coordinator bundle
echo "Extracting coordinator bundle..."
tar -xzf "${COORD_BUNDLE}"

# Build coordinator args
COORD_ARGS=(
    --campaign "${CAMPAIGN}"
    --job-index "${JOB_INDEX}"
    --source-manifests "${SOURCE_MANIFESTS}"
    --shower-modes "${SHOWER_MODES}"
    --analysis-type "${ANALYSIS_TYPE}"
    --n-sources "${N_SOURCES}"
    --max-events "${MAX_EVENTS}"
    --log-root "${LOG_ROOT}"
    --request-cpus "${REQUEST_CPUS}"
    --request-memory "${REQUEST_MEMORY}"
    --request-disk "${REQUEST_DISK}"
    --output-dir "${OUTPUT_DIR}"
    --processing-sub-template-path "${PROCESSING_SUB_TEMPLATE_PATH}"
    --processing-bundle-path "${PROCESSING_BUNDLE_PATH}"
    --processing-bundle-name "${PROCESSING_BUNDLE_NAME}"
    --proxy-bundle-path "${PROXY_BUNDLE_PATH}"
    --proxy-bundle-name "${PROXY_BUNDLE_NAME}"
    --processing-wrapper-path "${PROCESSING_WRAPPER_PATH}"
    --subdag-output-path "${SUBDAG_OUTPUT_PATH}"
    --max-block-subdag-jobs "${MAX_BLOCK_SUBDAG_JOBS}"
)
if [[ "${ENABLE_NTUPLE}" == "true" ]]; then
    COORD_ARGS+=(--enable-ntuple)
fi
if [[ "${EFFICIENCY_NTUPLE}" == "true" ]]; then
    COORD_ARGS+=(--efficiency-ntuple)
fi
if [[ "${CLEANUP}" == "true" ]]; then
    COORD_ARGS+=(--cleanup)
fi
if [[ "${SHUFFLE_MIXING}" == "true" ]]; then
    COORD_ARGS+=(--shuffle-mixing)
fi
if [[ -n "${TARGET_MACHINE}" ]]; then
    COORD_ARGS+=(--target-machine "${TARGET_MACHINE}")
fi
if [[ -n "${NTUPLE_SUB_TEMPLATE_PATH}" ]]; then
    COORD_ARGS+=(--ntuple-sub-template-path "${NTUPLE_SUB_TEMPLATE_PATH}")
fi
if [[ -n "${NTUPLE_BUNDLE_PATH}" ]]; then
    COORD_ARGS+=(--ntuple-bundle-path "${NTUPLE_BUNDLE_PATH}")
fi
if [[ -n "${NTUPLE_BUNDLE_NAME}" ]]; then
    COORD_ARGS+=(--ntuple-bundle-name "${NTUPLE_BUNDLE_NAME}")
fi
if [[ -n "${NTUPLE_WRAPPER_PATH}" ]]; then
    COORD_ARGS+=(--ntuple-wrapper-path "${NTUPLE_WRAPPER_PATH}")
fi
if [[ -n "${LOCAL_OUTPUT_BASE}" ]]; then
    COORD_ARGS+=(--local-output-base "${LOCAL_OUTPUT_BASE}")
fi

# Run the coordinator
echo "Running coordinate_lhe_blocks.py..."
cd runtime/tools
if ! python3 coordinate_lhe_blocks.py "${COORD_ARGS[@]}"; then
    echo "ERROR: LHE block coordination failed" >&2
    exit 1
fi

echo "=== LHE block coordination completed successfully ==="
