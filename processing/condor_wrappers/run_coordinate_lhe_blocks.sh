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
#   $7  CAMPAIGN_INPUTS             (comma-separated, with duplicates)
#   $8  ANALYSIS_TYPE
#   $9  N_SOURCES
#   $10 MAX_EVENTS
#   $11 ENABLE_NTUPLE
#   $12 EFFICIENCY_NTUPLE
#   $13 CLEANUP
#   $14 SHUFFLE_MIXING
#   $15 LOG_ROOT
#   $16 REQUEST_CPUS
#   $17 REQUEST_MEMORY
#   $18 REQUEST_DISK
#   $19 TARGET_MACHINE
#   $20 OUTPUT_DIR
#   $21 PROCESSING_SUB_TEMPLATE_PATH
#   $22 PROCESSING_BUNDLE_PATH
#   $23 PROCESSING_BUNDLE_NAME
#   $24 PROXY_BUNDLE_PATH
#   $25 PROXY_BUNDLE_NAME
#   $26 PROCESSING_WRAPPER_PATH
#   $27 NTUPLE_SUB_TEMPLATE_PATH
#   $28 NTUPLE_BUNDLE_PATH
#   $29 NTUPLE_BUNDLE_NAME
#   $30 NTUPLE_WRAPPER_PATH
#   $31 SUBDAG_OUTPUT_PATH
#   $32 MAX_BLOCK_SUBDAG_JOBS
#   $33 LOCAL_OUTPUT_BASE
# ==============================================================================
set -euo pipefail

PROXY_BUNDLE="$1"
COORD_BUNDLE="$2"
CAMPAIGN="$3"
JOB_INDEX="$4"
SOURCE_MANIFESTS="$5"
SHOWER_MODES="$6"
CAMPAIGN_INPUTS="$7"
ANALYSIS_TYPE="$8"
N_SOURCES="$9"
MAX_EVENTS="${10:--1}"
ENABLE_NTUPLE="${11:-false}"
EFFICIENCY_NTUPLE="${12:-false}"
CLEANUP="${13:-false}"
SHUFFLE_MIXING="${14:-false}"
LOG_ROOT="${15:-.}"
REQUEST_CPUS="${16:-8}"
REQUEST_MEMORY="${17:-20GB}"
REQUEST_DISK="${18:-50GB}"
TARGET_MACHINE="${19:-}"
OUTPUT_DIR="$20"
PROCESSING_SUB_TEMPLATE_PATH="$21"
PROCESSING_BUNDLE_PATH="$22"
PROCESSING_BUNDLE_NAME="$23"
PROXY_BUNDLE_PATH="$24"
PROXY_BUNDLE_NAME="$25"
PROCESSING_WRAPPER_PATH="$26"
NTUPLE_SUB_TEMPLATE_PATH="${27:-}"
NTUPLE_BUNDLE_PATH="${28:-}"
NTUPLE_BUNDLE_NAME="${29:-}"
NTUPLE_WRAPPER_PATH="${30:-}"
SUBDAG_OUTPUT_PATH="$31"
MAX_BLOCK_SUBDAG_JOBS="${32:-10}"
LOCAL_OUTPUT_BASE="${33:-}"

export LOCAL_OUTPUT_BASE="${LOCAL_OUTPUT_BASE}"

echo "=== LHE Block Coordinator Wrapper ==="
echo "Campaign: ${CAMPAIGN}  Job index: ${JOB_INDEX}"
echo "N sources: ${N_SOURCES}  Campaign inputs: ${CAMPAIGN_INPUTS}"

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
    --campaign-inputs "${CAMPAIGN_INPUTS}"
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
