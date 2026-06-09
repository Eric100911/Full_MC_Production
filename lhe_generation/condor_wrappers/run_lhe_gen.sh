#!/bin/bash

set -uo pipefail

PROXY_BUNDLE="$1"
LHE_BUNDLE="$2"
POOL="$3"
SEED="$4"
MIN_PT_CONIA="$5"
MIN_PT_BONIA="$6"
MIN_PT_Q="$7"
UNWEVT="$8"
TEST_MODE="$9"
LOCAL_OUTPUT_BASE="${10:-}"
COMPRESS_LHE="${11:-false}"
LHE_COMPRESSION_LEVEL="${12:-1}"

export LOCAL_OUTPUT_BASE="${LOCAL_OUTPUT_BASE:-}"

echo "=== LHE Generation Wrapper ==="
echo "Working directory: $(pwd)"
echo "Pool: ${POOL}"
echo "Seed: ${SEED}"
echo "LOCAL_OUTPUT_BASE: ${LOCAL_OUTPUT_BASE:-NOT SET}"
echo "COMPRESS_LHE: ${COMPRESS_LHE}"
echo "PATH: ${PATH}"
echo ""

if ! command -v tar >/dev/null 2>&1; then
    echo "ERROR: tar command not found" >&2
    echo "Available PATH: ${PATH}" >&2
    exit 1
fi

echo "Extracting proxy bundle..."
if ! tar -xzf "${PROXY_BUNDLE}"; then
    echo "ERROR: Failed to extract proxy bundle" >&2
    exit 1
fi

echo "Installing proxy..."
PROXY_TARGET="/tmp/x509up_u$(id -u)"
if ! install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"; then
    echo "ERROR: Failed to install proxy" >&2
    exit 1
fi

rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"
echo "X509_USER_PROXY=${X509_USER_PROXY}"
if command -v voms-proxy-info >/dev/null 2>&1; then
    voms-proxy-info --file "${X509_USER_PROXY}" --timeleft || true
fi

echo "Extracting LHE bundle..."
if ! tar -xzf "${LHE_BUNDLE}"; then
    echo "ERROR: Failed to extract LHE bundle" >&2
    exit 1
fi

echo "Running HELAC generation..."
cd runtime/lhe_generation
COMPRESS_ARGS=()
if [[ "${COMPRESS_LHE}" == "true" ]]; then
    COMPRESS_ARGS+=(--compress-lhe --lhe-compression-level "${LHE_COMPRESSION_LEVEL}")
fi
if ! bash run_helac.sh --pool "${POOL}" --seed "${SEED}" --min-pt-conia "${MIN_PT_CONIA}" --min-pt-bonia "${MIN_PT_BONIA}" --min-pt-q "${MIN_PT_Q}" --unwevt "${UNWEVT}" --test-mode "${TEST_MODE}" "${COMPRESS_ARGS[@]}"; then
    echo "ERROR: HELAC generation failed" >&2
    exit 1
fi

echo "=== LHE generation completed successfully ==="
