#!/bin/bash

set -uo pipefail

PROXY_BUNDLE="$1"
PROCESSING_BUNDLE="$2"
INPUTS="$3"
MODES="$4"
ANALYSIS="$5"
CAMPAIGN="$6"
JOB_ID="$7"
MAX_EVENTS="$8"
ENABLE_NTUPLE="$9"
EFFICIENCY_NTUPLE="${10:-false}"
CLEANUP="${11:-true}"
LOCAL_OUTPUT_BASE="${12:-}"
SHUFFLE_MIXING="${13:-false}"
TARGET_EOS_BASE="${14:-}"

export LOCAL_OUTPUT_BASE="${LOCAL_OUTPUT_BASE:-}"
export TARGET_EOS_BASE="${TARGET_EOS_BASE:-}"

echo "=== Processing Chain Wrapper ==="
echo "Working directory: $(pwd)"
echo "Campaign: ${CAMPAIGN}"
echo "Job ID: ${JOB_ID}"
echo "LOCAL_OUTPUT_BASE: ${LOCAL_OUTPUT_BASE:-NOT SET}"
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

echo "Extracting processing bundle..."
if ! tar -xzf "${PROCESSING_BUNDLE}"; then
    echo "ERROR: Failed to extract processing bundle" >&2
    exit 1
fi

export LD_LIBRARY_PATH="/usr/lib64:${LD_LIBRARY_PATH:-}"

echo "Running processing chain..."
cd runtime/processing
if ! bash run_chain.sh --inputs "${INPUTS}" --modes "${MODES}" --analysis "${ANALYSIS}" --campaign "${CAMPAIGN}" --job-id "${JOB_ID}" --max-events "${MAX_EVENTS}" --enable-ntuple "${ENABLE_NTUPLE}" --efficiency-ntuple "${EFFICIENCY_NTUPLE}" --shuffle-mixing "${SHUFFLE_MIXING}" --cleanup "${CLEANUP}"; then
    echo "ERROR: Processing chain failed" >&2
    exit 1
fi

echo "=== Processing chain completed successfully ==="
