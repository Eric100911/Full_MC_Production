#!/bin/bash
# ==============================================================================
# run_ntuple_only.sh — HTCondor wrapper for standalone ntuple re-running.
#
# Usage (arguments passed positionally by condor submit file):
#   $1  ntuple_bundle_name   e.g. ntuple_runtime_bundle.tar.gz
#   $2  proxy_bundle_name    e.g. proxy_bundle.tar.gz
#   $3  analysis             e.g. JJP
#   $4  campaign             e.g. JJP_TPS
#   $5  job_id               e.g. 0
#   $6  max_events           e.g. -1
#   $7  efficiency_ntuple    true|false
#   $8  cleanup              true|false
#   $9  miniaod_input        file:/path/to/MINIAOD.root  or  root://...
#   $10 target_eos_base       override EOS_BASE for XRootD output (may be empty)
#   $11 custom_output_subpath override output subpath (may be empty)
#   $12 custom_ntuple_basename override remote ntuple filename (may be empty)
#   LOCAL_OUTPUT_BASE is read from the environment (injected by ntuple.sub).
# ==============================================================================

set -euo pipefail

NTUPLE_BUNDLE="${1:?}"
PROXY_BUNDLE="${2:?}"
ANALYSIS="${3:?}"
CAMPAIGN="${4:?}"
JOB_ID="${5:?}"
MAX_EVENTS="${6:--1}"
EFFICIENCY_NTUPLE="${7:-false}"
CLEANUP="${8:-true}"
MINIAOD_INPUT="${9:?}"
# LOCAL_OUTPUT_BASE is read from the environment (set by ntuple.sub Environment line).

TARGET_EOS_BASE="${10:-}"
CUSTOM_OUTPUT_SUBPATH="${11:-}"
CUSTOM_NTUPLE_BASENAME="${12:-}"

export LOCAL_OUTPUT_BASE TARGET_EOS_BASE CUSTOM_OUTPUT_SUBPATH CUSTOM_NTUPLE_BASENAME

echo "=== Ntuple-only Wrapper ==="
echo "Host: $(hostname)"
echo "Date: $(date -Iseconds)"
echo "Working dir: $(pwd)"
echo "Campaign: ${CAMPAIGN}"
echo "Job ID:   ${JOB_ID}"
echo "Analysis: ${ANALYSIS}"
echo "MiniAOD:  ${MINIAOD_INPUT}"
echo "LOCAL_OUTPUT_BASE: ${LOCAL_OUTPUT_BASE:-NOT SET}"

# --- Extract proxy bundle ---
echo "--- Extracting proxy bundle ---"
if ! tar -xzf "${PROXY_BUNDLE}"; then
    echo "FATAL: Failed to extract proxy bundle" >&2
    exit 1
fi

PROXY_TARGET="/tmp/x509up_u$(id -u)"
if ! install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"; then
    echo "FATAL: Failed to install proxy" >&2
    exit 1
fi
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"

# --- Extract ntuple runtime bundle ---
echo "--- Extracting ntuple runtime bundle ---"
if ! tar -xzf "${NTUPLE_BUNDLE}"; then
    echo "FATAL: Failed to extract ntuple runtime bundle" >&2
    exit 1
fi

# --- Ensure library path ---
export LD_LIBRARY_PATH="/usr/lib64:${LD_LIBRARY_PATH:-}"

# --- Run the ntuple chain ---
echo "--- Running ntuple chain ---"
cd runtime/processing

bash run_chain.sh \
    --inputs file:/dev/null \
    --modes normal \
    --analysis "${ANALYSIS}" \
    --campaign "${CAMPAIGN}" \
    --job-id "${JOB_ID}" \
    --max-events "${MAX_EVENTS}" \
    --enable-ntuple true \
    --efficiency-ntuple "${EFFICIENCY_NTUPLE}" \
    --cleanup "${CLEANUP}" \
    --skip-to ntuple \
    --miniaod-input "${MINIAOD_INPUT}" \
    --transfer-miniaod false

rc=$?
if [[ $rc -ne 0 ]]; then
    echo "FATAL: run_chain.sh exited with code ${rc}" >&2
    exit $rc
fi

echo "=== Ntuple-only wrapper completed successfully ==="
