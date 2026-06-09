#!/bin/bash

set -euo pipefail

COMPRESSION_BUNDLE="$1"
POOL_DIR="$2"
LEVEL="${3:-1}"
KEEP="${4:-false}"

echo "=== LHE Compression Job ==="
echo "Working directory: $(pwd)"
echo "Pool dir: ${POOL_DIR}"
echo "Level: ${LEVEL}"
echo "Keep original: ${KEEP}"
echo ""

if ! command -v tar >/dev/null 2>&1; then
    echo "ERROR: tar command not found" >&2
    exit 1
fi

echo "Extracting compression bundle..."
if ! tar -xzf "${COMPRESSION_BUNDLE}"; then
    echo "ERROR: Failed to extract compression bundle" >&2
    exit 1
fi

KEEP_FLAG=""
if [[ "${KEEP}" == "true" ]]; then
    KEEP_FLAG="--keep"
fi

echo "Running compression..."
cd runtime/tools
python3 compress_existing_lhe.py \
    --pool-dir "${POOL_DIR}" \
    --level "${LEVEL}" \
    ${KEEP_FLAG:+"${KEEP_FLAG}"} \
    --output-manifest compression_manifest.json

echo "=== Compression complete ==="
