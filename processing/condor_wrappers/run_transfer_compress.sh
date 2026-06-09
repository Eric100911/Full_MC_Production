#!/bin/bash

set -e

COMPRESSION_BUNDLE="$1"
PROXY_BUNDLE="$2"
SOURCE_PREFIX="$3"
TARGET_SUBDIR="$4"
INPUT_LIST="$5"
LEVEL="${6:-1}"
CLEAN_SOURCE="${7:-false}"

WORKDIR="$(pwd)"
TARGET_BASE="root://cceos.ihep.ac.cn//eos/ihep/cms/store/user/chiw/MC_Production_v3/LHE_pool"

echo "=== LHE Transfer-Compress Job ==="
echo "Working directory: ${WORKDIR}"
echo "Source prefix:   ${SOURCE_PREFIX}"
echo "Target subdir:   ${TARGET_SUBDIR}"
echo "Input list:      ${INPUT_LIST}"
echo "Level:           ${LEVEL}"
echo "Clean source:    ${CLEAN_SOURCE}"
echo ""

# ------------------------------------------------------------------
# 1. Setup XRootD via LCG_109a
# ------------------------------------------------------------------
echo "Setting up XRootD from LCG_109a..."
if [[ -f /cvmfs/sft.cern.ch/lcg/views/LCG_109a/x86_64-el9-gcc15-opt/setup.sh ]]; then
    source /cvmfs/sft.cern.ch/lcg/views/LCG_109a/x86_64-el9-gcc15-opt/setup.sh
else
    echo "ERROR: LCG_109a not available on this node" >&2
    exit 1
fi
# LCG setup resets shell options; restore strict mode
set -euo pipefail

run_xrdfs() { xrdfs "$@"; }
run_xrdcp()  { xrdcp "$@"; }

if ! command -v xrdcp >/dev/null 2>&1; then
    echo "ERROR: xrdcp not found after sourcing LCG_109a" >&2
    exit 1
fi
echo "XRootD ready: $(which xrdcp)"

# ------------------------------------------------------------------
# 2. Extract proxy bundle and install proxy
# ------------------------------------------------------------------
echo "Extracting proxy bundle..."
if ! tar -xzf "${PROXY_BUNDLE}"; then
    echo "ERROR: Failed to extract proxy bundle" >&2
    exit 1
fi

PROXY_TARGET="/tmp/x509up_u$(id -u)"
if ! install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"; then
    echo "ERROR: Failed to install proxy" >&2
    exit 1
fi
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"
echo "X509_USER_PROXY=${X509_USER_PROXY}"

# ------------------------------------------------------------------
# 3. Download source LHE files into staging directory
# ------------------------------------------------------------------
STAGING_DIR="${WORKDIR}/staging"
mkdir -p "${STAGING_DIR}"

echo "Downloading source LHE files..."
DOWNLOAD_COUNT=0
while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    BASENAME="$(basename "${line}")"
    SOURCE_URL="${SOURCE_PREFIX}/${BASENAME}"
    LOCAL_PATH="${STAGING_DIR}/${BASENAME}"

    echo "  [${DOWNLOAD_COUNT}] ${SOURCE_URL} -> ${LOCAL_PATH}"
    if ! run_xrdcp -f "${SOURCE_URL}" "${LOCAL_PATH}"; then
        echo "ERROR: Failed to download ${SOURCE_URL}" >&2
        exit 1
    fi
    DOWNLOAD_COUNT=$((DOWNLOAD_COUNT + 1))
done < "${INPUT_LIST}"

echo "Downloaded ${DOWNLOAD_COUNT} files."

# ------------------------------------------------------------------
# 4. Extract compression bundle and compress
# ------------------------------------------------------------------
echo "Extracting compression bundle..."
if ! tar -xzf "${COMPRESSION_BUNDLE}"; then
    echo "ERROR: Failed to extract compression bundle" >&2
    exit 1
fi

echo "Running compression..."
cd runtime/tools
python3 compress_existing_lhe.py \
    --pool-dir "${STAGING_DIR}" \
    --level "${LEVEL}" \
    --keep \
    --output-manifest "${WORKDIR}/compression_manifest.json"
cd "${WORKDIR}"
echo "Compression complete."

# ------------------------------------------------------------------
# 5. Upload .lhe.gz files to target
# ------------------------------------------------------------------
echo "Uploading compressed files..."
TARGET_PREFIX="${TARGET_BASE}/${TARGET_SUBDIR}"

# Ensure target directory exists
run_xrdfs cceos.ihep.ac.cn mkdir -p "/eos/ihep/cms/store/user/chiw/MC_Production_v3/LHE_pool/${TARGET_SUBDIR}" 2>/dev/null || true

UPLOAD_COUNT=0
for gz_file in "${STAGING_DIR}"/*.lhe.gz; do
    [[ -f "${gz_file}" ]] || continue
    BASENAME="$(basename "${gz_file}")"
    TARGET_URL="${TARGET_PREFIX}/${BASENAME}"

    echo "  [${UPLOAD_COUNT}] ${gz_file} -> ${TARGET_URL}"
    if ! run_xrdcp --nopbar --force "${gz_file}" "${TARGET_URL}"; then
        echo "ERROR: Failed to upload ${gz_file}" >&2
        exit 1
    fi

    # Verify: check remote file exists and size matches local
    LOCAL_SIZE=$(stat -c%s "${gz_file}")
    REMOTE_STAT=$(run_xrdfs cceos.ihep.ac.cn stat "/eos/ihep/cms/store/user/chiw/MC_Production_v3/LHE_pool/${TARGET_SUBDIR}/${BASENAME}" 2>/dev/null || echo "")
    if [[ -z "${REMOTE_STAT}" ]]; then
        echo "ERROR: Remote file missing after upload: ${TARGET_URL}" >&2
        exit 1
    fi
    echo "    Verified: ${LOCAL_SIZE} bytes uploaded"
    UPLOAD_COUNT=$((UPLOAD_COUNT + 1))
done
echo "Uploaded ${UPLOAD_COUNT} files."

# ------------------------------------------------------------------
# 6. Optionally clean source LHE files
# ------------------------------------------------------------------
if [[ "${CLEAN_SOURCE}" == "true" ]]; then
    echo "Cleaning source files..."
    CLEAN_COUNT=0
    while IFS= read -r line; do
        [[ -z "${line}" ]] && continue
        BASENAME="$(basename "${line}")"
        REMOTE_PATH="/eos/ihep/cms/store/user/xcheng/MC_Production_v3/lhe_pools/${SOURCE_PREFIX##*/lhe_pools/}/${BASENAME}"
        echo "  Removing source: ${REMOTE_PATH}"
        run_xrdfs cceos.ihep.ac.cn rm "${REMOTE_PATH}" 2>/dev/null || echo "  WARNING: Could not remove source ${REMOTE_PATH}"
        CLEAN_COUNT=$((CLEAN_COUNT + 1))
    done < "${INPUT_LIST}"
    echo "Cleaned ${CLEAN_COUNT} source files."
fi

# ------------------------------------------------------------------
# 7. Write transfer manifest
# ------------------------------------------------------------------
python3 -c "
import json, os, glob, hashlib
from datetime import datetime, timezone

results = []
for gz_file in sorted(glob.glob('${STAGING_DIR}/*.lhe.gz')):
    basename = os.path.basename(gz_file)
    results.append({
        'file': basename,
        'compressed_size': os.path.getsize(gz_file),
        'target_url': '${TARGET_PREFIX}/' + basename,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    })
with open('${WORKDIR}/transfer_manifest.json', 'w') as f:
    json.dump(results, f, indent=2)
    f.write('\n')
print(f'Transfer manifest: {len(results)} entries')
"

echo "=== Transfer-Compress complete ==="
echo "Summary: ${DOWNLOAD_COUNT} downloaded, ${UPLOAD_COUNT} uploaded"
