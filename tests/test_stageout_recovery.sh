#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/stageout_recovery.XXXXXX")
trap 'rm -rf "${TEST_ROOT}"' EXIT

extract_function() {
    local name="$1"
    sed -n "/^${name}() {$/,/^}$/p" "${BASE_DIR}/processing/run_chain.sh"
}

msg_step() { :; }
msg_info() { :; }
msg_ok() { printf '%s\n' "$1"; }
msg_warn() { :; }
msg_error() { printf '%s\n' "$1" >&2; }
run_logged() {
    shift
    "$@"
}

WORKDIR="${TEST_ROOT}/work"
mkdir -p "${WORKDIR}"
CAMPAIGN_NAME="TEST_CAMPAIGN"
JOB_ID="JOB000001_BLOCK000002"
EOS_BASE="root://mock.example:1094///store/user/test/target"
EOS_PATH_BASE="/store/user/test/target"
EOS_XRDFS_TARGET="root://mock.example:1094/"
PROCESSING_RUNTIME_BUNDLE_SHA256="bundle-sha"
REMOTE_MANIFEST="${TEST_ROOT}/remote_manifest.json"
STAGE_TRACE="${TEST_ROOT}/stage.trace"

cat > "${REMOTE_MANIFEST}" <<EOF
{
  "campaign": "${CAMPAIGN_NAME}",
  "job_id": "${JOB_ID}",
  "status": "failed",
  "complete": false,
  "failure_reason": "transfer_failed",
  "actual_miniaod_events": 822,
  "miniaod_url": "${EOS_BASE}/output/${CAMPAIGN_NAME}/${JOB_ID}/output_MINIAOD.root"
}
EOF

run_xrdfs() {
    local endpoint="$1"
    local operation="$2"
    local path="$3"
    : "${endpoint}"
    [[ "${operation}" == "stat" ]] || return 2
    case "${path}" in
        */output_MINIAOD.root) printf 'Size: 58376696\n' ;;
        */processing_manifest_*.json) printf 'Size: %s\n' "$(stat -c '%s' "${REMOTE_MANIFEST}")" ;;
        *) return 1 ;;
    esac
}

run_xrdcp() {
    local destination="${*: -1}"
    cp "${REMOTE_MANIFEST}" "${destination}"
}

count_root_events() {
    printf '822\n'
}

make_remote_dir() {
    printf 'mkdir\t%s\n' "$1" >> "${STAGE_TRACE}"
}

stage_out() {
    local local_file="$1"
    local remote_subpath="$2"
    [[ -s "${local_file}" ]]
    printf 'stage\t%s\t%s\n' "${local_file}" "${remote_subpath}" >> "${STAGE_TRACE}"
}

source /dev/stdin < <(extract_function recover_existing_stageout)

recover_existing_stageout

REPAIRED="${WORKDIR}/processing_manifest_${CAMPAIGN_NAME}_${JOB_ID}.json"
python3 - "${REPAIRED}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    manifest = json.load(handle)
assert manifest["status"] == "ok"
assert manifest["complete"] is True
assert manifest["failure_reason"] is None
assert manifest["actual_miniaod_events"] == 822
assert manifest["recovery"]["mode"] == "validate-existing"
assert manifest["recovery"]["miniaod_remote_size"] == 58376696
assert manifest["recovery"]["runtime_bundle_sha256"] == "bundle-sha"
PY

grep -Eq $'stage\t.*original_processing_manifest_[0-9a-f]{64}\\.json' "${STAGE_TRACE}"
grep -Fq $'stage\t'"${REPAIRED}"$'\toutput/'"${CAMPAIGN_NAME}/${JOB_ID}/processing_manifest_${CAMPAIGN_NAME}_${JOB_ID}.json" "${STAGE_TRACE}"

# A mismatched ROOT count must fail before any manifest rewrite.
: > "${STAGE_TRACE}"
count_root_events() {
    printf '821\n'
}
if recover_existing_stageout; then
    echo "FAIL: mismatched ROOT event count was accepted" >&2
    exit 1
fi
if [[ -s "${STAGE_TRACE}" ]]; then
    echo "FAIL: recovery mutated storage after validation failure" >&2
    exit 1
fi

echo "stageout recovery tests passed"
