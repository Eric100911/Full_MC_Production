#!/bin/bash
# Verify that stage_out rejects xrdcp false-success with a truncated remote
# file, retries, and accepts only a size-matched destination.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/stage_out_verification.XXXXXX")
trap 'rm -rf "${TEST_ROOT}"' EXIT

extract_stage_out() {
    sed -n '/^stage_out() {$/,/^}$/p' "${BASE_DIR}/processing/run_chain.sh"
}

extract_sync_stageout_target() {
    sed -n '/^sync_stageout_target() {$/,/^}$/p' "${BASE_DIR}/processing/run_chain.sh"
}

msg_info() {
    :
}
msg_ok() {
    :
}
msg_warn() {
    :
}
msg_error() {
    :
}
run_logged() {
    shift
    "$@"
}

EOS_BASE="root://mock.example:1094///store/default"
EOS_PATH_BASE="/store/default"
EOS_XRDFS_TARGET="root://mock.example:1094/"
EOS_OUTPUT_SUBDIR="output"
EOS_OUTPUT="${EOS_BASE}/${EOS_OUTPUT_SUBDIR}"
EOS_HOST="mock.example:1094"
EOS_REDIRECTOR="${EOS_HOST}"
LOCAL_FILE="${TEST_ROOT}/output.root"
printf '1234567890' > "${LOCAL_FILE}"
LOCAL_SIZE=$(stat -c '%s' "${LOCAL_FILE}")

source /dev/stdin < <(extract_sync_stageout_target)
source /dev/stdin < <(extract_stage_out)

assert_equal() {
    local expected="$1"
    local actual="$2"
    local label="$3"
    if [[ "${actual}" != "${expected}" ]]; then
        echo "FAIL: ${label}: expected ${expected}, got ${actual}" >&2
        exit 1
    fi
}

TARGET_EOS_BASE=""
sync_stageout_target
assert_equal "root://mock.example:1094///store/default" "${EOS_BASE}" "default upload base"
assert_equal "/store/default" "${EOS_PATH_BASE}" "default stat/rm base"
assert_equal "root://mock.example:1094/" "${EOS_XRDFS_TARGET}" "default xrdfs endpoint"

TARGET_EOS_BASE="root://override.example:1094///store/user/test/production_v4/"
sync_stageout_target
assert_equal "root://override.example:1094///store/user/test/production_v4" "${EOS_BASE}" "override upload base"
assert_equal "/store/user/test/production_v4" "${EOS_PATH_BASE}" "override stat/rm base"
assert_equal "root://override.example:1094/" "${EOS_XRDFS_TARGET}" "override xrdfs endpoint"

attempts=0
remote_size=0
last_upload_url=""
XRDFS_TRACE="${TEST_ROOT}/xrdfs.trace"
: > "${XRDFS_TRACE}"
run_xrdcp() {
    attempts=$((attempts + 1))
    last_upload_url="${*: -1}"
    if [[ "${attempts}" -eq 1 ]]; then
        remote_size=$((LOCAL_SIZE - 1))
    else
        remote_size="${LOCAL_SIZE}"
    fi
}
run_xrdfs() {
    local endpoint="$1"
    local operation="$2"
    local path="$3"
    : "${endpoint}" "${path}"
    case "${operation}" in
        stat)
            printf 'stat\t%s\n' "${path}" >> "${XRDFS_TRACE}"
            printf 'Size: %s\n' "${remote_size}"
            ;;
        rm)
            printf 'rm\t%s\n' "${path}" >> "${XRDFS_TRACE}"
            remote_size=0
            ;;
        *)
            return 2
            ;;
    esac
}

STAGEOUT_MAX_ATTEMPTS=3
stage_out "${LOCAL_FILE}" "campaign/job/output.root"
if [[ "${attempts}" -ne 2 ]]; then
    echo "FAIL: expected one retry after the truncated false-success" >&2
    exit 1
fi
assert_equal \
    "root://override.example:1094///store/user/test/production_v4/campaign/job/output.root" \
    "${last_upload_url}" "override upload URL"
assert_equal \
    "/store/user/test/production_v4/campaign/job/output.root" \
    "$(awk -F '\t' '$1 == "stat" {value=$2} END {print value}' "${XRDFS_TRACE}")" \
    "override stat path"
assert_equal \
    "/store/user/test/production_v4/campaign/job/output.root" \
    "$(awk -F '\t' '$1 == "rm" {value=$2} END {print value}' "${XRDFS_TRACE}")" \
    "override retry rm path"

attempts=0
remote_size=0
run_xrdcp() {
    attempts=$((attempts + 1))
    remote_size=1
}

set +e
stage_out "${LOCAL_FILE}" "campaign/job/persistently_bad.root"
rc=$?
set -e
if [[ "${rc}" -eq 0 || "${attempts}" -ne 3 ]]; then
    echo "FAIL: persistent size mismatch was not rejected after three attempts" >&2
    exit 1
fi

echo "stage_out verification tests passed"
