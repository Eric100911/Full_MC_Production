#!/bin/bash
# Top-level DAGMan FINAL worker: report upstream state and archive staged logs.

set -u

if [[ $# -ne 9 ]]; then
    echo "ERROR: expected 9 arguments, got $#" >&2
    exit 2
fi

summary_bundle=$1
proxy_bundle=$2
log_root=$3
subdag_root=$4
target_base_url=$5
workflow_archive_id=$6
archive_enabled=$7
dag_status=$8
failed_count=$9

WRAPPER_LOG_DIR="${PWD}/wrapper_logs"
mkdir -p "${WRAPPER_LOG_DIR}"
if command -v tee >/dev/null 2>&1; then
    exec > >(tee -a "${WRAPPER_LOG_DIR}/wrapper.stdout") \
        2> >(tee -a "${WRAPPER_LOG_DIR}/wrapper.stderr" >&2)
else
    exec >"${WRAPPER_LOG_DIR}/wrapper.stdout" 2>"${WRAPPER_LOG_DIR}/wrapper.stderr"
fi

# The submit environment carries the workflow log level (empty for DAGs
# generated before the variable existed, hence the info fallback).
WORKFLOW_LOG_LEVEL="${WORKFLOW_LOG_LEVEL:-info}"
_slim() { [[ "${WORKFLOW_LOG_LEVEL}" == "slim" ]]; }
msg_info() { _slim || echo "$@"; }

msg_info "=========================================="
msg_info "Top-level FINAL summary"
echo "workflow_archive_id=${workflow_archive_id}"
echo "dag_status=${dag_status}"
echo "failed_count=${failed_count}"
msg_info "archive_enabled=${archive_enabled}"
msg_info "log_root=${log_root}"
msg_info "subdag_root=${subdag_root}"
msg_info "target_base_url=${target_base_url}"

archive_rc=0
if [[ "${archive_enabled}" == "true" ]]; then
    if ! tar -xzf "${summary_bundle}"; then
        echo "ERROR: failed to extract summary runtime bundle" >&2
        archive_rc=2
    else
        python3 runtime/tools/archive_workflow_logs.py \
        --log-root "${log_root}" \
        --subdag-root "${subdag_root}" \
        --target-base-url "${target_base_url}" \
        --workflow-id "${workflow_archive_id}" \
        --proxy-bundle "${proxy_bundle}" \
        --retry-attempts 3 \
        --dag-status "${dag_status}" \
        --failed-count "${failed_count}" \
        --log-level "${WORKFLOW_LOG_LEVEL}"
        archive_rc=$?
        # The helper owns persistent diagnostics. Its failure is intentionally
        # fail-soft relative to the upstream physics DAG.
    fi
else
    echo "Log archival disabled for this workflow."
fi

echo "archive_rc=${archive_rc}"
echo "=========================================="

# A FINAL node determines the DAG's final status. Preserve the upstream DAG
# result and never let a log-only failure mask or invalidate physics work.
if [[ "${dag_status}" == "0" && "${failed_count}" == "0" ]]; then
    exit 0
fi
exit 1
