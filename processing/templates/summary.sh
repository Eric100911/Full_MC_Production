#!/bin/bash
# Top-level DAGMan FINAL worker: report upstream state and archive staged logs.

set -u

if [[ $# -ne 8 ]]; then
    echo "ERROR: expected 8 arguments, got $#" >&2
    exit 2
fi

summary_bundle=$1
proxy_bundle=$2
log_root=$3
target_base_url=$4
workflow_archive_id=$5
archive_enabled=$6
dag_status=$7
failed_count=$8

echo "=========================================="
echo "Top-level FINAL summary"
echo "workflow_archive_id=${workflow_archive_id}"
echo "dag_status=${dag_status}"
echo "failed_count=${failed_count}"
echo "archive_enabled=${archive_enabled}"
echo "log_root=${log_root}"
echo "target_base_url=${target_base_url}"

archive_rc=0
if [[ "${archive_enabled}" == "true" ]]; then
    if ! tar -xzf "${summary_bundle}"; then
        echo "ERROR: failed to extract summary runtime bundle" >&2
        archive_rc=2
    else
        python3 runtime/tools/archive_workflow_logs.py \
        --log-root "${log_root}" \
        --target-base-url "${target_base_url}" \
        --workflow-id "${workflow_archive_id}" \
        --proxy-bundle "${proxy_bundle}" \
        --retry-attempts 3
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
