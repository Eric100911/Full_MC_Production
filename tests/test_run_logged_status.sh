#!/bin/bash
# Verify that both production command-log wrappers preserve command status.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/run_logged_status.XXXXXX")
trap 'rm -rf "${TEST_ROOT}"' EXIT

extract_run_logged() {
    local source_file="$1"
    sed -n '/^run_logged() {$/,/^}$/p' "${source_file}"
}

test_wrapper() (
    local source_file="$1"
    local case_name="$2"

    JOB_LOG_DIR="${TEST_ROOT}/${case_name}"
    COMMAND_LOG_INDEX=0

    ensure_job_log_dir() {
        mkdir -p "${JOB_LOG_DIR}"
    }
    sanitize_log_label() {
        printf '%s' "$1"
    }
    msg_info() {
        :
    }
    msg_ok() {
        :
    }
    msg_error() {
        :
    }
    show_log_tail() {
        :
    }

    # Load the production function itself, while avoiding each worker script's
    # top-level execution.
    source /dev/stdin < <(extract_run_logged "${source_file}")

    fail_with_42() {
        printf 'expected stdout\n'
        printf 'expected stderr\n' >&2
        return 42
    }

    set +e
    run_logged "failure_case" fail_with_42
    local rc=$?
    set -e

    if [[ "${rc}" -ne 42 ]]; then
        echo "FAIL: ${case_name} returned ${rc}, expected 42" >&2
        return 1
    fi
    grep -q "expected stdout" "${JOB_LOG_DIR}/001_failure_case.stdout"
    grep -q "expected stderr" "${JOB_LOG_DIR}/001_failure_case.stderr"

    run_logged "success_case" true
    if [[ "${COMMAND_LOG_INDEX}" -ne 2 ]]; then
        echo "FAIL: ${case_name} did not run both test commands" >&2
        return 1
    fi
)

test_wrapper "${BASE_DIR}/processing/run_chain.sh" "processing"
test_wrapper "${BASE_DIR}/lhe_generation/run_helac.sh" "lhe"

echo "run_logged status propagation tests passed"
