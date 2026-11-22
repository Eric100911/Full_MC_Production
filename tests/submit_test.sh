#!/bin/bash
# ==============================================================================
# tests/submit_test.sh — submit test_submit.sub with workspace-derived base_dir
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/paths.sh"

exec condor_submit "${SCRIPT_DIR}/test_submit.sub" \
    -append "base_dir = ${WORKSPACE_ROOT}" \
    "$@"
