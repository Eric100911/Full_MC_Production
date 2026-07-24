#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
HELPERS_DIR="${BASE_DIR}/processing/cmssw_helpers"
TEST_ROOT="$(mktemp -d /tmp/chiw/cmssw_helpers.XXXXXX)"

cleanup() {
    rm -rf "${TEST_ROOT}"
}
trap cleanup EXIT

mkdir -p \
    "${TEST_ROOT}/bin" \
    "${TEST_ROOT}/caller" \
    "${TEST_ROOT}/CMSSW_12/src" \
    "${TEST_ROOT}/CMSSW_15/src" \
    "${TEST_ROOT}/create_here"

cat > "${TEST_ROOT}/cmsset_default.sh" <<'EOF'
#!/bin/bash
export MOCK_CMSSET_SOURCED=1
EOF

cat > "${TEST_ROOT}/bin/scramv1" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "${MOCK_SCRAMV1_LOG}"
if [[ "${1:-}" == "runtime" && "${2:-}" == "-sh" ]]; then
    printf 'export MOCK_RUNTIME_LOADED=1\n'
fi
EOF

cat > "${TEST_ROOT}/bin/scram" <<'EOF'
#!/bin/bash
set -euo pipefail
{
    printf 'args=%s\n' "$*"
    printf 'pwd=%s\n' "${PWD}"
    printf 'arch=%s\n' "${SCRAM_ARCH:-}"
    printf 'runtime=%s\n' "${MOCK_RUNTIME_LOADED:-}"
} > "${MOCK_SCRAM_LOG}"
EOF

cat > "${TEST_ROOT}/bin/capture-command" <<'EOF'
#!/bin/bash
set -euo pipefail
{
    printf 'pwd=%s\n' "${PWD}"
    printf 'arch=%s\n' "${SCRAM_ARCH:-}"
    printf 'cmsset=%s\n' "${MOCK_CMSSET_SOURCED:-}"
    printf 'runtime=%s\n' "${MOCK_RUNTIME_LOADED:-}"
    for argument in "$@"; do
        printf 'arg=<%s>\n' "${argument}"
    done
} > "${MOCK_CAPTURE_OUTPUT}"
EOF

chmod +x \
    "${TEST_ROOT}/bin/scramv1" \
    "${TEST_ROOT}/bin/scram" \
    "${TEST_ROOT}/bin/capture-command"

export PATH="${TEST_ROOT}/bin:${PATH}"
export CMSSET_DEFAULT_SH="${TEST_ROOT}/cmsset_default.sh"
export MOCK_SCRAMV1_LOG="${TEST_ROOT}/scramv1.log"
export MOCK_SCRAM_LOG="${TEST_ROOT}/scram.log"
export MOCK_CAPTURE_OUTPUT="${TEST_ROOT}/capture.log"

assert_line() {
    local expected="$1"
    local path="$2"
    if ! grep -Fqx -- "${expected}" "${path}"; then
        echo "Missing expected line in ${path}: ${expected}" >&2
        sed -n '1,120p' "${path}" >&2
        exit 1
    fi
}

cd "${TEST_ROOT}/caller"

literal_substitution='literal$(touch should-not-exist)'
literal_glob='*.root'
/bin/bash "${HELPERS_DIR}/cmssw12_exec.sh" \
    "${TEST_ROOT}/CMSSW_12" \
    capture-command \
    "value with spaces" \
    "${literal_substitution}" \
    "${literal_glob}"

assert_line "pwd=${TEST_ROOT}/caller" "${MOCK_CAPTURE_OUTPUT}"
assert_line "arch=el8_amd64_gcc10" "${MOCK_CAPTURE_OUTPUT}"
assert_line "cmsset=1" "${MOCK_CAPTURE_OUTPUT}"
assert_line "runtime=1" "${MOCK_CAPTURE_OUTPUT}"
assert_line "arg=<value with spaces>" "${MOCK_CAPTURE_OUTPUT}"
assert_line "arg=<${literal_substitution}>" "${MOCK_CAPTURE_OUTPUT}"
assert_line "arg=<${literal_glob}>" "${MOCK_CAPTURE_OUTPUT}"
[[ ! -e "${TEST_ROOT}/caller/should-not-exist" ]]

: > "${MOCK_CAPTURE_OUTPUT}"
/bin/bash "${HELPERS_DIR}/cmssw15_exec.sh" \
    "${TEST_ROOT}/CMSSW_15" \
    capture-command \
    "inputFiles=file:/tmp/a file.root" \
    "maxEvents=-1"

assert_line "pwd=${TEST_ROOT}/CMSSW_15/src" "${MOCK_CAPTURE_OUTPUT}"
assert_line "arch=el9_amd64_gcc12" "${MOCK_CAPTURE_OUTPUT}"
assert_line "runtime=1" "${MOCK_CAPTURE_OUTPUT}"
assert_line "arg=<inputFiles=file:/tmp/a file.root>" "${MOCK_CAPTURE_OUTPUT}"
assert_line "arg=<maxEvents=-1>" "${MOCK_CAPTURE_OUTPUT}"

: > "${MOCK_SCRAMV1_LOG}"
/bin/bash "${HELPERS_DIR}/cmssw15_project_rename.sh" "${TEST_ROOT}/CMSSW_15"
assert_line "args=build ProjectRename" "${MOCK_SCRAM_LOG}"
assert_line "pwd=${TEST_ROOT}/CMSSW_15/src" "${MOCK_SCRAM_LOG}"
assert_line "arch=el9_amd64_gcc12" "${MOCK_SCRAM_LOG}"
assert_line "runtime=" "${MOCK_SCRAM_LOG}"
[[ ! -s "${MOCK_SCRAMV1_LOG}" ]]

: > "${MOCK_SCRAMV1_LOG}"
/bin/bash "${HELPERS_DIR}/cmssw15_project_create.sh" \
    "${TEST_ROOT}/create_here" \
    "CMSSW_15_0_15"
assert_line "project CMSSW CMSSW_15_0_15" "${MOCK_SCRAMV1_LOG}"

bundle_dir="${TEST_ROOT}/bundle"
python3 "${BASE_DIR}/dag_generator.py" prepare-runtime \
    --output-dir "${bundle_dir}" >/dev/null

bundle="${bundle_dir}/processing_runtime_bundle.tar.gz"
bundle_listing="${TEST_ROOT}/processing_bundle.list"
tar -tzf "${bundle}" > "${bundle_listing}"
for helper in \
    cmssw12_exec.sh \
    cmssw15_exec.sh \
    cmssw15_project_create.sh \
    cmssw15_project_rename.sh
do
    grep -Fqx "runtime/processing/cmssw_helpers/${helper}" "${bundle_listing}"
done

echo "CMSSW helper tests passed"
