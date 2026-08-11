#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/processing_recovery_fallback.XXXXXX")
trap 'rm -rf "${TEST_ROOT}"' EXIT

mkdir -p \
    "${TEST_ROOT}/proxy/credentials" \
    "${TEST_ROOT}/bundle/runtime/processing" \
    "${TEST_ROOT}/work"
printf 'test proxy\n' > "${TEST_ROOT}/proxy/credentials/x509_user_proxy"

cat > "${TEST_ROOT}/bundle/runtime/processing/run_chain.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\t%s\n' "${PROCESSING_STAGEOUT_RECOVERY:-unset}" "$*" >> "${FAKE_CHAIN_TRACE}"
if [[ " $* " == *" --stageout-recovery validate-existing "* ]]; then
    exit 20
fi
[[ "${PROCESSING_STAGEOUT_RECOVERY:-unset}" == "none" ]]
EOF
chmod +x "${TEST_ROOT}/bundle/runtime/processing/run_chain.sh"

tar -czf "${TEST_ROOT}/work/proxy_bundle.tar.gz" \
    -C "${TEST_ROOT}/proxy" credentials
tar -czf "${TEST_ROOT}/work/processing_runtime_bundle.tar.gz" \
    -C "${TEST_ROOT}/bundle" runtime

cat > "${TEST_ROOT}/work/processing_config.json" <<'EOF'
{
  "inputs": ["file:unused.lhe"],
  "modes": ["normal"],
  "analysis": "JJP",
  "campaign": "TEST",
  "job_id": "JOB000001_BLOCK000002",
  "max_events": 1,
  "enable_ntuple": false,
  "efficiency_ntuple": false,
  "cleanup": true,
  "shuffle_mixing": false
}
EOF

export FAKE_CHAIN_TRACE="${TEST_ROOT}/chain.trace"
export PROCESSING_STAGEOUT_RECOVERY="validate-existing-or-rerun"
(
    cd "${TEST_ROOT}/work"
    "${BASE_DIR}/processing/condor_wrappers/run_processing.sh" \
        proxy_bundle.tar.gz \
        processing_runtime_bundle.tar.gz \
        processing_config.json
)

if [[ "$(wc -l < "${FAKE_CHAIN_TRACE}")" -ne 2 ]]; then
    echo "FAIL: expected validation plus one full-chain fallback" >&2
    cat "${FAKE_CHAIN_TRACE}" >&2
    exit 1
fi

sed -n '1p' "${FAKE_CHAIN_TRACE}" |
    grep -Fq $'validate-existing-or-rerun\t'
sed -n '1p' "${FAKE_CHAIN_TRACE}" |
    grep -Fq -- '--stageout-recovery validate-existing'
sed -n '2p' "${FAKE_CHAIN_TRACE}" |
    grep -Fq $'none\t'
if sed -n '2p' "${FAKE_CHAIN_TRACE}" | grep -Fq -- '--stageout-recovery'; then
    echo "FAIL: fallback retained a recovery CLI option" >&2
    exit 1
fi

echo "processing recovery fallback test passed"
