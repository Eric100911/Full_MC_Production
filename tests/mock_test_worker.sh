#!/bin/bash
# ==============================================================================
# mock_test_worker.sh — Worker-node mock test using production tooling
# ==============================================================================
# Mirrors the exact production flow:
#   1. prepare-runtime  →  processing bundle (dag_generator.py)
#   2. Bundle + proxy + JSON config copied to worker working directory
#   3. run_processing.sh executes with those inputs
#
# Validates the worker infrastructure layer (bundles, wrapper, config, LHE
# resolution) through a short standard Pythia shower. It stops before CMSSW
# GEN-SIM/RAW/RECO/MiniAOD so the test does not require pileup access or full
# HTCondor production runtime.
#
# Host is el9 = production cmssw/el9 container OS, so bare execution is ABI-identical.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
PASS=0; FAIL=0
pass() { echo -e "${GREEN}[PASS]${NC} $1"; PASS=$((PASS+1)); }
fail() { echo -e "${RED}[FAIL]${NC} $1"; FAIL=$((FAIL+1)); }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

cleanup() {
    if [[ -n "${MOCK_DIR:-}" && -d "${MOCK_DIR}" ]]; then
        rm -rf "${MOCK_DIR}"
    fi
}
trap cleanup EXIT

MOCK_DIR=$(mktemp -d "/tmp/chiw/mock_test_worker_XXXXX")
info "Working dir: ${MOCK_DIR}"

# ── Sample LHE files (committed HELAC output, 500 events each) ─────────────
LHE_100="${BASE_DIR}/tests/local_chain_test/sample_jpsi_g_100.lhe.gz"
LHE_101="${BASE_DIR}/tests/local_chain_test/sample_jpsi_g_101.lhe.gz"
[[ -f "${LHE_100}" && -f "${LHE_101}" ]] || { fail "Sample LHE files missing"; exit 1; }
pass "Sample LHE files found"

# ── 2. Generate processing runtime bundle (production path) ──────────────────
info "Building processing runtime bundle via prepare-runtime..."
BUNDLE_DIR="${MOCK_DIR}/bundles"
mkdir -p "${BUNDLE_DIR}"
python3 "${BASE_DIR}/dag_generator.py" prepare-runtime \
    --output-dir "${BUNDLE_DIR}" > /dev/null 2>&1

PROCESSING_BUNDLE="${BUNDLE_DIR}/processing_runtime_bundle.tar.gz"
[[ -f "${PROCESSING_BUNDLE}" ]] || { fail "Processing bundle not created"; exit 1; }
pass "Processing bundle ready ($(du -h "${PROCESSING_BUNDLE}" | cut -f1))"

# ── 3. Dummy proxy bundle ──────────────────────────────────────────────────
mkdir -p "${BUNDLE_DIR}/proxy/credentials"
touch "${BUNDLE_DIR}/proxy/credentials/x509_user_proxy"
PROXY_BUNDLE="${BUNDLE_DIR}/proxy_bundle.tar.gz"
tar -czf "${PROXY_BUNDLE}" -C "${BUNDLE_DIR}/proxy" credentials/
pass "Proxy bundle ready"

# ── 4. JSON config (same format as dag_generator.py write_node_config()) ────
OUTPUT_DIR="${MOCK_DIR}/output"
CONFIG_JSON="${BUNDLE_DIR}/processing_config.json"
python3 -c "
import json
cfg = {
    'inputs': ['file:${LHE_100}', 'file:${LHE_101}'],
    'modes': ['normal', 'normal'],
    'analysis': 'JJP',
    'campaign': 'MOCK_TEST',
    'job_id': '0',
    'max_events': 5,
    'enable_ntuple': False,
    'efficiency_ntuple': False,
    'cleanup': False,
    'shuffle_mixing': False,
    'stop_at': 'shower',
    'local_output_base': '${OUTPUT_DIR}',
}
with open('${CONFIG_JSON}', 'w') as f:
    json.dump(cfg, f, indent=2)
"
pass "JSON config written"

# ── 5. Copy to worker working directory ────────────────────────────────────
WORK_DIR="${MOCK_DIR}/work"
mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}"
cp "${PROXY_BUNDLE}" "${PROCESSING_BUNDLE}" "${CONFIG_JSON}" "${WORK_DIR}/"
info "Bundles + config copied to worker area"

# ── 6. Execute worker wrapper ──────────────────────────────────────────────
info "Executing run_processing.sh (production worker entry point)..."

set +e
(
    cd "${WORK_DIR}"
    bash "${BASE_DIR}/processing/condor_wrappers/run_processing.sh" \
        "$(basename "${PROXY_BUNDLE}")" \
        "$(basename "${PROCESSING_BUNDLE}")" \
        "$(basename "${CONFIG_JSON}")" \
        2>&1 | tee "${MOCK_DIR}/wrapper.log"
)
WRAPPER_RC=$?
set -e

# ── 7. Validate infrastructure ─────────────────────────────────────────────
info "Validating infrastructure..."

# Check wrapper started (always true if we got here)
grep -q 'Processing Chain Wrapper' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "Wrapper started" \
    || fail "Wrapper did not start"

# Check bundles were extracted
grep -q 'Extracting proxy bundle' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "Proxy bundle extracted" \
    || fail "Proxy bundle not extracted"
grep -q 'Extracting processing bundle' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "Processing bundle extracted" \
    || fail "Processing bundle not extracted"

# Check config parsed and run_chain.sh launched
grep -q 'MC Production Chain' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "run_chain.sh launched with config" \
    || fail "run_chain.sh did not launch"

# Check LHE files were resolved
grep -q 'Source 1:.*sample_jpsi_g_100.lhe.gz' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "LHE source 1 resolved" \
    || fail "LHE source 1 not resolved"
grep -q 'Source 2:.*sample_jpsi_g_101.lhe.gz' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "LHE source 2 resolved" \
    || fail "LHE source 2 not resolved"

# Check CMSSW environment was set up (needed for shower)
grep -q 'CMSSW environment: CMSSW_12_4_14' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "CMSSW environment ready" \
    || fail "CMSSW environment not set up"

# Check shower binaries were validated/used
grep -q 'Reusing transferred shower' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "Shower binaries validated" \
    || fail "Shower binaries not validated"

# Check local compressed LHE inputs were normalized for the shower binaries
grep -q 'Decompressing .*sample_jpsi_g_100.lhe.gz' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "Compressed LHE source decompressed" \
    || fail "Compressed LHE source was not decompressed"

# Check Pythia started. Successful command stdout is captured under command_logs,
# not echoed to wrapper.log.
if grep -q 'PYTHIA version 8\.' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    || find "${WORK_DIR}" -path '*/command_logs/*.stdout' -type f -print0 \
        | xargs -0 grep -q 'PYTHIA version 8\.' 2>/dev/null; then
    pass "Pythia 8 started"
else
    fail "Pythia 8 did not start"
fi

# With --stop-at shower, the mock should produce both shower outputs locally.
find "${WORK_DIR}" -path '*/shower_0.hepmc' -type f -size +0c | grep -q . \
    && find "${WORK_DIR}" -path '*/shower_1.hepmc' -type f -size +0c | grep -q . \
    && pass "Shower outputs produced" \
    || fail "Shower outputs missing"

[[ ${WRAPPER_RC} -eq 0 ]] \
    && pass "Wrapper completed through shower stop point" \
    || fail "Wrapper failed before shower stop point"

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo -e "Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo "========================================"
echo ""
echo "NOTE: This mock test validates prepare-runtime → bundle → wrapper → config"
echo "through the standard Pythia shower stop point. Phi-enriched shower modes"
echo "and full CMSSW production validation still belong in container/Condor tests."

exit $(( FAIL > 0 ? 1 : 0 ))
