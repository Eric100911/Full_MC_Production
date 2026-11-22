#!/bin/bash
# ==============================================================================
# mock_test_worker.sh — Worker-node mock test using production tooling
# ==============================================================================
# Mirrors the exact production flow:
#   1. prepare-runtime  →  processing bundle (dag_generator.py)
#   2. Bundle + proxy + JSON config copied to worker working directory
#   3. run_processing.sh executes with those inputs
#
# Validates the worker infrastructure layer and the full production chain
# through MiniAOD, local stage-out, and the edmFileUtil event-count manifest.
# This requires CVMFS, a valid CMS proxy, premix access, and the nested EL8
# compatibility container used by CMSSW_12 on production EL9 workers.
#
# Run the wrapper inside cmssw/el9 so an uncontainerized EL8 edmFileUtil call
# fails here in the same way it fails on a production worker.
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
        if [[ "${KEEP_MOCK_WORKDIR:-0}" == "1" || "${FAIL:-0}" -gt 0 || "${WRAPPER_RC:-0}" -ne 0 ]]; then
            info "Preserving mock workdir for diagnostics: ${MOCK_DIR}"
        else
            rm -rf "${MOCK_DIR}"
        fi
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

# ── 3. Proxy bundle ──────────────────────────────────────────────────
mkdir -p "${BUNDLE_DIR}/proxy/credentials"
PROXY_SOURCE="${X509_USER_PROXY:-}"
if [[ -z "${PROXY_SOURCE}" || ! -s "${PROXY_SOURCE}" ]]; then
    PROXY_SOURCE="${BASE_DIR}/../x509up"
fi
if [[ ! -s "${PROXY_SOURCE}" ]]; then
    fail "A valid proxy is required; set X509_USER_PROXY"
    exit 1
fi
PROXY_TIMELEFT=$(voms-proxy-info --file "${PROXY_SOURCE}" --timeleft 2>/dev/null || echo 0)
if [[ ! "${PROXY_TIMELEFT}" =~ ^[0-9]+$ || "${PROXY_TIMELEFT}" -le 0 ]]; then
    fail "Proxy is invalid or expired: ${PROXY_SOURCE}"
    exit 1
fi
cp "${PROXY_SOURCE}" "${BUNDLE_DIR}/proxy/credentials/x509_user_proxy"
PROXY_BUNDLE="${BUNDLE_DIR}/proxy_bundle.tar.gz"
tar -czf "${PROXY_BUNDLE}" -C "${BUNDLE_DIR}/proxy" credentials/
pass "Proxy bundle ready (${PROXY_TIMELEFT}s remaining)"

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
    'max_events': 1,
    'target_mixed_events': 1,
    'enable_ntuple': False,
    'efficiency_ntuple': False,
    'cleanup': False,
    'shuffle_mixing': False,
    'edm_event_id': {
        'first_run': 1,
        'first_luminosity_block': 1,
        'first_event': 42,
        'reserved_events': 1,
        'number_events_in_luminosity_block': 0,
    },
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
info "Executing full worker chain inside the production-style EL9 container..."

set +e
(
    cd "${WORK_DIR}"
    /cvmfs/cms.cern.ch/common/cmssw-el9 \
        -B "${MOCK_DIR}" \
        --command-to-run \
        "/bin/bash ${BASE_DIR}/processing/condor_wrappers/run_processing.sh $(basename "${PROXY_BUNDLE}") $(basename "${PROCESSING_BUNDLE}") $(basename "${CONFIG_JSON}")" \
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

grep -q 'EDM EventID:  run=1 lumi=1 firstEvent=42 eventsPerLumi=0' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "EDM EventID config propagated to run_chain.sh" \
    || fail "EDM EventID config was not propagated"

# Check LHE files were resolved
grep -q 'Source slot 0:.*sample_jpsi_g_100.lhe.gz' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "LHE source 1 resolved" \
    || fail "LHE source 1 not resolved"
grep -q 'Source slot 1:.*sample_jpsi_g_101.lhe.gz' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "LHE source 2 resolved" \
    || fail "LHE source 2 not resolved"

# Check CMSSW environment was set up (needed for shower)
grep -q 'CMSSW environment: CMSSW_12_4_14' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
    && pass "CMSSW environment ready" \
    || fail "CMSSW environment not set up"

# Check shower binaries were validated/used
grep -Eq 'Reusing transferred shower|Rebuilding shower/mixer tools' "${MOCK_DIR}/wrapper.log" 2>/dev/null \
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

# The full mock retains worker intermediates for inspection.
find "${WORK_DIR}" -path '*/shower_0.hepmc' -type f -size +0c | grep -q . \
    && find "${WORK_DIR}" -path '*/shower_1.hepmc' -type f -size +0c | grep -q . \
    && pass "Shower outputs produced" \
    || fail "Shower outputs missing"

[[ ${WRAPPER_RC} -eq 0 ]] \
    && pass "Wrapper completed through MiniAOD and local transfer" \
    || fail "Wrapper failed before completing the full chain"

MINIAOD_OUTPUT="${OUTPUT_DIR}/output/MOCK_TEST/0/output_MINIAOD.root"
PROCESSING_MANIFEST="${OUTPUT_DIR}/output/MOCK_TEST/0/processing_manifest_MOCK_TEST_0.json"
[[ -s "${MINIAOD_OUTPUT}" ]] \
    && pass "MiniAOD output produced and copied locally" \
    || fail "MiniAOD output missing: ${MINIAOD_OUTPUT}"
[[ -s "${PROCESSING_MANIFEST}" ]] \
    && pass "Processing manifest produced" \
    || fail "Processing manifest missing: ${PROCESSING_MANIFEST}"

if [[ -s "${PROCESSING_MANIFEST}" ]]; then
    if python3 - "${PROCESSING_MANIFEST}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    manifest = json.load(handle)

assert manifest["status"] == "ok", manifest
assert manifest["complete"] is True, manifest
assert manifest["actual_mixed_hepmc_events"] == 1, manifest
assert manifest["actual_miniaod_events"] == 1, manifest
assert manifest["miniaod_count_source"] == "edmFileUtil", manifest
assert manifest["unused_hepmc_warning_fraction"] == 0.15, manifest
assert manifest["maximum_unused_event_fraction"] == 0.0, manifest
assert len(manifest["source_event_balance"]) == 2, manifest
assert all(
    item["assessment"] == "within_threshold"
    and item["unused_hepmc_events"] == 0
    for item in manifest["source_event_balance"]
), manifest
PY
    then
        pass "Manifest records verified MiniAOD count and source-event balance"
    else
        fail "Processing manifest does not contain the verified edmFileUtil count"
    fi
fi

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo -e "Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo "========================================"
echo ""
echo "NOTE: This mock validates prepare-runtime → bundle → wrapper → shower"
echo "→ mix → GEN-SIM → RAW → RECO → MiniAOD → edmFileUtil → local transfer."

exit $(( FAIL > 0 ? 1 : 0 ))
