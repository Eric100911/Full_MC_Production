#!/bin/bash
# ==============================================================================
# run_full_chain_test.sh — End-to-end local chain test: LHE → ntuple
#
# Uses real HELAC-Onia 500-event J/psi+g LHE samples committed alongside this
# script.  Measures wall time and peak RSS for the full processing chain inside
# the cmssw/el9 Singularity container (matching production processing.sub).
#
# Test flow:
#   1. Compile lhe_shuffle_split natively (el9)
#   2. Planner: shuffle-split both LHE files into 25-event blocks
#   3. Full chain: two-source DPS (shower → mix → gensim → raw → reco → miniaod → ntuple)
#   4. Validate ntuple: MC_GenPart_*, SingleJpsi*, trigger branches
#   5. Resource summary table
#
# Usage:
#   cd /afs/cern.ch/user/c/chiw/condor/Full_MC_Production
#   bash tests/local_chain_test/run_full_chain_test.sh
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# --------------------------------------------------------------------
# Test state
# --------------------------------------------------------------------
PASS_COUNT=0
FAIL_COUNT=0
START_TIME=$(date +%s)

msg_pass() { echo -e "\033[0;32m[PASS]\033[0m $*"; ((PASS_COUNT++)) || true; }
msg_fail() { echo -e "\033[0;31m[FAIL]\033[0m $*"; ((FAIL_COUNT++)) || true; }
msg_info() { echo -e "\033[1;33m[INFO]\033[0m $*"; }
msg_step() { echo ""; echo -e "\033[1;36m========================================\033[0m"; echo -e "\033[1;36m  $*\033[0m"; echo -e "\033[1;36m========================================\033[0m"; }

cleanup() {
    if [[ ${FAIL_COUNT} -eq 0 && -n "${WORKDIR:-}" && -d "${WORKDIR}" ]]; then
        echo ""
        echo "[INFO] All tests passed — workdir preserved: ${WORKDIR}"
        return 0
    fi
    if [[ -n "${WORKDIR:-}" && -d "${WORKDIR}" ]]; then
        cd / && rm -rf "${WORKDIR}"
    fi
}
trap cleanup EXIT

WORKDIR=$(mktemp -d /tmp/chiw/chain_test_XXXXX)
RESULT_FILE="${WORKDIR}/resource_summary.txt"

echo "Test directory: ${WORKDIR}"
echo "Start time: $(date)"
echo ""

# --------------------------------------------------------------------
# Step 0: Pre-flight checks
# --------------------------------------------------------------------
msg_step "Step 0: Pre-flight checks"

if [[ ! -f /cvmfs/cms.cern.ch/cmsset_default.sh ]]; then
    msg_fail "CVMFS not available — /cvmfs/cms.cern.ch/cmsset_default.sh missing"
    exit 1
fi
msg_pass "CVMFS accessible"

PROXY_PATH="${X509_USER_PROXY:-}"
if [[ -z "${PROXY_PATH}" ]]; then
    PROXY_PATH="/tmp/x509up_u$(id -u)"
fi
if [[ -f "${PROXY_PATH}" ]]; then
    PROXY_TIMELEFT=$(voms-proxy-info -timeleft -file "${PROXY_PATH}" 2>/dev/null || echo "0")
    msg_pass "Proxy: ${PROXY_PATH} (${PROXY_TIMELEFT}s remaining)"
else
    msg_info "No VOMS proxy found — pileup download may fail"
fi

# Host is el9 — run_chain.sh runs natively.  CMSSW_12 steps that need el8
# are wrapped internally via run_cmssw12_command → el8 Singularity container.
msg_pass "Host is el9 — running natively (CMSSW_12 steps auto-wrap in el8 container)"

LHE_100="${SCRIPT_DIR}/sample_jpsi_g_100.lhe.gz"
LHE_101="${SCRIPT_DIR}/sample_jpsi_g_101.lhe.gz"
for f in "$LHE_100" "$LHE_101"; do
    if [[ ! -f "$f" ]]; then msg_fail "LHE file missing: $f"; exit 1; fi
done
msg_pass "Test LHE files present (500 events each)"

# --------------------------------------------------------------------
# Step 1: Compile lhe_shuffle_split natively
# --------------------------------------------------------------------
msg_step "Step 1: Compile lhe_shuffle_split (native el9)"

SHUFFLE_BIN="${WORKDIR}/lhe_shuffle_split"
g++ -std=c++14 -O2 -Wall -o "${SHUFFLE_BIN}" \
    "${BASE_DIR}/lhe_generation/lhe_shuffle_split.cc" 2>&1
BIN_SIZE=$(stat -c%s "${SHUFFLE_BIN}")
echo "Binary: ${BIN_SIZE} bytes, $(file ${SHUFFLE_BIN} | cut -d, -f1-2)"
msg_pass "Compiled (${BIN_SIZE} bytes)"

# --------------------------------------------------------------------
# Step 2: Run planner on both LHE files
# --------------------------------------------------------------------
msg_step "Step 2: Planner (shuffle-split both LHE files)"

BLOCKS_100="${WORKDIR}/blocks_100"
BLOCKS_101="${WORKDIR}/blocks_101"
PLAN_OUT_100="${WORKDIR}/plan_out_100"
PLAN_OUT_101="${WORKDIR}/plan_out_101"

mkdir -p "${BLOCKS_100}" "${BLOCKS_101}" "${PLAN_OUT_100}" "${PLAN_OUT_101}"

for seed in 100 101; do
    lhe_var="LHE_${seed}"
    blocks_var="BLOCKS_${seed}"
    plan_var="PLAN_OUT_${seed}"
    lhe_file="${!lhe_var}"
    block_dir="${!blocks_var}"
    plan_dir="${!plan_var}"

    msg_info "Planning seed ${seed} (LHE: $(du -h ${lhe_file} | cut -f1))..."
    /usr/bin/time -v -o "${WORKDIR}/time_planner_${seed}.log" \
        python3 "${BASE_DIR}/tools/plan_lhe_blocks.py" \
            --pool-name pool_jpsi_CSCO_g \
            --helac-seed "${seed}" \
            --lhe-path "${lhe_file}" \
            --output-dir "${plan_dir}" \
            --events-per-block 25 \
            --shuffle-seed "${seed}037" \
            --shuffle-mode stratified \
            --n-strata auto \
            --block-output-dir "${block_dir}" \
            --lhe-shuffle-split-bin "${SHUFFLE_BIN}" 2>&1 | tail -3

    block_count=$(ls "${block_dir}/"*.lhe.gz 2>/dev/null | wc -l)
    block_size=$(du -sh "${block_dir}" 2>/dev/null | cut -f1)
    rss_kb=$(grep 'Maximum resident' "${WORKDIR}/time_planner_${seed}.log" | awk '{print $NF}')
    rss_mb=$((rss_kb / 1024))
    msg_pass "Seed ${seed}: ${block_count} blocks, ${block_size} total, ${rss_mb} MB peak RSS"
done

# --------------------------------------------------------------------
# Step 3: Full chain inside cmssw/el9 container
# --------------------------------------------------------------------
msg_step "Step 3: Full chain (shower → mix → gensim → raw → reco → miniaod → ntuple)"

CHAIN_WORKDIR="${WORKDIR}/chain_workdir"
mkdir -p "${CHAIN_WORKDIR}"

BLOCK_100=$(ls "${BLOCKS_100}/"*.lhe.gz 2>/dev/null | head -1)
BLOCK_101=$(ls "${BLOCKS_101}/"*.lhe.gz 2>/dev/null | head -1)

if [[ -z "${BLOCK_100}" || -z "${BLOCK_101}" ]]; then
    msg_fail "No blocks produced — cannot run chain"
    exit 1
fi

msg_info "Source 1 (seed 100, mode=normal): ${BLOCK_100}"
msg_info "Source 2 (seed 101, mode=phi_mpi_off): ${BLOCK_101}"
msg_info "Decompressing blocks for chain..."

zcat "${BLOCK_100}" > "${WORKDIR}/block_100.lhe"
zcat "${BLOCK_101}" > "${WORKDIR}/block_101.lhe"
EVENTS_100=$(grep -c '<event>' "${WORKDIR}/block_100.lhe")
EVENTS_101=$(grep -c '<event>' "${WORKDIR}/block_101.lhe")
msg_info "Events: ${EVENTS_100} + ${EVENTS_101}"

msg_info "Running chain natively on el9..."
msg_info "This will take ~45 minutes for all steps through ntuple..."

export X509_USER_PROXY="${PROXY_PATH}"
export LOCAL_OUTPUT_BASE="${CHAIN_WORKDIR}/output"
/usr/bin/time -v -o "${WORKDIR}/time_chain.log" \
    bash "${BASE_DIR}/processing/run_chain.sh" \
        --inputs "file:${WORKDIR}/block_100.lhe,file:${WORKDIR}/block_101.lhe" \
        --modes normal,phi_mpi_off \
        --analysis JJP \
        --campaign JJP_DPS1 \
        --job-id chain_test \
        --max-events -1 \
        --cleanup false \
        --workdir "${CHAIN_WORKDIR}" \
        --enable-ntuple true 2>&1

CHAIN_RC=${PIPESTATUS[0]}
# Stageout failures are expected in local tests without EOS write access.
# Only fail if actual processing outputs (MiniAOD, ntuple) are missing.
if [[ ! -f "${CHAIN_WORKDIR}/output_MINIAOD.root" ]]; then
    msg_fail "Chain missing MiniAOD output (exit ${CHAIN_RC})"
    echo "Check logs in: ${CHAIN_WORKDIR}/command_logs/"
    exit 1
fi

# Extract per-step timing from command_logs
CHAIN_WALL=$(grep 'Elapsed' "${WORKDIR}/time_chain.log" | awk '{print $NF}' | head -1)
CHAIN_RSS_KB=$(grep 'Maximum resident' "${WORKDIR}/time_chain.log" | awk '{print $NF}')
CHAIN_RSS_MB=$((CHAIN_RSS_KB / 1024))
msg_pass "Chain complete: wall=${CHAIN_WALL}, peak RSS=${CHAIN_RSS_MB} MB"

# Per-step wall times from command_log timestamps
msg_info "Per-step wall times (from command_logs):"
for log_stdout in "${CHAIN_WORKDIR}"/command_logs/*.stdout; do
    step_name=$(basename "$log_stdout" .stdout)
    # Extract step duration from log timestamps (format: start=HH:MM:SS)
    start_ts=$(head -1 "$log_stdout" 2>/dev/null | grep -oP '\d{2}:\d{2}:\d{2}' | head -1 || echo "?")
    echo "  ${step_name}: ${start_ts}"
done

# --------------------------------------------------------------------
# Step 4: Validate ntuple output
# --------------------------------------------------------------------
msg_step "Step 4: Validate ntuple"

NTUPLE_FILE="${CHAIN_WORKDIR}/output_ntuple.root"
MINIAOD_FILE="${CHAIN_WORKDIR}/output_MINIAOD.root"

for f in "${MINIAOD_FILE}" "${NTUPLE_FILE}"; do
    fname=$(basename "$f")
    if [[ -f "$f" ]]; then
        size=$(stat -c%s "$f")
        msg_pass "${fname}: $(numfmt --to=iec ${size})"
    else
        msg_fail "${fname}: MISSING"
    fi
done

# Branch validation
if [[ -f "${NTUPLE_FILE}" ]]; then
    msg_info "Checking ntuple branches..."
    BRANCH_CHECK=$(python3 -c "
import ROOT, sys
f = ROOT.TFile('${NTUPLE_FILE}')
if not f or f.IsZombie():
    print('FAIL: cannot open file')
    sys.exit(1)
t = f.Get('mkcands/X_data')
if not t:
    print('FAIL: cannot find mkcands/X_data tree')
    sys.exit(1)
branches = [b.GetName() for b in t.GetListOfBranches()]
# v2.0 uses flat vector branches; check for the actual names
checks = {
    'MC_GenPart': any('MC_GenPart' in b for b in branches),
    'SingleJpsi': any('SingleJpsi' in b for b in branches),
    'SinglePhi': any('SinglePhi' in b for b in branches),
    'Trigger (muIsJpsiTrigMatch)': any('muIsJpsiTrigMatch' in b for b in branches),
    'Muon (muPdgId)': any('muPdgId' in b for b in branches),
    'Event (evtNum)': any('evtNum' in b for b in branches),
    'RecoKaonTrack': any('RecoKaonTrack' in b for b in branches),
    'Jpsi_1': any('Jpsi_1_' in b for b in branches),
    'Phi_': any(b.startswith('Phi_') for b in branches),
}
all_ok = True
for name, ok in checks.items():
    status = 'OK' if ok else 'MISSING'
    if not ok: all_ok = False
    print(f'  {name}: {status}')
print(f'Entries: {t.GetEntries()}, Branches: {len(branches)}')
f.Close()
sys.exit(0 if all_ok else 1)
" 2>&1)
    echo "${BRANCH_CHECK}"
    if echo "${BRANCH_CHECK}" | grep -q 'MISSING'; then
        msg_fail "Some expected branches are missing"
    else
        msg_pass "All expected ntuple branches present"
    fi
fi

# --------------------------------------------------------------------
# Step 5: Resource summary
# --------------------------------------------------------------------
msg_step "Step 5: Resource summary"

ELAPSED=$(( $(date +%s) - START_TIME ))
ELAPSED_MIN=$(( ELAPSED / 60 ))

{
    echo "=============================================="
    echo "Full Chain Test — Resource Summary"
    echo "=============================================="
    echo "Date: $(date)"
    echo "Total elapsed: ${ELAPSED_MIN} min (${ELAPSED}s)"
    echo ""
    echo "--- Planner ---"
    for seed in 100 101; do
        rss=$(grep 'Maximum resident' "${WORKDIR}/time_planner_${seed}.log" 2>/dev/null | awk '{print $NF}')
        wall=$(grep 'Elapsed' "${WORKDIR}/time_planner_${seed}.log" 2>/dev/null | awk '{print $NF}')
        blocks=$(ls "${WORKDIR}/blocks_${seed}/"*.lhe.gz 2>/dev/null | wc -l)
        size=$(du -sh "${WORKDIR}/blocks_${seed}" 2>/dev/null | cut -f1)
        echo "  Seed ${seed}: ${blocks} blocks, ${size} total, ${wall:-?} wall, $((rss/1024)) MB peak RSS"
    done
    echo ""
    echo "--- Full Chain (shower → ntuple) ---"
    echo "  Wall time:     ${CHAIN_WALL:-?}"
    echo "  Peak RSS:      ${CHAIN_RSS_MB} MB"
    echo "  Workdir size:  $(du -sh ${CHAIN_WORKDIR} 2>/dev/null | cut -f1)"
    echo ""
    echo "--- Output files ---"
    for f in "${CHAIN_WORKDIR}"/*.root "${CHAIN_WORKDIR}"/*.hepmc; do
        if [[ -f "$f" ]]; then
            echo "  $(basename $f): $(du -h $f | cut -f1)"
        fi
    done
    echo ""
    echo "--- Recommendations ---"
    echo "  request_memory: $(( (CHAIN_RSS_MB + 1023) / 1024 )) GB  (peak RSS + safety margin)"
    CHAIN_DISK_KB=$(du -sk "${CHAIN_WORKDIR}" 2>/dev/null | cut -f1)
    CHAIN_DISK_GB=$(( (CHAIN_DISK_KB + 1048575) / 1048576 ))
    echo "  request_disk:   ${CHAIN_DISK_GB} GB  (intermediate + output files)"
    echo "  request_cpus:   2  (cmsRun uses 1 thread for this small test)"
    echo "  +MaxRuntime:    $(( (ELAPSED * 2 + 3599) / 3600 ))h  (2x margin)"
} | tee "${RESULT_FILE}"

# --------------------------------------------------------------------
# Final summary
# --------------------------------------------------------------------
echo ""
echo "=============================================="
echo "Results: ${PASS_COUNT} passed, ${FAIL_COUNT} failed"
echo "Test directory preserved: ${WORKDIR}"
echo "Resource summary: ${RESULT_FILE}"
echo "=============================================="

if [[ ${FAIL_COUNT} -gt 0 ]]; then
    exit 1
fi
exit 0
