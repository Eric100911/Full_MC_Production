#!/bin/bash
# ==============================================================================
# mock_test_edm_eventid.sh -- local GEN-SIM EventID mock with real HepMC input
# ==============================================================================
# Uses a proper HepMC file extracted from an existing runtime output.  No
# synthetic HepMC content is generated here.
#
# Optional:
#   HEPMC_FIXTURE=/path/to/mixed.hepmc ./tests/mock_test_edm_eventid.sh
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

WORKDIR="${EDM_EVENTID_WORKDIR:-$(mktemp -d "/tmp/chiw/mock_test_edm_eventid_XXXXX")}"
KEEP_WORKDIR="${KEEP_WORKDIR:-0}"

cleanup() {
    if [[ "${KEEP_WORKDIR}" != "1" && -d "${WORKDIR}" ]]; then
        rm -rf "${WORKDIR}"
    fi
}
trap cleanup EXIT

find_runtime_hepmc() {
    if [[ -n "${HEPMC_FIXTURE:-}" ]]; then
        [[ -s "${HEPMC_FIXTURE}" ]] || fail "HEPMC_FIXTURE not found or empty: ${HEPMC_FIXTURE}"
        printf '%s\n' "${HEPMC_FIXTURE}"
        return 0
    fi

    local candidate
    for candidate in \
        "${BASE_DIR}/JJP_SPS_CS_CHAIN_TEST_0/mixed.hepmc" \
        "${BASE_DIR}/tests/fixtures/mixed.hepmc"
    do
        if [[ -s "${candidate}" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    done

    candidate=$(
        find "${BASE_DIR}" \
            -path '*/mixed.hepmc' \
            -type f \
            -size +0c \
            -print 2>/dev/null \
            | sort \
            | head -n 1
    )
    if [[ -n "${candidate}" && -s "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
    fi

    return 1
}

setup_cmssw12() {
    if [[ ! -f "/cvmfs/cms.cern.ch/cmsset_default.sh" ]]; then
        fail "CVMFS CMS setup not available"
    fi

    export HOME="${WORKDIR}/home"
    mkdir -p "${HOME}"
    # Keep git from trying to update the real AFS ~/.gitconfig during scram setup.
    export GIT_CONFIG_GLOBAL="${HOME}/.gitconfig"

    source /cvmfs/cms.cern.ch/cmsset_default.sh
    export SCRAM_ARCH=el8_amd64_gcc10

    local cmssw_src="${BASE_DIR}/JJP_SPS_CS_CHAIN_TEST_0/CMSSW_12_4_14/src"
    if [[ ! -d "${cmssw_src}" ]]; then
        cmssw_src="${WORKDIR}/CMSSW_12_4_14/src"
        if [[ ! -d "${cmssw_src}" ]]; then
            info "Creating temporary CMSSW_12_4_14 area"
            (cd "${WORKDIR}" && scramv1 project CMSSW CMSSW_12_4_14 >/dev/null)
        fi
    fi

    cd "${cmssw_src}"
    eval "$(scramv1 runtime -sh)"
    cd "${WORKDIR}"

    command -v cmsRun >/dev/null 2>&1 || fail "cmsRun not available after CMSSW setup"
    command -v edmFileUtil >/dev/null 2>&1 || fail "edmFileUtil not available after CMSSW setup"
    pass "CMSSW_12_4_14 runtime ready"
}

run_case() {
    local name="$1"
    local output="$2"
    local first_lumi="$3"
    local first_event="$4"
    local events_per_lumi="$5"
    local max_events="$6"
    local log_file="${WORKDIR}/${name}.log"

    info "Running ${name}: firstLumi=${first_lumi} firstEvent=${first_event} eventsPerLumi=${events_per_lumi}"
    cmsRun "${BASE_DIR}/common/cmssw_configs/hepmc_to_GENSIM.py" \
        inputFiles="file:${WORKDIR}/input.hepmc" \
        outputFile="file:${output}" \
        maxEvents="${max_events}" \
        nThreads=1 \
        firstRun=1 \
        firstLuminosityBlock="${first_lumi}" \
        firstEvent="${first_event}" \
        numberEventsInLuminosityBlock="${events_per_lumi}" \
        >"${log_file}" 2>&1

    [[ -s "${output}" ]] || fail "${name} did not create ${output}; see ${log_file}"
    edmFileUtil -f "${output}" -e >"${WORKDIR}/${name}.events"
}

expect_event() {
    local events_file="$1"
    local run="$2"
    local lumi="$3"
    local event="$4"
    awk -v run="${run}" -v lumi="${lumi}" -v event="${event}" '
        $1 == run && $2 == lumi && $3 == event { found = 1 }
        END { exit found ? 0 : 1 }
    ' "${events_file}" || fail "Missing EventID ${run}:${lumi}:${event} in ${events_file}"
}

reject_event() {
    local events_file="$1"
    local run="$2"
    local lumi="$3"
    local event="$4"
    if awk -v run="${run}" -v lumi="${lumi}" -v event="${event}" '
        $1 == run && $2 == lumi && $3 == event { found = 1 }
        END { exit found ? 0 : 1 }
    ' "${events_file}"; then
        fail "Unexpected EventID ${run}:${lumi}:${event} in ${events_file}"
    fi
}

mkdir -p "${WORKDIR}"
info "Working dir: ${WORKDIR}"

RUNTIME_HEPMC="$(find_runtime_hepmc)" || fail "No real runtime mixed.hepmc found; set HEPMC_FIXTURE=/path/to/mixed.hepmc"
cp "${RUNTIME_HEPMC}" "${WORKDIR}/input.hepmc"
pass "Extracted real HepMC input: ${RUNTIME_HEPMC}"

setup_cmssw12

run_case "fixed_lumi" "${WORKDIR}/fixed_lumi_GENSIM.root" 7 42 0 2
expect_event "${WORKDIR}/fixed_lumi.events" 1 7 42
expect_event "${WORKDIR}/fixed_lumi.events" 1 7 43
reject_event "${WORKDIR}/fixed_lumi.events" 1 8 43
pass "numberEventsInLuminosityBlock=0 keeps events in the configured lumi"

run_case "auto_lumi" "${WORKDIR}/auto_lumi_GENSIM.root" 7 100 1 2
expect_event "${WORKDIR}/auto_lumi.events" 1 7 100
expect_event "${WORKDIR}/auto_lumi.events" 1 8 101
pass "numberEventsInLuminosityBlock=1 advances lumi after one event"

echo "[OK] Local EDM EventID mock test passed"
