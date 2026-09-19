#!/bin/bash
# ==============================================================================
# test_helac_remote_url.sh — Worker-side XRootD URL reconstruction regression
# ==============================================================================
# Regression test 6 from docs/lhe_output_location_and_crystalball_nmc_2026-09-18.md
# ("The worker's final xrdcp URL retains the endpoint and triple slash").
#
# Why this test exists
# --------------------
# lhe_generation/run_helac.sh reconstructs the final `xrdcp` target from a
# directory spec plus a file name.  Before the fix it joined host and absolute
# path with a single `/`, collapsing the canonical IHEP form
#   root://cceos.ihep.ac.cn:1094///store/user/chiw/...
# down to
#   root://cceos.ihep.ac.cn:1094//store/user/chiw/...
# That violates the repository XRootD contract (CLAUDE.md, "Production
# Operations") on the very path that stages out generated LHE.
#
# How it works
# ------------
# run_helac.sh has top-level side effects (it sources compression_helpers.sh,
# parses argv, and then runs HELAC-Onia), so it cannot be sourced directly.
# This test extracts the pure helper functions
#   normalize_remote_path, join_remote_spec, resolve_remote_target,
#   remote_url_for_spec
# between their function boundaries into a scratch file under /tmp/chiw/, sets
# the storage environment variables the script would set, sources the scratch
# copy, and exercises the helpers.  No HELAC build, no DAG, no network.
#
# Detecting the defect
# --------------------
# Set HELAC_SH_SRC to a copy of the script to test that copy instead of the
# repository one, e.g. the pre-fix revision:
#   git show HEAD:lhe_generation/run_helac.sh > /tmp/chiw/run_helac_prefix.sh
#   HELAC_SH_SRC=/tmp/chiw/run_helac_prefix.sh bash tests/test_helac_remote_url.sh
# The test must FAIL on the pre-fix revision and PASS on the fixed one.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
HELAC_SH="${HELAC_SH_SRC:-${BASE_DIR}/lhe_generation/run_helac.sh}"

TMP_ROOT="/tmp/chiw"
mkdir -p "${TMP_ROOT}"
TMP_DIR="$(mktemp -d "${TMP_ROOT}/helac_remote_url_XXXXX")"
EXTRACTED="${TMP_DIR}/run_helac_remote_url_helpers.sh"

PASS=0
FAIL=0

cleanup() {
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

msg_pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
msg_fail() { echo -e "${RED}[FAIL]${NC} $1"; }
msg_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

check_pass() {
    PASS=$((PASS + 1))
    msg_pass "$1"
}
check_fail() {
    FAIL=$((FAIL + 1))
    msg_fail "$1"
}

check_eq() {
    # check_eq <label> <expected> <actual>
    local label="$1" expected="$2" actual="$3"
    if [[ "${actual}" == "${expected}" ]]; then
        check_pass "${label}"
    else
        check_fail "${label}: expected [${expected}] but got [${actual}]"
    fi
}

check_contains() {
    # check_contains <label> <needle> <haystack>
    local label="$1" needle="$2" haystack="$3"
    if [[ "${haystack}" == *"${needle}"* ]]; then
        check_pass "${label}"
    else
        check_fail "${label}: [${haystack}] does not contain [${needle}]"
    fi
}

# ---------------------------------------------------------------------------
# URL shape validator
# ---------------------------------------------------------------------------
# Canonical form required by the repository:
#   root://<host>[:<port>]///<lfn-without-leading-slash>
# i.e. exactly three slashes after the authority, and never a collapsed `//`
# inside the path body.  Scheme-level `//` is removed before the check so it is
# not mistaken for a collapsed separator.
count_leading_slashes() {
    local s="$1" n=0
    while [[ "${s:${n}:1}" == "/" ]]; do
        n=$((n + 1))
    done
    printf '%s' "${n}"
}

url_shape_error() {
    # Prints a reason string when <url> is malformed, nothing when it is valid.
    local url="$1"
    local rest="" host="" tail="" slashes="" body=""

    if [[ "${url}" != root://* ]]; then
        printf 'missing root:// scheme'
        return 0
    fi

    rest="${url#root://}"
    host="${rest%%/*}"
    if [[ -z "${host}" || "${host}" == "${rest}" ]]; then
        printf 'missing authority before the first path separator'
        return 0
    fi

    tail="${rest#"${host}"}"
    slashes=$(count_leading_slashes "${tail}")
    if [[ "${slashes}" -ne 3 ]]; then
        printf 'expected exactly /// after %s, found %s slash(es)' "${host}" "${slashes}"
        return 0
    fi

    body="${tail:3}"
    if [[ "${body}" == *"//"* ]]; then
        printf 'collapsed double slash inside path body [%s]' "${body}"
        return 0
    fi
    return 0
}

check_url_shape() {
    # check_url_shape <label> <url>
    local label="$1" url="$2" reason=""
    reason=$(url_shape_error "${url}")
    if [[ -z "${reason}" ]]; then
        check_pass "${label}: ${url}"
    else
        check_fail "${label}: ${reason} in [${url}]"
    fi
}

# ---------------------------------------------------------------------------
# Extract the pure helpers from run_helac.sh (top-level side effects avoided)
# ---------------------------------------------------------------------------
msg_info "Source script: ${HELAC_SH}"
if [[ ! -s "${HELAC_SH}" ]]; then
    echo "[ERROR] run_helac.sh source not found or empty: ${HELAC_SH}" >&2
    exit 1
fi

HELPER_FUNCS=(
    normalize_remote_path
    join_remote_spec
    resolve_remote_target
    remote_url_for_spec
)

extract_functions() {
    # extract_functions <source> <name>...
    local src="$1"
    shift
    local names="$*"
    awk -v names="${names}" '
        BEGIN {
            n = split(names, arr, " ")
            for (i = 1; i <= n; i++) { want[arr[i]] = 1 }
            printing = 0
        }
        /^[A-Za-z_][A-Za-z0-9_]*\(\) \{/ {
            fname = $0
            sub(/\(\).*/, "", fname)
            if (fname in want) { printing = 1 }
        }
        printing { print }
        printing && /^\}$/ { printing = 0 }
    ' "${src}"
}

{
    echo "#!/bin/bash"
    echo "# Auto-extracted pure helpers from ${HELAC_SH}"
    echo "# Generated by tests/test_helac_remote_url.sh; do not edit."
    extract_functions "${HELAC_SH}" "${HELPER_FUNCS[@]}"
} > "${EXTRACTED}"

# Storage environment the worker script would set (run_helac.sh lines 82-89),
# without a TARGET_EOS_BASE override.
EOS_HOST="cceos.ihep.ac.cn"
EOS_XRDFS_TARGET="root://${EOS_HOST}"
EOS_PATH_BASE="/store/user/chiw/MC_Production_v3"

# shellcheck disable=SC1090
source "${EXTRACTED}"

missing=""
for fn in "${HELPER_FUNCS[@]}"; do
    if ! declare -F "${fn}" >/dev/null; then
        missing="${missing} ${fn}"
    fi
done
if [[ -n "${missing}" ]]; then
    check_fail "extracted helper functions from ${HELAC_SH}; missing:${missing}"
    echo ""
    echo "========================================"
    echo -e "Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
    echo "========================================"
    exit 1
fi
check_pass "extracted helpers from run_helac.sh: ${HELPER_FUNCS[*]}"

# ---------------------------------------------------------------------------
# Test 1: full remote URL round-trips unchanged, with a single-slash LFN
# ---------------------------------------------------------------------------
INVENTORY_URL="root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO"
INVENTORY_LFN="/store/user/chiw/MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO"
INVENTORY_HOST="root://cceos.ihep.ac.cn:1094"

IFS=$'\t' read -r t1_host t1_path t1_url < <(resolve_remote_target "${INVENTORY_URL}")

check_eq "Test 1: url round-trips the exact inventory URL" "${INVENTORY_URL}" "${t1_url}"
check_eq "Test 1: path is the LFN with one leading slash" "${INVENTORY_LFN}" "${t1_path}"
check_eq "Test 1: host keeps redirector and port" "${INVENTORY_HOST}" "${t1_host}"
check_url_shape "Test 1: inventory URL shape" "${t1_url}"

# ---------------------------------------------------------------------------
# Test 2: /store/<lfn> branch builds the canonical triple-slash URL
# ---------------------------------------------------------------------------
IFS=$'\t' read -r t2_host t2_path t2_url < <(resolve_remote_target "${INVENTORY_LFN}")

check_eq "Test 2: /store/ url uses EOS_HOST with triple slash" \
    "root://cceos.ihep.ac.cn///store/user/chiw/MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO" "${t2_url}"
check_eq "Test 2: /store/ host is the configured EOS host" "root://cceos.ihep.ac.cn" "${t2_host}"
check_eq "Test 2: /store/ path is unchanged" "${INVENTORY_LFN}" "${t2_path}"
check_url_shape "Test 2: /store/ URL shape" "${t2_url}"

# ---------------------------------------------------------------------------
# Test 3: /eos/ branch maps to eosuser.cern.ch with a triple slash
# ---------------------------------------------------------------------------
EOS_LFN="/eos/user/c/chiw/MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO"
IFS=$'\t' read -r t3_host t3_path t3_url < <(resolve_remote_target "${EOS_LFN}")

check_eq "Test 3: /eos/ url uses eosuser.cern.ch with triple slash" \
    "root://eosuser.cern.ch///eos/user/c/chiw/MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO" "${t3_url}"
check_eq "Test 3: /eos/ host is eosuser.cern.ch" "root://eosuser.cern.ch" "${t3_host}"
check_eq "Test 3: /eos/ path is unchanged" "${EOS_LFN}" "${t3_path}"
check_url_shape "Test 3: /eos/ URL shape" "${t3_url}"

# ---------------------------------------------------------------------------
# Test 4: relative/default branch uses EOS_XRDFS_TARGET and EOS_PATH_BASE
# ---------------------------------------------------------------------------
GENERATED_SPEC="lhe_pools/SPS-JpsiJpsi-LO"
IFS=$'\t' read -r t4_host t4_path t4_url < <(resolve_remote_target "${GENERATED_SPEC}")

check_eq "Test 4: relative spec url built from EOS_XRDFS_TARGET/EOS_PATH_BASE" \
    "root://cceos.ihep.ac.cn///store/user/chiw/MC_Production_v3/lhe_pools/SPS-JpsiJpsi-LO" "${t4_url}"
check_eq "Test 4: relative spec host is EOS_XRDFS_TARGET" "${EOS_XRDFS_TARGET}" "${t4_host}"
check_eq "Test 4: relative spec path joins EOS_PATH_BASE" \
    "/store/user/chiw/MC_Production_v3/lhe_pools/SPS-JpsiJpsi-LO" "${t4_path}"
check_url_shape "Test 4: relative/default URL shape" "${t4_url}"

# ---------------------------------------------------------------------------
# Test 5: the real stageout join (run_helac.sh:1622) keeps the triple slash
# ---------------------------------------------------------------------------
GEN_DIR_URL="root://cceos.ihep.ac.cn:1094///store/user/chiw/v4_5_full/lhe_pools/SPS-JpsiJpsi-LO"
LHE_FILENAME="sample_SPS-JpsiJpsi-LO_60100.lhe.gz"
LHE_RELSPEC="$(join_remote_spec "${GEN_DIR_URL}" "${LHE_FILENAME}")"
LHE_STAGEOUT_URL="$(remote_url_for_spec "${LHE_RELSPEC}")"

check_eq "Test 5: joined directory-plus-filename spec" \
    "${GEN_DIR_URL}/${LHE_FILENAME}" "${LHE_RELSPEC}"
check_eq "Test 5: stageout url equals directory url plus filename" \
    "root://cceos.ihep.ac.cn:1094///store/user/chiw/v4_5_full/lhe_pools/SPS-JpsiJpsi-LO/${LHE_FILENAME}" \
    "${LHE_STAGEOUT_URL}"
check_contains "Test 5: stageout url keeps the endpoint and port" \
    "root://cceos.ihep.ac.cn:1094///" "${LHE_STAGEOUT_URL}"
check_contains "Test 5: stageout url keeps the lfn body" \
    "/store/user/chiw/v4_5_full/lhe_pools/SPS-JpsiJpsi-LO/${LHE_FILENAME}" "${LHE_STAGEOUT_URL}"
check_url_shape "Test 5: stageout URL shape" "${LHE_STAGEOUT_URL}"

# Relative-directory variant of the same stageout join.
LHE_RELSPEC_2="$(join_remote_spec "${GENERATED_SPEC}" "${LHE_FILENAME}")"
LHE_STAGEOUT_URL_2="$(remote_url_for_spec "${LHE_RELSPEC_2}")"
check_eq "Test 5: relative-directory stageout url" \
    "root://cceos.ihep.ac.cn///store/user/chiw/MC_Production_v3/lhe_pools/SPS-JpsiJpsi-LO/${LHE_FILENAME}" \
    "${LHE_STAGEOUT_URL_2}"
check_url_shape "Test 5: relative-directory stageout URL shape" "${LHE_STAGEOUT_URL_2}"

# Block stageout join (run_helac.sh:1638) uses the same helper on a subdir.
BLOCK_DIR_URL="root://cceos.ihep.ac.cn:1094///store/user/chiw/v4_5_full/lhe_pools/SPS-JpsiJpsi-LO/lhe_blocks"
BLOCK_FILENAME="60100_block_000001.lhe.gz"
BLOCK_URL="$(remote_url_for_spec "$(join_remote_spec "${BLOCK_DIR_URL}" "${BLOCK_FILENAME}")")"
check_eq "Test 5: block stageout url" \
    "${BLOCK_DIR_URL}/${BLOCK_FILENAME}" "${BLOCK_URL}"
check_url_shape "Test 5: block stageout URL shape" "${BLOCK_URL}"

# ---------------------------------------------------------------------------
# Test 6: no produced URL ever collapses the separator to a double slash
# ---------------------------------------------------------------------------
ALL_URLS=("${t1_url}" "${t2_url}" "${t3_url}" "${t4_url}" "${LHE_STAGEOUT_URL}" "${LHE_STAGEOUT_URL_2}" "${BLOCK_URL}")
COLLAPSED=""
for candidate in "${ALL_URLS[@]}"; do
    # Look only at the part after the authority: a `//` there is the defect.
    rest="${candidate#root://}"
    authority="${rest%%/*}"
    tail="${rest#"${authority}"}"
    if [[ "${tail}" == "//"* && "${tail}" != "///"* ]]; then
        COLLAPSED="${candidate}"
        break
    fi
done
if [[ -z "${COLLAPSED}" ]]; then
    check_pass "Test 6: no URL has a collapsed double slash before the LFN"
else
    check_fail "Test 6: collapsed double slash in [${COLLAPSED}]"
fi

# Same guard expressed over the raw text of every URL: `//` may only ever
# appear as part of the leading `///` separator or the `root://` scheme.
DOUBLE_SLASH_URL=""
for candidate in "${ALL_URLS[@]}"; do
    body="${candidate#root://}"
    authority="${body%%/*}"
    path_part="${body#"${authority}"}"
    if [[ "${path_part}" == "///"* ]]; then
        path_part="/${path_part#///}"
    fi
    if [[ "${path_part}" == *"//"* ]]; then
        DOUBLE_SLASH_URL="${candidate}"
        break
    fi
done
if [[ -z "${DOUBLE_SLASH_URL}" ]]; then
    check_pass "Test 6: no URL contains a doubled slash inside its path"
else
    check_fail "Test 6: doubled slash inside path of [${DOUBLE_SLASH_URL}]"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "========================================"
echo -e "Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo "========================================"

if [[ "${FAIL}" -gt 0 ]]; then
    exit 1
fi
exit 0
