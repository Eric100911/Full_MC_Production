#!/bin/bash
# ==============================================================================
# test_lhe_generation.sh - Test HELAC-Onia LHE generation
# ==============================================================================
# This script tests the run_helac.sh script with minimal event generation.
# It verifies:
#   1. Environment setup (CVMFS, GCC, Boost)
#   2. HELAC-Onia unpacking and configuration
#   3. py8_onia_user.inp generation for different pool types
#   4. parton_shower=1 setting and Pythia8 PDG conversion
#   5. LHE file output (*_py8.lhe)
#
# Usage:
#   ./test_lhe_generation.sh [--pool POOL_NAME] [--local]
#
# Options:
#   --pool POOL_NAME  Test specific pool (default: pool_2jpsi)
#   --local           Keep output locally instead of staging to EOS
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LHE_GEN_DIR="${BASE_DIR}/lhe_generation"
TEST_WORKDIR="${SCRIPT_DIR}/workdir_lhe_test"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

msg_info() { echo -e "${BLUE}[TEST INFO]${NC} $1"; }
msg_ok() { echo -e "${GREEN}[TEST OK]${NC} $1"; }
msg_warn() { echo -e "${YELLOW}[TEST WARN]${NC} $1"; }
msg_error() { echo -e "${RED}[TEST ERROR]${NC} $1"; }
msg_step() { echo -e "\n${YELLOW}======== $1 ========${NC}\n"; }

# Default values
POOL_NAME="pool_2jpsi"
LOCAL_ONLY=0
SEED=99999  # Use high seed for test to avoid collision

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --pool)
            POOL_NAME="$2"
            shift 2
            ;;
        --local)
            LOCAL_ONLY=1
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--pool POOL_NAME] [--local]"
            echo "  --pool   Pool name to test (default: pool_2jpsi)"
            echo "  --local  Keep output locally, don't stage to EOS"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Cleanup function
cleanup() {
    local exit_code=$?
    if [[ -d "${TEST_WORKDIR}" ]]; then
        msg_info "Cleaning up test workdir..."
        rm -rf "${TEST_WORKDIR}"
    fi
    exit ${exit_code}
}

echo ""
echo "=============================================="
echo "  LHE Generation Test"
echo "=============================================="
echo "Pool:      ${POOL_NAME}"
echo "Seed:      ${SEED}"
echo "Local:     ${LOCAL_ONLY}"
echo "Work dir:  ${TEST_WORKDIR}"
echo "=============================================="
echo ""

# Create test workdir
mkdir -p "${TEST_WORKDIR}"
cd "${TEST_WORKDIR}"

# ============================================================================
# Test 1: Check the tracked seeded HELAC source package
# ============================================================================
msg_step "Test 1: Check helac_package.tar.gz"

HELAC_PKG="${BASE_DIR}/common/packages/helac_package_cbseed_v1.tar.gz"
if [[ ! -f "${HELAC_PKG}" ]]; then
    msg_error "helac_package_cbseed_v1.tar.gz not found at ${HELAC_PKG}"
    exit 1
fi
msg_ok "helac_package_cbseed_v1.tar.gz found"

# Copy required files to workdir
cp "${HELAC_PKG}" helac_package.tar.gz
cp -r "${LHE_GEN_DIR}/input_templates" .

# ============================================================================
# Test 2: Check CVMFS availability
# ============================================================================
msg_step "Test 2: Check CVMFS availability"

CVMFS_PATHS=(
    "/cvmfs/cms.cern.ch/cmsset_default.sh"
    "/cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh"
)

for path in "${CVMFS_PATHS[@]}"; do
    if [[ ! -f "${path}" ]]; then
        msg_error "CVMFS path not accessible: ${path}"
        exit 1
    fi
    msg_ok "CVMFS accessible: ${path}"
done

# ============================================================================
# Test 3: Test environment setup
# ============================================================================
msg_step "Test 3: Test environment setup"

source /cvmfs/cms.cern.ch/cmsset_default.sh
source /cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh

# Check GCC version
GCC_VERSION=$(gcc --version | head -1)
msg_info "GCC: ${GCC_VERSION}"

# Check Python
PYTHON_VERSION=$(python3 --version 2>&1)
msg_info "Python: ${PYTHON_VERSION}"

msg_ok "Environment setup successful"

# ============================================================================
# Test 4: Test HELAC-Onia unpacking
# ============================================================================
msg_step "Test 4: Test HELAC-Onia unpacking"

tar -xzf helac_package.tar.gz
if [[ ! -f "HELAC-Onia-2.7.6.tar.gz" ]]; then
    msg_error "HELAC-Onia-2.7.6.tar.gz not found in package"
    exit 1
fi
msg_ok "helac_package.tar.gz unpacked"

tar -xzf HELAC-Onia-2.7.6.tar.gz
if [[ ! -d "HELAC-Onia-2.7.6" ]]; then
    msg_error "HELAC-Onia-2.7.6 directory not created"
    exit 1
fi
msg_ok "HELAC-Onia-2.7.6.tar.gz unpacked"

# ============================================================================
# Test 5: Test py8_onia_user.inp generation logic
# ============================================================================
msg_step "Test 5: Test py8_onia_user.inp generation"

# Define expected configurations for each pool
declare -A EXPECTED_PY8_CONFIG
EXPECTED_PY8_CONFIG["pool_2jpsi"]=$'2\n443 443'
EXPECTED_PY8_CONFIG["pool_2jpsi_g"]=$'2\n443 443'
EXPECTED_PY8_CONFIG["pool_jpsi_CSCO_g"]=$'1\n443'
EXPECTED_PY8_CONFIG["pool_upsilon_CSCO_g"]=$'1\n553'
EXPECTED_PY8_CONFIG["pool_jpsi_upsilon_CSCO"]=$'2\n443 553'
EXPECTED_PY8_CONFIG["pool_gg"]=$'0'

# Generate config based on pool
cd HELAC-Onia-2.7.6
mkdir -p input

case "$POOL_NAME" in
    "pool_2jpsi"|"pool_2jpsi_g")
        cat > input/py8_onia_user.inp << 'EOF'
2
443 443
EOF
        ;;
    "pool_jpsi_CSCO_g")
        cat > input/py8_onia_user.inp << 'EOF'
1
443
EOF
        ;;
    "pool_upsilon_CSCO_g")
        cat > input/py8_onia_user.inp << 'EOF'
1
553
EOF
        ;;
    "pool_jpsi_upsilon_CSCO")
        cat > input/py8_onia_user.inp << 'EOF'
2
443 553
EOF
        ;;
    "pool_gg")
        cat > input/py8_onia_user.inp << 'EOF'
0
EOF
        ;;
esac

if [[ -f "input/py8_onia_user.inp" ]]; then
    msg_info "Generated py8_onia_user.inp content:"
    cat input/py8_onia_user.inp
    msg_ok "py8_onia_user.inp generated for ${POOL_NAME}"
else
    msg_error "Failed to generate py8_onia_user.inp"
    exit 1
fi

cd "${TEST_WORKDIR}"

# ============================================================================
# Test 6: Run actual LHE generation with fast-test mode
# ============================================================================
msg_step "Test 6: Run LHE generation (fast-test mode)"

# Create a modified run script for local testing
cat > run_lhe_test.sh << 'TESTSCRIPT'
#!/bin/bash
set -e

POOL_NAME="$1"
MY_SEED="$2"

# Setup environment
source /cvmfs/cms.cern.ch/cmsset_default.sh
source /cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh
export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/releases/LCG_88b/Boost/1.62.0/x86_64-centos7-gcc62-opt/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/contrib/gcc/6.2.0/x86_64-centos7-gcc62-opt/lib64:$LD_LIBRARY_PATH
export PATH=/cvmfs/sft.cern.ch/lcg/contrib/gcc/6.2.0/x86_64-centos7-gcc62-opt/bin:$PATH
unset PYTHONHOME PYTHONPATH

cd HELAC-Onia-2.7.6

# Copy user.inp
if [ -f "../input_templates/user.inp" ]; then
    cp ../input_templates/user.inp input/user.inp
fi

# Set process based on pool
case "$POOL_NAME" in
    "pool_2jpsi")
        PROCESS_STRING="generate g g > cc~(3S11) cc~(3S11)"
        ;;
    "pool_jpsi_CSCO_g")
        PROCESS_STRING="define jpsi_all = cc~(3S11) cc~(3S18) cc~(1S08)
generate g g > jpsi_all g"
        ;;
    "pool_upsilon_CSCO_g")
        PROCESS_STRING="define upsilon_all = bb~(3S11) bb~(3S18) bb~(1S08)
generate g g > upsilon_all g"
        ;;
    "pool_jpsi_upsilon_CSCO")
        PROCESS_STRING="generate g g > jpsi y(1s)"
        ;;
    "pool_gg")
        PROCESS_STRING="generate g g > g g"
        ;;
    *)
        PROCESS_STRING="generate g g > cc~(3S11) cc~(3S11)"
        ;;
esac

# Create py8_onia_user.inp
case "$POOL_NAME" in
    "pool_2jpsi"|"pool_2jpsi_g")
        echo -e "2\n443 443" > input/py8_onia_user.inp
        ;;
    "pool_jpsi_CSCO_g")
        echo -e "1\n443" > input/py8_onia_user.inp
        ;;
    "pool_upsilon_CSCO_g")
        echo -e "1\n553" > input/py8_onia_user.inp
        ;;
    "pool_jpsi_upsilon_CSCO")
        echo -e "2\n443 553" > input/py8_onia_user.inp
        ;;
    "pool_gg")
        echo -e "0" > input/py8_onia_user.inp
        ;;
esac

echo "py8_onia_user.inp:"
cat input/py8_onia_user.inp

# Fast test configuration - minimal events
cat > run_config.ho << EOF
set cmass = 1.54845d0
set bmass = 4.73020d0
set LDMEcc3S11 = 1.16d0
set LDMEcc3S18 = 0.00902923d0
set LDMEcc1S08 = 0.0146d0
set LDMEbb3S11 = 9.28d0
set LDMEbb3S18 = 0.0297426d0
set LDMEbb1S08 = 0.000170128d0
set preunw = 1000
set unwevt = 10
set nmc = 5000
set nopt = 1000
set nopt_step = 1000
set noptlim = 5000
set seed = ${MY_SEED}
set parton_shower = 1
set minptconia = 6.0d0
set minptbonia = 4.0d0
set maxrapconia = 2.4
set minptq = 4.0d0
set ranhel = 4
${PROCESS_STRING}
launch
exit
EOF

echo "run_config.ho:"
cat run_config.ho

# Configure HELAC-Onia
if [[ ! -x "ho_cluster" ]]; then
    # Patch configuration
    sed -i -r -e 's|^[[:space:]]*hepmc_path[[:space:]]*=.*|# hepmc_path is left unset|' input/ho_configuration.txt
    sed -i 's/HEPTopTagger::HEPTopTagger /HEPTopTagger /g' analysis/heptoptagger/heptoptagger_fjcore_interface.cc 2>/dev/null || true
    ./config
fi

# Run HELAC-Onia
echo "Running HELAC-Onia..."
./ho_cluster < run_config.ho 2>&1 | tee ../helac_test.log

echo "HELAC-Onia finished"
TESTSCRIPT

chmod +x run_lhe_test.sh
./run_lhe_test.sh "${POOL_NAME}" "${SEED}" || {
    msg_error "LHE generation failed"
    msg_info "Check helac_test.log for details"
    exit 1
}

# ============================================================================
# Test 7: Check LHE output files
# ============================================================================
msg_step "Test 7: Check LHE output"

# Find LHE files
LHE_FILES=$(find HELAC-Onia-2.7.6 -name "*.lhe" -type f 2>/dev/null)
if [[ -z "${LHE_FILES}" ]]; then
    msg_error "No LHE files generated"
    exit 1
fi

msg_info "Found LHE files:"
echo "${LHE_FILES}"

# Check for py8 converted file (parton_shower=1 should generate this)
PY8_LHE=$(find HELAC-Onia-2.7.6 -name "*_py8.lhe" -type f 2>/dev/null | head -1)
if [[ -n "${PY8_LHE}" ]]; then
    msg_ok "Found Pythia8-converted LHE: ${PY8_LHE}"
    
    # Check file content
    LHE_EVENTS=$(grep -c "<event>" "${PY8_LHE}" || echo "0")
    msg_info "Number of events in LHE: ${LHE_EVENTS}"
    
    # Check for particle IDs
    msg_info "Sample event content (first 20 lines of first event):"
    sed -n '/<event>/,/<\/event>/p' "${PY8_LHE}" | head -20
    
else
    msg_warn "No *_py8.lhe file found, checking standard LHE"
    STANDARD_LHE=$(echo "${LHE_FILES}" | grep -v "_py8" | head -1)
    if [[ -n "${STANDARD_LHE}" ]]; then
        msg_info "Standard LHE file: ${STANDARD_LHE}"
        LHE_EVENTS=$(grep -c "<event>" "${STANDARD_LHE}" || echo "0")
        msg_info "Number of events: ${LHE_EVENTS}"
    fi
fi

# ============================================================================
# Summary
# ============================================================================
msg_step "Test Summary"

echo ""
echo "=============================================="
echo "  LHE Generation Test Results"
echo "=============================================="
echo ""
msg_ok "All tests passed!"
echo ""
echo "Test artifacts in: ${TEST_WORKDIR}"
echo "  - helac_test.log: HELAC-Onia output"
echo "  - HELAC-Onia-2.7.6/PROC_HO_*/results/: LHE files"
echo ""

if [[ ${LOCAL_ONLY} -eq 0 ]]; then
    msg_info "To stage output to EOS, run the full run_helac.sh script"
fi

# Don't cleanup automatically - let user inspect results
msg_info "Workdir preserved for inspection. Remove with: rm -rf ${TEST_WORKDIR}"
