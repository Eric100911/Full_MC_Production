#!/bin/bash
# ==============================================================================
# setup.sh - MC 生产公共环境脚本
# ==============================================================================
# 保留给本地调试或单脚本测试使用；DAG 正式运行时以 worker 脚本内部逻辑为准。
# ==============================================================================

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Base paths
export MC_PRODUCTION_BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export COMMON_DIR="${MC_PRODUCTION_BASE}/common"
export PACKAGES_DIR="${COMMON_DIR}/packages"
export CMSSW_CONFIGS_DIR="${COMMON_DIR}/cmssw_configs"

# CMSSW 安装默认走 CVMFS，本地若有其他路径可在外部覆盖环境变量
export CMSSW_12_BASE="${CMSSW_12_BASE:-/cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw/CMSSW_12_4_14}"
export CMSSW_15_BASE="${CMSSW_15_BASE:-/cvmfs/cms.cern.ch/el9_amd64_gcc12/cms/cmssw/CMSSW_15_0_15}"

# T2_CN_Beijing XRootD storage paths
export EOS_HOST="cceos.ihep.ac.cn"
export EOS_XRDFS_TARGET="${EOS_HOST}"
export EOS_PATH_BASE="/eos/ihep/cms/store/user/xcheng/MC_Production_v3"
export EOS_BASE="root://${EOS_HOST}/${EOS_PATH_BASE}"
export EOS_LHE_POOL="${EOS_BASE}/lhe_pools"
export EOS_OUTPUT="${EOS_BASE}/output"

# workbook_v2 版本默认以远端 LHE pool 动态扫描为准，这里只保留基础路径。
export EXISTING_LHE_JPSI_G="${EOS_BASE}/lhe_pools/pool_jpsi_CSCO_g"
export EXISTING_LHE_GG="${EOS_BASE}/lhe_pools/pool_gg"

# Function to print colored messages
msg_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

msg_ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

msg_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

msg_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to setup CMS base environment
setup_cms_base() {
    if [ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        msg_ok "CMS environment loaded from CVMFS"
    else
        msg_error "Cannot find CMS environment on CVMFS"
        return 1
    fi
}

# Function to setup CMSSW 12 environment (for GEN-SIM)
setup_cmssw12() {
    msg_info "Setting up CMSSW_12_4_14 environment..."
    
    if [ ! -d "${CMSSW_12_BASE}/src" ]; then
        msg_error "CMSSW_12_4_14 not found at ${CMSSW_12_BASE}"
        return 1
    fi
    
    setup_cms_base
    
    cd "${CMSSW_12_BASE}/src"
    eval $(scramv1 runtime -sh)
    cd - > /dev/null
    
    export CMSSW_ACTIVE="${CMSSW_12_BASE}"
    msg_ok "CMSSW_12_4_14 environment ready (${CMSSW_VERSION})"
}

# Function to setup CMSSW 15 environment (for Ntuple)
setup_cmssw15() {
    msg_info "Setting up CMSSW_15_0_15 environment..."
    
    if [ ! -d "${CMSSW_15_BASE}/src" ]; then
        msg_error "CMSSW_15_0_15 not found at ${CMSSW_15_BASE}"
        return 1
    fi
    
    setup_cms_base
    
    export SCRAM_ARCH=el9_amd64_gcc12
    cd "${CMSSW_15_BASE}/src"
    eval $(scramv1 runtime -sh)
    cd - > /dev/null
    
    export CMSSW_ACTIVE="${CMSSW_15_BASE}"
    msg_ok "CMSSW_15_0_15 environment ready (${CMSSW_VERSION})"
}

# Function to setup HELAC-Onia environment (requires cmssw-el7 container)
setup_helac() {
    msg_info "Setting up HELAC-Onia environment..."
    
    # HELAC-Onia requires older environment
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    source /cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh
    
    export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/releases/LCG_88b/Boost/1.62.0/x86_64-centos7-gcc62-opt/lib:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/contrib/gcc/6.2.0/x86_64-centos7-gcc62-opt/lib64:$LD_LIBRARY_PATH
    
    msg_ok "HELAC-Onia environment ready"
}

# Function to setup Pythia8 with HepMC3 for shower
setup_pythia_shower() {
    msg_info "Setting up Pythia8 + HepMC environment..."
    
    setup_cmssw12
    
    # Additional paths for HepMC
    if [ -n "$HEPMC_DIR" ]; then
        export LD_LIBRARY_PATH=${HEPMC_DIR}/lib:${HEPMC_DIR}/lib64:${LD_LIBRARY_PATH}
    fi
    
    msg_ok "Pythia8 shower environment ready"
}

# Function to ensure EOS directories exist (XRootD)
ensure_eos_dirs() {
    msg_info "Ensuring EOS output directories exist on T2_CN_Beijing..."
    
    for subdir in "lhe_pools" "output"; do
        xrdfs "${EOS_XRDFS_TARGET}" mkdir -p "${EOS_PATH_BASE}/${subdir}" 2>/dev/null || \
            msg_warn "Cannot create ${EOS_PATH_BASE}/${subdir} (may already exist or permission issue)"
    done
}

# Function to create remote directory via XRootD
make_remote_dir() {
    local remote_path="$1"
    msg_info "Creating remote directory: ${remote_path}"
    xrdfs "${EOS_XRDFS_TARGET}" mkdir -p "${EOS_PATH_BASE}/${remote_path}" || {
        msg_error "Failed to create remote directory: ${EOS_PATH_BASE}/${remote_path}"
        return 1
    }
    msg_ok "Remote directory ready: ${EOS_PATH_BASE}/${remote_path}"
}

# Function to stage out files via XRootD
stage_out() {
    local local_file="$1"
    local remote_subpath="$2"
    
    if [[ ! -f "${local_file}" ]]; then
        msg_error "Local file not found: ${local_file}"
        return 1
    fi
    
    local remote_url="${EOS_BASE}/${remote_subpath}"
    msg_info "Staging out: ${local_file} -> ${remote_url}"
    
    xrdcp --nopbar --force "${local_file}" "${remote_url}" || {
        msg_error "Failed to stage out ${local_file} to ${remote_url}"
        return 1
    }
    msg_ok "Staged out: ${remote_url}"
}

# Function to get LHE file from pool (XRootD listing)
get_lhe_file() {
    local pool_name="$1"
    local index="$2"
    local pool_subpath="lhe_pools/${pool_name}"
    
    # List files via xrdfs
    local file_list
    file_list=$(xrdfs "${EOS_XRDFS_TARGET}" ls "${EOS_PATH_BASE}/${pool_subpath}" 2>/dev/null | grep -E '\.lhe(\.gz)?$' | sort)
    
    if [[ -z "${file_list}" ]]; then
        msg_error "No LHE files found in ${EOS_PATH_BASE}/${pool_subpath}"
        return 1
    fi
    
    # Convert to array
    local files=()
    while IFS= read -r line; do
        files+=("${line}")
    done <<< "${file_list}"
    
    local n_files=${#files[@]}
    
    if [[ $n_files -eq 0 ]]; then
        msg_error "No LHE files found in ${pool_subpath}"
        return 1
    fi
    
    # Wrap around if index exceeds available files
    local file_idx=$((index % n_files))
    local selected_path="${files[$file_idx]}"
    
    # Return full XRootD URL
    echo "root://${EOS_HOST}/${selected_path}"
}

# Parse command line arguments
if [ "$1" = "--cmssw12" ]; then
    setup_cmssw12
elif [ "$1" = "--cmssw14" ]; then
    msg_warn "--cmssw14 is deprecated; using CMSSW_15_0_15 for ntuple debugging"
    setup_cmssw15
elif [ "$1" = "--cmssw15" ]; then
    setup_cmssw15
elif [ "$1" = "--helac" ]; then
    setup_helac
elif [ "$1" = "--shower" ]; then
    setup_pythia_shower
elif [ "$1" = "--ensure-eos" ]; then
    ensure_eos_dirs
fi

# Export functions for use in scripts
export -f msg_info msg_ok msg_warn msg_error
export -f setup_cms_base setup_cmssw12 setup_cmssw15 setup_helac
export -f setup_pythia_shower ensure_eos_dirs get_lhe_file
export -f make_remote_dir stage_out

msg_info "MC Production environment variables set"
msg_info "Base directory: ${MC_PRODUCTION_BASE}"
msg_info "EOS storage: ${EOS_BASE}"
