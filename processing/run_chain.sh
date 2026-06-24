#!/bin/bash
# ==============================================================================
# run_chain.sh - 通用处理链 wrapper
# ==============================================================================
# 在 HTCondor worker 上执行：
#   LHE -> Shower -> Mix -> GEN-SIM -> RAW -> RECO -> MiniAOD -> [可选] Ntuple
#
# 设计目标：
# 1. worker 节点运行时不再依赖 AFS 业务目录。
# 2. shower 模式使用统一枚举，兼容旧的 phi/sps 写法。
# 3. 支持本轮测试以 MiniAOD 为验收终点，同时保留 Ntuple 接口。
# ==============================================================================

set -e

# ==============================================================================
# Cleanup on Exit (trap)
# ==============================================================================
# This ensures intermediate files are always cleaned up, even on failure.
# This is critical to avoid HTCondor trying to transfer huge files (>1GB).
cleanup_on_exit() {
    local exit_code=$?
    if [[ "${CLEANUP}" == "true" ]] && [[ -n "${WORKDIR}" ]]; then
        echo "[INFO] Cleaning up intermediate files on exit (code=${exit_code})..."
        rm -f "${WORKDIR}"/*.hepmc "${WORKDIR}"/*.hepmc.gz 2>/dev/null || true
        rm -f "${WORKDIR}"/*.lhe "${WORKDIR}"/*.lhe.gz 2>/dev/null || true
        rm -f "${WORKDIR}"/output_GENSIM.root 2>/dev/null || true
        rm -f "${WORKDIR}"/output_RAW.root 2>/dev/null || true
        rm -f "${WORKDIR}"/output_RECO.root 2>/dev/null || true
        rm -f "${WORKDIR}"/output_MINIAOD.root 2>/dev/null || true
        rm -f "${WORKDIR}"/output_ntuple.root 2>/dev/null || true
        rm -f "${WORKDIR}"/ntuple_*.root 2>/dev/null || true
        rm -f "${WORKDIR}"/.bash_history 2>/dev/null || true
        rm -f "${WORKDIR}"/.viminfo 2>/dev/null || true
        rm -rf "${WORKDIR}"/CMSSW_14_0_18 2>/dev/null || true
        rm -rf "${WORKDIR}"/CMSSW_15_0_15 2>/dev/null || true
        echo "[INFO] Cleanup done"
    fi
    exit ${exit_code}
}
trap cleanup_on_exit EXIT

# ==============================================================================
# Configuration
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMMON_DIR="${BASE_DIR}/common"
SHOWER_DIR="${SCRIPT_DIR}/pythia_shower"

if [[ -f "${COMMON_DIR}/compression_helpers.sh" ]]; then
    source "${COMMON_DIR}/compression_helpers.sh"
fi
CMSSW_CONFIGS_DIR="${COMMON_DIR}/cmssw_configs"
PACKAGES_DIR="${COMMON_DIR}/packages"

# Fallback when run from a transferred sandbox (run_chain.sh + sibling folders)
if [[ ! -d "${COMMON_DIR}" && -d "${SCRIPT_DIR}/common" ]]; then
    BASE_DIR="${SCRIPT_DIR}"
    COMMON_DIR="${BASE_DIR}/common"
    SHOWER_DIR="${BASE_DIR}/pythia_shower"
    CMSSW_CONFIGS_DIR="${COMMON_DIR}/cmssw_configs"
    PACKAGES_DIR="${COMMON_DIR}/packages"
fi

# CMSSW paths (prefer node CVMFS installs; override via env if needed)
CMSSW_12_BASE="${CMSSW_12_BASE:-/cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw/CMSSW_12_4_14}"
CMSSW_15_BASE="${CMSSW_15_BASE:-/cvmfs/cms.cern.ch/el9_amd64_gcc12/cms/cmssw/CMSSW_15_0_15}"

# T2_CN_Beijing XRootD storage paths
# Canonical form: redirector + LFN (see common/node_config_defaults.json).
# Override TARGET_EOS_BASE to redirect output to a different storage area.
EOS_REDIRECTOR="cceos.ihep.ac.cn"
EOS_HOST="${EOS_REDIRECTOR}"
EOS_XRDFS_TARGET="${EOS_HOST}"
EOS_LFN_BASE="/store/user/chiw/MC_Production_v3"
EOS_BASE="${TARGET_EOS_BASE:-root://${EOS_REDIRECTOR}/${EOS_LFN_BASE}}"
EOS_PATH_BASE="${EOS_LFN_BASE}"
EOS_GENERATED_LHE_BASE="${EOS_BASE}/lhe_pools"
EOS_OUTPUT="${EOS_BASE}/output"
NODE_CONFIG=""

# LHE pools on T2_CN_Beijing storage
declare -A POOL_STORAGE_NAME=()
declare -A EXACT_LHE_POOL_PATH=()

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Avoid leaving shell history files
export HISTFILE=/dev/null
export HISTSIZE=0
export HISTFILESIZE=0

if [[ ! -d "${COMMON_DIR}" ]]; then
    echo "[ERROR] Common directory not found at ${COMMON_DIR}. Ensure common/ is transferred alongside run_chain.sh."
    exit 1
fi

# ==============================================================================
# Utility Functions
# ==============================================================================

msg_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
msg_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
msg_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
msg_error() { echo -e "${RED}[ERROR]${NC} $1"; }
msg_step() { echo -e "\n${YELLOW}========================================${NC}"; echo -e "${YELLOW}  $1${NC}"; echo -e "${YELLOW}========================================${NC}\n"; }

# 高噪声命令统一写入 worker 本地日志，避免 Condor stdout/stderr 过大被 hold。
JOB_LOG_DIR=""
COMMAND_LOG_INDEX=0

sanitize_log_label() {
    local raw_label="$1"
    printf '%s' "${raw_label}" | sed 's/[^A-Za-z0-9._-]/_/g'
}

ensure_job_log_dir() {
    if [[ -n "${JOB_LOG_DIR}" ]]; then
        mkdir -p "${JOB_LOG_DIR}"
        return 0
    fi
    if [[ -z "${WORKDIR:-}" ]]; then
        return 1
    fi
    JOB_LOG_DIR="${WORKDIR}/command_logs"
    mkdir -p "${JOB_LOG_DIR}"
}

show_log_tail() {
    local label="$1"
    local file_path="$2"
    local max_lines="$3"
    if [[ -s "${file_path}" ]]; then
        msg_warn "${label} 摘要 (${file_path}, tail -n ${max_lines})"
        tail -n "${max_lines}" "${file_path}"
    fi
}

worker_tmp_file() {
    local template="$1"
    local tmp_base="${WORKDIR:-${TMPDIR:-/tmp}}"
    if [[ ! -d "${tmp_base}" ]]; then
        tmp_base="${TMPDIR:-/tmp}"
    fi
    mktemp --tmpdir="${tmp_base}" "${template}"
}

run_logged() {
    local label="$1"
    shift

    ensure_job_log_dir || {
        msg_error "无法初始化本地命令日志目录"
        return 1
    }

    COMMAND_LOG_INDEX=$((COMMAND_LOG_INDEX + 1))
    local safe_label=""
    safe_label=$(sanitize_log_label "${label}")
    local log_prefix
    printf -v log_prefix "%s/%03d_%s" "${JOB_LOG_DIR}" "${COMMAND_LOG_INDEX}" "${safe_label}"
    local stdout_log="${log_prefix}.stdout"
    local stderr_log="${log_prefix}.stderr"
    local rc=0
    local stdout_size=0
    local stderr_size=0

    msg_info "执行 ${label}，详细日志写入 ${log_prefix}.[stdout|stderr]"
    if "$@" >"${stdout_log}" 2>"${stderr_log}"; then
        stdout_size=$(wc -c < "${stdout_log}" 2>/dev/null || echo 0)
        stderr_size=$(wc -c < "${stderr_log}" 2>/dev/null || echo 0)
        msg_ok "${label} 完成 (stdout=${stdout_size} B, stderr=${stderr_size} B)"
        return 0
    fi

    rc=$?
    msg_error "${label} 失败 (rc=${rc})"
    show_log_tail "${label} stderr" "${stderr_log}" 80 >&2
    show_log_tail "${label} stdout" "${stdout_log}" 40
    return "${rc}"
}

normalize_shower_mode() {
    case "$1" in
        normal)
            echo "normal"
            ;;
        phi|phi_default|phi_mode1|phi_mpi_off|sps)
            echo "phi_mpi_off"
            ;;
        phi_mode2|phi_mpi_on_gluon|phi_gluon)
            echo "phi_mpi_on_gluon"
            ;;
        *)
            return 1
            ;;
    esac
}

stable_seed() {
    local material="$1"
    python3 - "${material}" <<'PYHELPER'
import hashlib
import sys

material = sys.argv[1].encode("utf-8", errors="replace")
value = int.from_bytes(hashlib.sha256(material).digest()[:4], "big")
print(value % 900000000 + 1)
PYHELPER
}

load_node_config() {
    local config_path="$1"
    [[ -n "${config_path}" ]] || return 0
    if [[ ! -s "${config_path}" ]]; then
        msg_error "Node config JSON not found or empty: ${config_path}"
        return 1
    fi

    local parsed_line kind key value extra
    while IFS=$'\t' read -r kind key value extra; do
        case "${kind}" in
            storage)
                case "${key}" in
                    eos_redirector)
                        EOS_REDIRECTOR="${value}"
                        EOS_HOST="${EOS_REDIRECTOR}"
                        EOS_XRDFS_TARGET="${EOS_HOST}"
                        ;;
                    eos_lfn_base)
                        EOS_LFN_BASE="${value}"
                        EOS_PATH_BASE="${EOS_LFN_BASE}"
                        ;;
                    eos_base)
                        EOS_BASE="${TARGET_EOS_BASE:-${value}}"
                        ;;
                    target_eos_base)
                        if [[ -n "${value}" ]]; then
                            EOS_BASE="${TARGET_EOS_BASE:-${value}}"
                        fi
                        ;;
                    generated_lhe_base)
                        EOS_GENERATED_LHE_BASE="${value}"
                        ;;
                    output_subdir)
                        EOS_OUTPUT="${EOS_BASE}/${value}"
                        ;;
                esac
                ;;
            pool)
                POOL_STORAGE_NAME["${key}"]="${value}"
                EXACT_LHE_POOL_PATH["${key}"]="${extra}"
                ;;
        esac
    done < <(python3 - "${config_path}" <<'PYHELPER'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    cfg = json.load(handle)

storage = cfg.get("storage", {})
if not isinstance(storage, dict):
    raise SystemExit("storage must be an object")

for key in ("eos_redirector", "eos_lfn_base", "eos_base", "target_eos_base", "generated_lhe_base", "output_subdir"):
    value = storage.get(key)
    if value not in (None, ""):
        print(f"storage\t{key}\t{value}\t")

pool_dirs = storage.get("lhe_pool_directories", cfg.get("lhe_pool_directories", {}))
if not isinstance(pool_dirs, dict):
    raise SystemExit("lhe_pool_directories must be an object")
for pool, info in sorted(pool_dirs.items()):
    if not isinstance(info, dict):
        raise SystemExit(f"lhe_pool_directories.{pool} must be an object")
    storage_name = str(info.get("storage_name") or pool)
    exact_path = str(info.get("path") or "").rstrip("/")
    print(f"pool\t{pool}\t{storage_name}\t{exact_path}")
PYHELPER
    )

    EOS_BASE="${TARGET_EOS_BASE:-${EOS_BASE}}"
    EOS_OUTPUT="${EOS_OUTPUT:-${EOS_BASE}/output}"
    if [[ -z "${EOS_GENERATED_LHE_BASE}" ]]; then
        msg_error "Configured generated_lhe_base is empty"
        return 1
    fi
}

# Run XRootD tools from the CMSSW_15 el9 runtime. Processing jobs run in the
# CMSSW el9 Singularity image, and IHEP EOS access needs that runtime plus the
# installed proxy/certificate environment.
run_xrootd_tool() {
    local tool_name="$1"
    shift

    (
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        export SCRAM_ARCH=el9_amd64_gcc12
        if [[ -d "${CMSSW_15_BASE}/src" ]]; then
            cd "${CMSSW_15_BASE}/src"
            eval "$(scramv1 runtime -sh)"
        fi
        if command -v "${tool_name}" >/dev/null 2>&1; then
            "${tool_name}" "$@"
            exit $?
        fi
        msg_error "XRootD tool not found after CMSSW runtime setup: ${tool_name}" >&2
        exit 127
    )
}

run_xrdfs() {
    run_xrootd_tool xrdfs "$@"
}

run_xrdcp() {
    run_xrootd_tool xrdcp "$@"
}

# Convert HELAC 9900xxxx octet codes to Pythia OniaShower 99nqnsnrnLnJ scheme
convert_lhe_octet_codes() {
    local lhe_file="$1"
    local octet_tool="${COMMON_DIR}/octet_pdg.py"
    if [[ ! -f "${lhe_file}" ]]; then
        msg_error "LHE file not found for conversion: ${lhe_file}"
        return 1
    fi
    if [[ ! -f "${octet_tool}" ]]; then
        msg_error "Octet PDG tool not found: ${octet_tool}"
        return 1
    fi
    if ! grep -q "\<9900" "${lhe_file}"; then
        return 0
    fi
    msg_info "Converting color-octet PDG codes in ${lhe_file}..."
    PYTHONIOENCODING=UTF-8 python3 "${octet_tool}" convert-file "${lhe_file}" --in-place >/dev/null
    PYTHONIOENCODING=UTF-8 python3 "${octet_tool}" scan "${lhe_file}" --fail-on-legacy
}

# Create remote directory via XRootD
make_remote_dir() {
    local remote_subpath="$1"
    msg_info "Creating remote directory: ${EOS_PATH_BASE}/${remote_subpath}"
    run_logged "xrdfs_mkdir_${remote_subpath//\//_}" run_xrdfs "${EOS_XRDFS_TARGET}" mkdir -p "${EOS_PATH_BASE}/${remote_subpath}" || {
        msg_error "Failed to create remote directory: ${EOS_PATH_BASE}/${remote_subpath}"
        return 1
    }
    msg_ok "Remote directory ready: ${EOS_PATH_BASE}/${remote_subpath}"
}

# Stage out files via XRootD
stage_out() {
    local local_file="$1"
    local remote_subpath="$2"

    if [[ ! -f "${local_file}" ]]; then
        msg_error "Local file not found: ${local_file}"
        return 1
    fi
    
    local remote_url="${EOS_BASE}/${remote_subpath}"
    msg_info "Staging out: ${local_file} -> ${remote_url}"
    
    run_logged "xrdcp_stageout_$(basename "${local_file}")" run_xrdcp --nopbar --force "${local_file}" "${remote_url}" || {
        msg_error "Failed to stage out ${local_file} to ${remote_url}"
        return 1
    }
    msg_ok "Staged out: ${remote_url}"
}

get_lhe_file() {
    local pool_name="$1"
    local index="$2"
    local files=()
    local file_list_path=""
    local n_files=0
    local file_idx=0

    file_list_path=$(worker_tmp_file "lhe_files_${pool_name}.XXXXXX") || {
        msg_error "Failed to create temporary LHE listing file" >&2
        return 1
    }
    if ! list_lhe_files "${pool_name}" >"${file_list_path}"; then
        rm -f "${file_list_path}"
        return 1
    fi
    mapfile -t files < "${file_list_path}"
    rm -f "${file_list_path}"

    n_files=${#files[@]}
    if [[ ${n_files} -eq 0 ]]; then
        msg_error "No LHE files found in exact configured path for ${pool_name}: ${EXACT_LHE_POOL_PATH[$pool_name]:-<missing>}" >&2
        return 1
    fi

    file_idx=$((index % n_files))
    echo "${files[$file_idx]}"
}

list_lhe_files() {
    local pool_name="$1"
    local exact_path="${EXACT_LHE_POOL_PATH[$pool_name]:-}"
    local file_list=""

    if [[ -z "${exact_path}" ]]; then
        msg_error "No exact LHE pool path configured for pool ${pool_name}" >&2
        return 1
    fi

    if [[ "${exact_path}" == root://* ]]; then
        local host_path="${exact_path#root://}"
        local host="${host_path%%/*}"
        local xrdfs_host="root://${host}"
        local remote_path="${host_path#*/}"
        local xrdfs_output=""
        local xrdfs_error_file=""
        remote_path="/${remote_path#/}"
        xrdfs_error_file=$(worker_tmp_file "xrdfs_lhe_list.XXXXXX.stderr") || {
            msg_error "Failed to create temporary xrdfs stderr file" >&2
            return 1
        }
        if ! xrdfs_output=$(run_xrdfs "${xrdfs_host}" ls "${remote_path}" 2>"${xrdfs_error_file}"); then
            msg_error "Failed to list exact LHE pool path via xrdfs: ${exact_path}" >&2
            show_log_tail "xrdfs ls stderr" "${xrdfs_error_file}" 40 >&2
            rm -f "${xrdfs_error_file}"
            return 1
        fi
        rm -f "${xrdfs_error_file}"
        file_list=$(printf '%s\n' "${xrdfs_output}" | grep -E '\.lhe(\.gz)?$' | sort || true)
    else
        if [[ ! -d "${exact_path}" ]]; then
            msg_error "Local LHE directory not found: ${exact_path}" >&2
            return 1
        fi
        if ! file_list=$(find "${exact_path}" -maxdepth 1 -type f \( -name "*.lhe" -o -name "*.lhe.gz" \) | sort); then
            msg_error "Failed to list LHE files from ${exact_path}" >&2
            return 1
        fi
    fi

    if [[ -z "${file_list}" ]]; then
        return 0
    fi

    if [[ "${exact_path}" == root://* ]]; then
        local host_path="${exact_path#root://}"
        local host="${host_path%%/*}"
        while IFS= read -r line; do
            [[ -n "${line}" ]] || continue
            printf 'root://%s/%s\n' "${host}" "${line}"
        done <<< "${file_list}"
    else
        echo "${file_list}"
    fi
}

# Check if a remote XRootD file exists
check_remote_file() {
    local url="$1"
    if [[ "$url" == root://* ]]; then
        # Extract host and path from XRootD URL
        local host_path="${url#root://}"
        local host="${host_path%%/*}"
        local path="/${host_path#*/}"
        run_xrdfs "${host}" stat "${path}" &>/dev/null
        return $?
    else
        # Local file check
        [[ -f "$url" ]]
        return $?
    fi
}

# Ensure CMSSW_12 project exists in workdir (created on demand from CVMFS release)
ensure_cmssw12_project() {
    local project_dir="${WORKDIR}/CMSSW_12_4_14"
    
    if [[ -d "${project_dir}/src" ]]; then
        echo "${project_dir}"
        return 0
    fi
    
    # Use stderr for info messages to avoid polluting function return value
    msg_info "Creating CMSSW_12_4_14 project from CVMFS..." >&2
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    
    cd "${WORKDIR}"
    run_logged "scram_project_CMSSW_12_4_14" scramv1 project CMSSW CMSSW_12_4_14 >&2 || {
        msg_error "Failed to create CMSSW_12_4_14 project" >&2
        return 1
    }
    cd - > /dev/null
    
    echo "${project_dir}"
}

setup_cmssw12() {
    msg_info "Setting up CMSSW_12_4_14..."
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    # Force el8 architecture for CMSSW_12
    export SCRAM_ARCH=el8_amd64_gcc10
    export SITECONFIG_PATH=/cvmfs/cms.cern.ch/SITECONF/T2_CN_Beijing
    
    # Create project on demand if needed
    local base_path
    base_path=$(ensure_cmssw12_project) || return 1
    export CMSSW_12_BASE="${base_path}"

    cd "${base_path}/src"
    eval $(scramv1 runtime -sh)
    cd - > /dev/null
    msg_ok "CMSSW environment: ${CMSSW_VERSION}"
}

# Ensure CMSSW_15 project exists in workdir (created on demand from CVMFS release)
ensure_cmssw15_project() {
    local project_dir="${WORKDIR}/CMSSW_15_0_15"
    
    if [[ -d "${project_dir}/src" ]]; then
        echo "${project_dir}"
        return 0
    fi
    
    # Use stderr for info messages to avoid polluting function return value
    msg_info "Creating CMSSW_15_0_15 project from CVMFS in el9 container..." >&2
    
    local tmp_script=$(mktemp --suffix=_create_cmssw15.sh)
    cat > "${tmp_script}" << CREATEEOF
#!/bin/bash
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el9_amd64_gcc12
cd "${WORKDIR}"
scramv1 project CMSSW CMSSW_15_0_15
CREATEEOF
    chmod +x "${tmp_script}"
    
    run_el9_script_logged "create_CMSSW_15_0_15" "${tmp_script}" >&2
    
    local rc=$?
    rm -f "${tmp_script}"
    
    if [[ $rc -ne 0 ]]; then
        msg_error "Failed to create CMSSW_15_0_15 project" >&2
        return 1
    fi
    
    echo "${project_dir}"
}

setup_cmssw15() {
    msg_info "Setting up CMSSW_15_0_15..."
    
    # Create project on demand if needed
    local base_path
    base_path=$(ensure_cmssw15_project) || return 1
    export CMSSW_15_BASE="${base_path}"

    # Note: CMSSW_15 is built in el9 container, setup happens in container too
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    # Force el9 architecture for CMSSW_15
    export SCRAM_ARCH=el9_amd64_gcc12
    cd "${base_path}/src"
    eval $(scramv1 runtime -sh)
    cd - > /dev/null
    msg_ok "CMSSW environment: ${CMSSW_VERSION}"
}

# EL9 container path for CMSSW_15
EL9_CONTAINER="/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el9:x86_64"

CMSSW12_CONTAINER="${CMSSW12_CONTAINER:-/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el8:x86_64}"

running_on_el9() {
    [[ -f /etc/os-release ]] && grep -Eq '^(VERSION_ID="?9|PLATFORM_ID="platform:el9)' /etc/os-release
}

run_el9_script_logged() {
    local label="$1"
    local script_path="$2"

    if running_on_el9; then
        run_logged "${label}" /bin/bash "${script_path}"
        return $?
    fi

    run_logged "${label}" apptainer exec \
        --bind /cvmfs:/cvmfs \
        --bind /tmp:/tmp \
        --bind "${WORKDIR}:${WORKDIR}" \
        --env "X509_USER_PROXY=${X509_USER_PROXY:-}" \
        --env "HOME=${HOME}" \
        "${EL9_CONTAINER}" \
        /bin/bash "${script_path}"
}

container_runtime() {
    if command -v apptainer >/dev/null 2>&1; then
        echo apptainer
        return 0
    fi
    if command -v singularity >/dev/null 2>&1; then
        echo singularity
        return 0
    fi
    msg_error "Neither apptainer nor singularity is available on this worker" >&2
    return 1
}

host_needs_cmssw12_container() {
    if [[ -r /etc/os-release ]] && grep -Eq '^VERSION_ID="9(\.[0-9]+)?"$' /etc/os-release; then
        return 0
    fi
    if ! ldconfig -p 2>/dev/null | grep -q "libssl.so.1.1"; then
        return 0
    fi
    return 1
}

run_in_cmssw12_container() {
    local script_content="$1"
    local tmp_script=""
    local scratch_root=""
    local siteconf_overlay=""
    tmp_script=$(mktemp --suffix=_cmssw12_cmd.sh)
    scratch_root=$(dirname "${WORKDIR}")
    siteconf_overlay="${scratch_root}/cms_siteconf_overlay"
    rm -rf "${siteconf_overlay}"
    mkdir -p "${siteconf_overlay}"
    cp -a /cvmfs/cms.cern.ch/SITECONF/T2_CN_Beijing "${siteconf_overlay}/"
    ln -s T2_CN_Beijing "${siteconf_overlay}/local"
    cat > "${tmp_script}" <<'SCRIPT_HEADER'
#!/bin/bash
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc10
SCRIPT_HEADER

    echo "${script_content}" >> "${tmp_script}"
    chmod +x "${tmp_script}"

    UNPACKED_IMAGE="${CMSSW12_CONTAINER}" \
    X509_USER_PROXY="${X509_USER_PROXY:-}" \
    HOME="${HOME}" \
    /cvmfs/cms.cern.ch/common/cmssw-el8 \
        -B "${scratch_root}" \
        -B /tmp \
        -B "${siteconf_overlay}:/cvmfs/cms.cern.ch/SITECONF" \
        --command-to-run "/bin/bash ${tmp_script}"

    local rc=$?
    rm -f "${tmp_script}"
    return ${rc}
}

quote_shell_words() {
    local quoted=""
    printf -v quoted '%q ' "$@"
    printf '%s' "${quoted% }"
}

run_cmssw12_command() {
    local label="$1"
    shift

    if host_needs_cmssw12_container; then
        local cmd_text=""
        cmd_text=$(quote_shell_words "$@")
        msg_info "Running ${label} inside el8 container for CMSSW_12 compatibility..."
        run_logged "${label}" run_in_cmssw12_container "
cd '${CMSSW_12_BASE}/src'
eval \$(scramv1 runtime -sh)
cd - >/dev/null
${cmd_text}
"
        return $?
    fi

    run_logged "${label}" "$@"
}

validate_root_file() {
    local label="$1"
    local file_path="$2"

    if [[ ! -s "${file_path}" ]]; then
        msg_error "${label} output missing or empty: ${file_path}"
        return 1
    fi

    local checker="${WORKDIR}/validate_root_file.py"
    cat > "${checker}" <<'PYCHECK'
import sys

import ROOT

path = sys.argv[1]
root_file = ROOT.TFile.Open(path)
if not root_file:
    print(f"ROOT validation failed: could not open {path}", file=sys.stderr)
    sys.exit(2)
if root_file.IsZombie():
    print(f"ROOT validation failed: zombie file {path}", file=sys.stderr)
    sys.exit(3)
if root_file.TestBit(ROOT.TFile.kRecovered):
    print(f"ROOT validation failed: recovered/incompletely closed file {path}", file=sys.stderr)
    sys.exit(4)
if root_file.GetNkeys() <= 0:
    print(f"ROOT validation failed: no keys in {path}", file=sys.stderr)
    sys.exit(5)
print(f"ROOT validation OK: {path} keys={root_file.GetNkeys()}")
sys.exit(0)
PYCHECK

    run_cmssw12_command "validate_root_${label}" python3 "${checker}" "${file_path}"
}

# Run command inside el9 container using apptainer
run_in_el9_container() {
    local script_content="$1"
    local tmp_script=$(mktemp --suffix=_el9_cmd.sh)
    
    cat > "${tmp_script}" << 'SCRIPT_HEADER'
#!/bin/bash
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el9_amd64_gcc12
SCRIPT_HEADER
    
    echo "${script_content}" >> "${tmp_script}"
    chmod +x "${tmp_script}"
    
    if running_on_el9; then
        /bin/bash "${tmp_script}"
    else
        apptainer exec \
            --bind /cvmfs:/cvmfs \
            --bind /tmp:/tmp \
            --bind "${WORKDIR}:${WORKDIR}" \
            "${EL9_CONTAINER}" \
            /bin/bash "${tmp_script}"
    fi
    
    local rc=$?
    rm -f "${tmp_script}"
    return ${rc}
}

prepare_premix_filelist() {
    local source_list="$1"
    local output_list="${WORKDIR}/premix_input_eoscms.txt"
    local redirector="${PREMIX_REDIRECTOR:-root://eoscms.cern.ch}"

    if [[ ! -f "${source_list}" ]]; then
        msg_error "Premix source filelist not found: ${source_list}"
        return 1
    fi

    msg_info "Preparing premix filelist via ${redirector}..." >&2
    python3 - "${source_list}" "${output_list}" "${redirector}" <<'PYHELPER'
import sys
from pathlib import Path

source = Path(sys.argv[1])
output = Path(sys.argv[2])
redirector = sys.argv[3].rstrip('/')
converted = []
for raw in source.read_text().splitlines():
    line = raw.strip()
    if not line or line.startswith('#'):
        continue
    if '/store/' in line:
        store_path = '/store/' + line.split('/store/', 1)[1]
        converted.append(f"{redirector}//{store_path.lstrip('/')}")
    else:
        converted.append(line)
output.write_text('\n'.join(converted) + '\n')
print(f"wrote {len(converted)} entries to {output}", file=sys.stderr)
PYHELPER
    echo "${output_list}"
}

prepare_cached_premix_filelist() {
    local source_list="$1"
    local output_list="${WORKDIR}/premix_input_localcache.txt"
    local cache_dir="${WORKDIR}/premix_cache"
    local redirector="${PREMIX_CACHE_REDIRECTOR:-${PREMIX_REDIRECTOR:-root://eoscms.cern.ch}}"
    local n_files="${PREMIX_CACHE_FILES:-1}"
    local timeout_seconds="${PREMIX_CACHE_TIMEOUT:-7200s}"
    local retries="${PREMIX_CACHE_RETRIES:-3}"

    if [[ ! -f "${source_list}" ]]; then
        msg_error "Premix source filelist not found: ${source_list}"
        return 1
    fi
    if ! [[ "${n_files}" =~ ^[0-9]+$ ]] || [[ "${n_files}" -le 0 ]]; then
        msg_error "PREMIX_CACHE_FILES must be a positive integer"
        return 1
    fi

    mkdir -p "${cache_dir}"
    python3 - "${source_list}" "${cache_dir}" "${output_list}" "${redirector}" "${n_files}" "${timeout_seconds}" "${retries}" <<'PYHELPER'
import subprocess
import sys
from pathlib import Path

source = Path(sys.argv[1])
cache_dir = Path(sys.argv[2])
output = Path(sys.argv[3])
redirector = sys.argv[4].rstrip('/')
n_files = int(sys.argv[5])
timeout_seconds = sys.argv[6]
retries = int(sys.argv[7])

urls = []
for raw in source.read_text().splitlines():
    line = raw.strip()
    if not line or line.startswith('#'):
        continue
    if '/store/' in line:
        store_path = '/store/' + line.split('/store/', 1)[1]
        urls.append(f"{redirector}//{store_path.lstrip('/')}")
    else:
        urls.append(line)
    if len(urls) >= n_files:
        break

if not urls:
    raise SystemExit("no premix URLs found")

local_files = []
for index, url in enumerate(urls):
    local = cache_dir / f"premix_{index}.root"
    if not local.exists() or local.stat().st_size == 0:
        for attempt in range(1, retries + 1):
            cmd = ["timeout", "--preserve-status", timeout_seconds, "xrdcp", "--nopbar", "-f", url, str(local)]
            proc = subprocess.run(cmd)
            if proc.returncode == 0 and local.exists() and local.stat().st_size > 0:
                break
            if local.exists():
                local.unlink()
        else:
            raise SystemExit(f"failed to cache premix file: {url}")
    local_files.append(f"file:{local}")

output.write_text("\n".join(local_files) + "\n")
print(f"cached {len(local_files)} premix files to {cache_dir}", file=sys.stderr)
PYHELPER
    echo "${output_list}"
}

prepare_local_pool_premix_filelist() {
    local pool_dir="${PREMIX_LOCAL_POOL_DIR:-}"
    local output_list="${WORKDIR}/premix_input_localpool.txt"
    local max_files="${PREMIX_LOCAL_POOL_FILES:-0}"

    if [[ -z "${pool_dir}" ]]; then
        msg_error "PREMIX_LOCAL_POOL_DIR must point to a directory of local premix ROOT files"
        return 1
    fi
    if [[ ! -d "${pool_dir}" ]]; then
        msg_error "Premix local pool directory not found: ${pool_dir}"
        return 1
    fi
    if ! [[ "${max_files}" =~ ^[0-9]+$ ]]; then
        msg_error "PREMIX_LOCAL_POOL_FILES must be a non-negative integer"
        return 1
    fi

    python3 - "${pool_dir}" "${output_list}" "${max_files}" <<'PYHELPER'
import sys
from pathlib import Path

pool_dir = Path(sys.argv[1])
output = Path(sys.argv[2])
max_files = int(sys.argv[3])

files = sorted(
    path for path in pool_dir.glob("*.root")
    if path.is_file() and path.stat().st_size > 0
)
if max_files > 0:
    files = files[:max_files]
if not files:
    raise SystemExit(f"no non-empty ROOT files found in {pool_dir}")
output.write_text("".join(f"file:{path}\n" for path in files))
print(f"wrote {len(files)} local premix files to {output}", file=sys.stderr)
PYHELPER
    echo "${output_list}"
}

run_cmsrun_cmssw15() {
    local cfg="$1"
    shift
    
    msg_info "Running cmsRun in el9 container for CMSSW_15..."
    
    # Build the full command with arguments
    local tmp_script=$(mktemp --suffix=_cmsrun.sh)
    cat > "${tmp_script}" << SCRIPT_EOF
#!/bin/bash
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el9_amd64_gcc12
cd "${CMSSW_15_BASE}/src"
eval \$(scramv1 runtime -sh)
cmsRun "${cfg}" $@
SCRIPT_EOF
    
    chmod +x "${tmp_script}"
    
    run_el9_script_logged "cmsRun_$(basename "${cfg}")" "${tmp_script}"
    
    local rc=$?
    rm -f "${tmp_script}"
    return ${rc}
}

ensure_voms_proxy() {
    msg_info "Checking VOMS proxy for pileup access..."

    # Prefer user-provided proxy if already set
    if [[ -z "${X509_USER_PROXY:-}" ]] && [[ -f "/tmp/x509up_u$(id -u)" ]]; then
        export X509_USER_PROXY="/tmp/x509up_u$(id -u)"
        msg_info "Using default proxy path: ${X509_USER_PROXY}"
    fi

    if command -v voms-proxy-info >/dev/null 2>&1; then
        if voms-proxy-info --exists >/dev/null 2>&1; then
            local tl
            tl=$(voms-proxy-info --timeleft 2>/dev/null || true)
            msg_ok "VOMS proxy valid (timeleft: ${tl}s)"
            return 0
        fi
    else
        msg_warn "voms-proxy-info not found; skipping proxy validation"
        return 0
    fi

    msg_warn "No valid VOMS proxy detected. If DIGI premix download fails, run: voms-proxy-init -voms cms -valid 192:00"
}

prepare_cmssw15_from_package() {
    local analysis="$1"
    local pkg="${PACKAGES_DIR}/tpsonia2mumu_code.tar.gz"

    if [[ ! -d "${PACKAGES_DIR}" ]]; then
        msg_error "Packages directory missing: ${PACKAGES_DIR}"
        return 1
    fi
    if [[ ! -f "${pkg}" ]]; then
        msg_error "Package not found: ${pkg} (ensure common/packages is transferred)"
        return 1
    fi

    local project_dir="${WORKDIR}/CMSSW_15_0_15"
    export CMSSW_15_BASE="${project_dir}"
    
    if [[ ! -d "${project_dir}/src" ]]; then
        msg_info "Creating CMSSW_15_0_15 project in el9 container at ${project_dir}..."

        local tmp_script=$(mktemp --suffix=_create_cmssw15.sh)
        cat > "${tmp_script}" << CREATEEOF
#!/bin/bash
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el9_amd64_gcc12
cd "${WORKDIR}"
scramv1 project CMSSW CMSSW_15_0_15
CREATEEOF
        chmod +x "${tmp_script}"
        
        run_el9_script_logged "create_CMSSW_15_0_15_pkg" "${tmp_script}"
        rm -f "${tmp_script}"
    fi

    local pkg_check_dir="${project_dir}/src/HeavyFlavorAnalysis/TPS-Onia2MuMu"
    
    if [[ ! -d "${pkg_check_dir}" ]]; then
        msg_info "Unpacking ${pkg} into CMSSW src..."
        tar -xzf "${pkg}" -C "${project_dir}/src"
    fi

    local stamp="${project_dir}/.built_${analysis,,}"
    if [[ ! -f "${stamp}" ]]; then
        msg_info "Compiling ntuple code for ${analysis} in el9 container..."
        
        local tmp_script=$(mktemp --suffix=_build_cmssw15.sh)
        cat > "${tmp_script}" << BUILDEOF
#!/bin/bash
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el9_amd64_gcc12
cd "${project_dir}/src"
eval \$(scramv1 runtime -sh)
scram b -j 4 HeavyFlavorAnalysis/TPS-Onia2MuMu
BUILDEOF
        chmod +x "${tmp_script}"
        
        run_el9_script_logged "scram_b_${ANALYSIS_TYPE}" "${tmp_script}"
        rm -f "${tmp_script}"
        
        touch "${stamp}"
    else
        msg_info "Reusing existing CMSSW_15_0_15 build for ${analysis}"
    fi
}

prepare_cmssw15_from_runtime() {
    local pkg="${PACKAGES_DIR}/cmssw15_tpsonia2mumu_runtime.tar.gz"
    local project_dir="${WORKDIR}/CMSSW_15_0_15"

    if [[ ! -f "${pkg}" ]]; then
        return 1
    fi

    export CMSSW_15_BASE="${project_dir}"

    if [[ ! -d "${project_dir}/src" ]]; then
        msg_info "Unpacking prebuilt CMSSW_15_0_15 runtime from ${pkg}..."
        tar -xzf "${pkg}" -C "${WORKDIR}"
    fi

    if [[ ! -d "${project_dir}/src" ]]; then
        msg_error "Prebuilt CMSSW15 runtime did not create ${project_dir}/src"
        return 1
    fi

    if [[ ! -f "${project_dir}/.project_renamed" ]]; then
        local tmp_script=$(mktemp --suffix=_rename_cmssw15.sh)
        cat > "${tmp_script}" << RENAMEEOF
#!/bin/bash
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el9_amd64_gcc12
cd "${project_dir}/src"
eval $(scramv1 runtime -sh)
scram build ProjectRename
RENAMEEOF
        chmod +x "${tmp_script}"
        run_el9_script_logged "scram_ProjectRename_CMSSW_15_0_15" "${tmp_script}" || {
            rm -f "${tmp_script}"
            return 1
        }
        rm -f "${tmp_script}"
        touch "${project_dir}/.project_renamed"
    fi

    msg_info "Using prebuilt CMSSW_15_0_15 runtime"
}

prepare_cmssw15_for_ntuple() {
    if prepare_cmssw15_from_runtime; then
        return 0
    fi

    prepare_cmssw15_from_package "$@"
}

ntuple_cfg_path() {
    case "${ANALYSIS_TYPE}" in
        "JJP") echo "${CMSSW_CONFIGS_DIR}/ntuple_jjp_cfg.py" ;;
        "JUP") echo "${CMSSW_CONFIGS_DIR}/ntuple_jup_cfg.py" ;;
        *) return 1 ;;
    esac
}

# ==============================================================================
# Processing Steps
# ==============================================================================

SHOWER_BUILD_DONE="false"

ensure_worker_shower_tools() {
    if [[ "${SHOWER_BUILD_DONE}" == "true" ]]; then
        return 0
    fi

    local required_tools=(shower_normal shower_phi shower_sps event_mixer_multisource)
    local tool=""
    if host_needs_cmssw12_container; then
        msg_info "Rebuilding shower/mixer tools inside CMSSW_12 el8 container for ABI compatibility..."
        run_in_cmssw12_container \
            "cd \"${CMSSW_12_BASE}/src\" && eval \$(scramv1 runtime -sh) && cd \"${SHOWER_DIR}\" && make -B all" \
            || return 1
        SHOWER_BUILD_DONE="true"
        return 0
    fi

    local can_reuse="true"
    for tool in "${required_tools[@]}"; do
        if [[ ! -x "${tool}" ]] || ! ldd "./${tool}" >/dev/null 2>&1; then
            can_reuse="false"
            break
        fi
    done

    if [[ "${can_reuse}" == "true" ]]; then
        msg_info "Reusing transferred shower/mixer binaries after ldd validation"
        SHOWER_BUILD_DONE="true"
        return 0
    fi

    msg_info "Rebuilding shower/mixer tools inside worker for ABI compatibility..."
    run_logged "build_pythia_shower_tools" make -B all || return 1
    SHOWER_BUILD_DONE="true"
}

# Step 1: Shower LHE files
run_shower() {
    local lhe_files=("$@")
    local n_files=${#lhe_files[@]}
    local n_modes=${#SHOWER_MODES[@]}
    
    msg_step "Step 1: Pythia8 Shower"
    
    if [[ $n_files -ne $n_modes ]]; then
        msg_error "Number of LHE files ($n_files) doesn't match modes ($n_modes)"
        return 1
    fi
    
    local shower_events=-1
    if [[ "${MAX_EVENTS}" -gt 0 ]]; then
        shower_events="${MAX_EVENTS}"
    fi

    HEPMC_FILES=()
    
    setup_cmssw12
    cd "${SHOWER_DIR}"
    ensure_worker_shower_tools || return 1
    
    for ((i=0; i<n_files; i++)); do
        local lhe_file="${lhe_files[$i]}"
        local mode="${SHOWER_MODES[$i]}"
        local normalized_mode=""
        local hepmc_output="${WORKDIR}/shower_${i}.hepmc"
        local local_lhe="${WORKDIR}/input_${i}.lhe"
        
        msg_info "Processing source $((i+1))/${n_files}: ${lhe_file}"
        if ! normalized_mode=$(normalize_shower_mode "${mode}"); then
            msg_error "Unknown shower mode: ${mode}"
            return 1
        fi
        msg_info "Shower mode: ${mode} -> ${normalized_mode}"
        
        # Download remote LHE inputs first; C++ shower tools always receive a
        # plain local .lhe file, never .lhe.gz.
        if [[ "$lhe_file" == root://* ]]; then
            if is_gz_file "${lhe_file}"; then
                local_lhe="${WORKDIR}/input_${i}.lhe.gz"
            fi
            msg_info "Downloading LHE from XRootD..."
            run_logged "xrdcp_input_lhe_${i}" run_xrdcp -f "${lhe_file}" "${local_lhe}"
            if [[ $? -ne 0 ]] || [[ ! -f "${local_lhe}" ]]; then
                msg_error "Failed to download LHE file from ${lhe_file}"
                return 1
            fi
            msg_ok "Downloaded: ${local_lhe}"
            lhe_file="${local_lhe}"
        fi
        if is_gz_file "${lhe_file}"; then
            local plain_lhe="${WORKDIR}/input_${i}.lhe"
            msg_info "Decompressing ${lhe_file} -> ${plain_lhe}"
            gunzip -c "${lhe_file}" > "${plain_lhe}.tmp" \
                && mv "${plain_lhe}.tmp" "${plain_lhe}" \
                || { msg_error "Failed to decompress ${lhe_file}"; return 1; }
            lhe_file="${plain_lhe}"
        fi

        # Convert HELAC 9900xxxx octet codes to OniaShower 99nqnsnrnLnJ scheme
        # Note: With parton_shower=1 in HELAC-Onia, the *_py8.lhe files should
        # already have correct Pythia8 PDG codes. This conversion handles any
        # remaining HELAC-style codes that might be present.
        convert_lhe_octet_codes "${lhe_file}"

        local source_seed
        source_seed=$(stable_seed "${CAMPAIGN_NAME}|${JOB_ID}|source=${i}|${INPUT_SPECS[$i]}|${mode}")
        msg_info "Pythia RNG seed for source $((i+1)): ${source_seed}"
        
        if [[ "$normalized_mode" == "phi_mpi_off" ]]; then
            # workbook_v2 默认模式：关闭 MPI，循环 hadronize 找 phi。
            msg_info "Running phi-enriched mode-1 shower (MPI off)..."
            run_logged "shower_sps_${i}" ./shower_sps "${lhe_file}" "${hepmc_output}" "${shower_events}" 3.0 2.5 2.4 5000 "${source_seed}"
        elif [[ "$normalized_mode" == "phi_mpi_on_gluon" ]]; then
            # 扩展模式：开启 MPI，并交给 shower_phi 处理来源判定。
            msg_info "Running phi-enriched mode-2 shower (MPI on)..."
            run_logged "shower_phi_${i}" ./shower_phi "${lhe_file}" "${hepmc_output}" "${shower_events}" 3.0 2.5 2.4 5000 1 "${source_seed}"
        else
            # 普通 shower。
            run_logged "shower_normal_${i}" ./shower_normal "${lhe_file}" "${hepmc_output}" "${shower_events}" 2.5 2.4 1000 "${source_seed}"
        fi
        
        if [[ ! -f "${hepmc_output}" ]]; then
            msg_error "Shower failed: ${hepmc_output} not created"
            return 1
        fi
        
        HEPMC_FILES+=("${hepmc_output}")
        msg_ok "Shower complete: ${hepmc_output}"
    done
    
    cd "${WORKDIR}"
}

# Step 2: Mix HepMC files
run_mix() {
    msg_step "Step 2: Event Mixing"
    
    local n_sources=${#HEPMC_FILES[@]}
    MIXED_HEPMC="${WORKDIR}/mixed.hepmc"
    
    cd "${SHOWER_DIR}"
    ensure_worker_shower_tools || return 1
    
    if [[ $n_sources -eq 1 ]]; then
        msg_info "Single source - converting to HepMC2 format..."
        run_logged "event_mixer_single" ./event_mixer_multisource "${MIXED_HEPMC}" "${HEPMC_FILES[0]}"
    else
        msg_info "Mixing ${n_sources} sources..."
        if [[ "${SHUFFLE_MIXING}" == "true" ]]; then
            local shuffle_seed
            shuffle_seed=$(stable_seed "${CAMPAIGN_NAME}|${JOB_ID}|shuffle|${INPUTS}|${MODES}")
            msg_info "Shuffle mixing enabled with seed base ${shuffle_seed}"
            run_logged "event_mixer_shuffle" ./event_mixer_multisource \
                "${MIXED_HEPMC}" "${HEPMC_FILES[@]}" \
                --shuffle-sources --shuffle-seed-base "${shuffle_seed}"
        else
            run_logged "event_mixer_multi" ./event_mixer_multisource "${MIXED_HEPMC}" "${HEPMC_FILES[@]}"
        fi
    fi
    
    if [[ ! -f "${MIXED_HEPMC}" ]]; then
        msg_error "Mixing failed: ${MIXED_HEPMC} not created"
        return 1
    fi
    
    msg_ok "Mixing complete: ${MIXED_HEPMC}"
    cd "${WORKDIR}"
}

# Step 3: GEN-SIM
run_gensim() {
    msg_step "Step 3: GEN-SIM"
    
    GENSIM_OUTPUT="${WORKDIR}/output_GENSIM.root"

    # When resuming with --skip-to gensim, ensure MIXED_HEPMC points to the
    # expected mixed file in the workdir.
    MIXED_HEPMC="${MIXED_HEPMC:-${WORKDIR}/mixed.hepmc}"
    if [[ ! -f "${MIXED_HEPMC}" ]]; then
        msg_error "GEN-SIM input missing: ${MIXED_HEPMC}"
        return 1
    fi
    
    setup_cmssw12
    
    msg_info "Running HepMC -> GEN-SIM..."
    run_cmssw12_command "cmsRun_hepmc_to_GENSIM" cmsRun "${CMSSW_CONFIGS_DIR}/hepmc_to_GENSIM.py" \
        inputFiles="file:${MIXED_HEPMC}" \
        outputFile="file:${GENSIM_OUTPUT}" \
        maxEvents=${MAX_EVENTS} \
        nThreads=4
    
    if [[ ! -f "${GENSIM_OUTPUT}" ]]; then
        msg_error "GEN-SIM failed: ${GENSIM_OUTPUT} not created"
        return 1
    fi
    
    msg_ok "GEN-SIM complete: ${GENSIM_OUTPUT}"
}

# Step 4: RAW (DIGI + HLT)
run_raw() {
    msg_step "Step 4: RAW (DIGI + HLT)"
    
    RAW_OUTPUT="${WORKDIR}/output_RAW.root"
    
    setup_cmssw12
    
    local cfg_file=$(mktemp --suffix=_raw_cfg.py)
    local raw_threads="${RAW_THREADS:-1}"
    local raw_streams="${RAW_STREAMS:-1}"
    local raw_watchdog_timeout="${RAW_WATCHDOG_TIMEOUT:-7200s}"
    local raw_watchdog_kill_after="${RAW_WATCHDOG_KILL_AFTER:-300s}"

    local premix_mode="${PREMIX_INPUT_MODE:-eoscms}"
    local premix_input=""
    case "${premix_mode}" in
        dbs)
            premix_input="dbs:/Neutrino_E-10_gun/Run3Summer21PrePremix-Summer22_124X_mcRun3_2022_realistic_v11-v2/PREMIX"
            ;;
        eoscms|filelist)
            local premix_filelist="/cvmfs/cms.cern.ch/offcomp-prod/premixPUlist/PREMIX-Run3Summer22DRPremix.txt"
            local premix_runtime_filelist=""
            premix_runtime_filelist=$(prepare_premix_filelist "${premix_filelist}") || return 1
            premix_input="filelist:${premix_runtime_filelist}"
            ;;
        localcache)
            local premix_filelist="/cvmfs/cms.cern.ch/offcomp-prod/premixPUlist/PREMIX-Run3Summer22DRPremix.txt"
            local premix_runtime_filelist=""
            premix_runtime_filelist=$(prepare_cached_premix_filelist "${premix_filelist}") || return 1
            premix_input="filelist:${premix_runtime_filelist}"
            ;;
        *)
            msg_error "Unknown PREMIX_INPUT_MODE=${premix_mode}; expected eoscms, filelist, localcache or dbs"
            return 1
            ;;
    esac
    
    local raw_cmsdriver_timeout="${RAW_CMSDRIVER_TIMEOUT:-600s}"
    msg_info "Generating RAW config with PREMIX_INPUT_MODE=${premix_mode}..."
    run_logged "cmsDriver_step2_raw" timeout --preserve-status --kill-after=60s "${raw_cmsdriver_timeout}" cmsDriver.py step2 \
        --mc --no_exec \
        --python_filename "${cfg_file}" \
        --eventcontent PREMIXRAW \
        --step DIGI,DATAMIX,L1,DIGI2RAW,HLT:2022v12 \
        --procModifiers premix_stage2,siPixelQualityRawToDigi \
        --datamix PreMix \
        --datatier GEN-SIM-RAW \
        --conditions 124X_mcRun3_2022_realistic_v12 \
        --beamspot Realistic25ns13p6TeVEarly2022Collision \
        --era Run3 \
        --geometry DB:Extended \
        -n "${MAX_EVENTS}" \
        --customise Configuration/DataProcessing/Utils.addMonitoring \
        --nThreads "${raw_threads}" --nStreams "${raw_streams}" \
        --pileup_input "${premix_input}" \
        --filein "file:${GENSIM_OUTPUT}" \
        --fileout "file:${RAW_OUTPUT}"
    
    msg_info "Running RAW step with nThreads=${raw_threads}, nStreams=${raw_streams}, watchdog=${raw_watchdog_timeout}..."
    run_cmssw12_command "cmsRun_step2_raw" timeout --preserve-status --kill-after="${raw_watchdog_kill_after}" "${raw_watchdog_timeout}" cmsRun "${cfg_file}"
    rm -f "${cfg_file}"
    
    if [[ ! -f "${RAW_OUTPUT}" ]]; then
        msg_error "RAW step failed: ${RAW_OUTPUT} not created"
        return 1
    fi
    
    msg_ok "RAW complete: ${RAW_OUTPUT}"
}

# Step 5: RECO
run_reco() {
    msg_step "Step 5: RECO"
    
    RECO_OUTPUT="${WORKDIR}/output_RECO.root"
    
    setup_cmssw12
    
    local cfg_file=$(mktemp --suffix=_reco_cfg.py)
    
    msg_info "Generating RECO config..."
    run_logged "cmsDriver_step3_reco" cmsDriver.py step3 \
        --mc --no_exec \
        --python_filename "${cfg_file}" \
        --eventcontent AODSIM \
        --step RAW2DIGI,L1Reco,RECO,RECOSIM \
        --procModifiers siPixelQualityRawToDigi \
        --datatier AODSIM \
        --conditions 124X_mcRun3_2022_realistic_v12 \
        --beamspot Realistic25ns13p6TeVEarly2022Collision \
        --era Run3 \
        --geometry DB:Extended \
        -n "${MAX_EVENTS}" \
        --customise Configuration/DataProcessing/Utils.addMonitoring \
        --nThreads 4 --nStreams 4 \
        --filein "file:${RAW_OUTPUT}" \
        --fileout "file:${RECO_OUTPUT}"
    
    msg_info "Running RECO step..."
    run_cmssw12_command "cmsRun_step3_reco" cmsRun "${cfg_file}"
    rm -f "${cfg_file}"
    
    if [[ ! -f "${RECO_OUTPUT}" ]]; then
        msg_error "RECO step failed: ${RECO_OUTPUT} not created"
        return 1
    fi
    
    msg_ok "RECO complete: ${RECO_OUTPUT}"
}

# Step 6: MiniAOD
run_miniaod() {
    msg_step "Step 6: MiniAOD"
    
    MINIAOD_OUTPUT="${WORKDIR}/output_MINIAOD.root"
    
    setup_cmssw12
    
    local cfg_file=$(mktemp --suffix=_miniaod_cfg.py)
    
    msg_info "Generating MiniAOD config..."
    run_logged "cmsDriver_step4_miniaod" cmsDriver.py step4 \
        --mc --no_exec \
        --python_filename "${cfg_file}" \
        --eventcontent MINIAODSIM \
        --step PAT \
        --datatier MINIAODSIM \
        --conditions 124X_mcRun3_2022_realistic_v12 \
        --era Run3 \
        --geometry DB:Extended \
        -n "${MAX_EVENTS}" \
        --customise Configuration/DataProcessing/Utils.addMonitoring \
        --nThreads 4 --nStreams 4 \
        --filein "file:${RECO_OUTPUT}" \
        --fileout "file:${MINIAOD_OUTPUT}"
    
    msg_info "Running MiniAOD step..."
    run_cmssw12_command "cmsRun_step4_miniaod" cmsRun "${cfg_file}"
    rm -f "${cfg_file}"
    
    if [[ ! -f "${MINIAOD_OUTPUT}" ]]; then
        msg_error "MiniAOD step failed: ${MINIAOD_OUTPUT} not created"
        return 1
    fi
    
    msg_ok "MiniAOD complete: ${MINIAOD_OUTPUT}"
}

# Step 7: Ntuple
run_ntuple() {
    msg_step "Step 7: Ntuple (${ANALYSIS_TYPE})"
    
    NTUPLE_OUTPUT="${WORKDIR}/output_ntuple.root"
    MINIAOD_OUTPUT="${MINIAOD_OUTPUT:-${WORKDIR}/output_MINIAOD.root}"
    local analysis_mode=""
    case "${ANALYSIS_TYPE}" in
        "JJP") analysis_mode="JpsiJpsiPhi" ;;
        "JUP") analysis_mode="JpsiUpsPhi" ;;
        *)
            msg_error "Unknown analysis type: ${ANALYSIS_TYPE}"
            return 1
            ;;
    esac

    if [[ -n "${MINIAOD_INPUT}" ]]; then
        local downloaded_miniaod="${WORKDIR}/output_MINIAOD.root"
        if [[ "${MINIAOD_INPUT}" == root://* ]]; then
            msg_info "Downloading MiniAOD input for standalone ntuple node..."
            run_logged "xrdcp_miniaod_input" run_xrdcp -f "${MINIAOD_INPUT}" "${downloaded_miniaod}" || return 1
            MINIAOD_OUTPUT="${downloaded_miniaod}"
        elif [[ "${MINIAOD_INPUT}" == file:* ]]; then
            MINIAOD_OUTPUT="${MINIAOD_INPUT#file:}"
        else
            MINIAOD_OUTPUT="${MINIAOD_INPUT}"
        fi
    fi

    if [[ ! -f "${MINIAOD_OUTPUT}" ]]; then
        msg_error "Ntuple input missing: ${MINIAOD_OUTPUT}"
        return 1
    fi

    prepare_cmssw15_for_ntuple "${ANALYSIS_TYPE}" || return 1
    setup_cmssw15

    cfg_path=$(ntuple_cfg_path) || {
        msg_error "No repo-owned ntuple config is available for ${ANALYSIS_TYPE}"
        return 1
    }

    if [[ ! -f "${cfg_path}" ]]; then
        msg_error "Ntuple config missing: ${cfg_path}"
        return 1
    fi

    msg_info "Running ${ANALYSIS_TYPE} Ntuple analysis via ${cfg_path}..."
    run_cmsrun_cmssw15 "${cfg_path}" \
        inputFiles="file:${MINIAOD_OUTPUT}" \
        outputFile="${NTUPLE_OUTPUT}" \
        runOnMC=True \
        maxEvents=-1
    
    if [[ ! -f "${NTUPLE_OUTPUT}" ]]; then
        msg_error "Ntuple step failed: ${NTUPLE_OUTPUT} not created"
        return 1
    fi
    
    msg_ok "Ntuple complete: ${NTUPLE_OUTPUT}"
}

# Step 8: Transfer output
transfer_output() {
    local output_subpath="${CUSTOM_OUTPUT_SUBPATH:-output/${CAMPAIGN_NAME}/${JOB_ID}}"

    if [[ -n "${LOCAL_OUTPUT_BASE:-}" ]]; then
        msg_step "Step 8: Copy outputs to local storage"
        local local_output_dir="${LOCAL_OUTPUT_BASE}/${output_subpath}"
        mkdir -p "${local_output_dir}" || return 1

        if [[ -f "${MINIAOD_OUTPUT}" ]]; then
            local miniaod_basename
            miniaod_basename=$(basename "${MINIAOD_OUTPUT}")
            run_logged "copy_local_miniaod_${JOB_ID}" cp -f "${MINIAOD_OUTPUT}" "${local_output_dir}/${miniaod_basename}" || return 1
        fi

        if [[ -f "${NTUPLE_OUTPUT:-}" ]]; then
            local ntuple_basename
            ntuple_basename=$(basename "${NTUPLE_OUTPUT}")
            run_logged "copy_local_ntuple_${JOB_ID}" cp -f "${NTUPLE_OUTPUT}" "${local_output_dir}/${ntuple_basename}" || return 1

            if [[ "${EFFICIENCY_NTUPLE}" == "true" ]]; then
                local manifest_file="${WORKDIR}/ntuple_manifest_${CAMPAIGN_NAME}_${JOB_ID}.json"
                cat > "${manifest_file}" << MANIFESTEOF
{
  "${CAMPAIGN_NAME}": [
    "${local_output_dir}/${ntuple_basename}"
  ]
}
MANIFESTEOF
                run_logged "copy_local_ntuple_manifest_${JOB_ID}" cp -f "${manifest_file}" "${local_output_dir}/$(basename "${manifest_file}")" || return 1
            fi
        fi

        if [[ "${CLEANUP}" == "true" ]]; then
            msg_info "Cleaning up intermediate files..."
            rm -f "${WORKDIR}"/*.hepmc "${WORKDIR}"/*.hepmc.gz
            rm -f "${WORKDIR}"/output_GENSIM.root
            rm -f "${WORKDIR}"/output_RAW.root
            rm -f "${WORKDIR}"/output_RECO.root
            msg_ok "Cleanup complete"
        fi

        msg_ok "Local copy complete: ${local_output_dir}/"
        return 0
    fi

    msg_step "Step 8: Transfer to T2_CN_Beijing Storage"
    
    # Create remote directory
    make_remote_dir "${output_subpath}" || return 1
    
    # Copy final outputs via XRootD
    if [[ "${TRANSFER_MINIAOD}" == "true" && -f "${MINIAOD_OUTPUT}" ]]; then
        local miniaod_basename=$(basename "${MINIAOD_OUTPUT}")
        stage_out "${MINIAOD_OUTPUT}" "${output_subpath}/${miniaod_basename}" || return 1
    fi
    
    if [[ -f "${NTUPLE_OUTPUT:-}" ]]; then
        local ntuple_basename="${CUSTOM_NTUPLE_BASENAME:-$(basename "${NTUPLE_OUTPUT}")}"
        stage_out "${NTUPLE_OUTPUT}" "${output_subpath}/${ntuple_basename}" || return 1

        if [[ "${EFFICIENCY_NTUPLE}" == "true" ]]; then
            local manifest_file="${WORKDIR}/ntuple_manifest_${CAMPAIGN_NAME}_${JOB_ID}.json"
            cat > "${manifest_file}" << MANIFESTEOF
{
  "${CAMPAIGN_NAME}": [
    "${EOS_BASE}/${output_subpath}/${ntuple_basename}"
  ]
}
MANIFESTEOF
            stage_out "${manifest_file}" "${output_subpath}/$(basename "${manifest_file}")" || return 1
        fi
    fi
    
    # Cleanup intermediate files
    if [[ "${CLEANUP}" == "true" ]]; then
        msg_info "Cleaning up intermediate files..."
        rm -f "${WORKDIR}"/*.hepmc "${WORKDIR}"/*.hepmc.gz
        rm -f "${WORKDIR}"/output_GENSIM.root
        rm -f "${WORKDIR}"/output_RAW.root
        rm -f "${WORKDIR}"/output_RECO.root
        msg_ok "Cleanup complete"
    fi
    
    msg_ok "Transfer complete: ${EOS_BASE}/${output_subpath}/"
}

# ==============================================================================
# Main
# ==============================================================================

usage() {
    cat << EOF
Usage: $0 [options]

Required options:
  --inputs INPUTS       Comma-separated list of pool:index pairs
                        Supports prefixes: GEN:, EOS:
  --modes MODES         逗号分隔的 shower 模式
  --analysis TYPE       Analysis type: JJP or JUP
  --campaign NAME       Campaign name (e.g., JJP_DPS1)
  --job-id ID           Job identifier

Optional:
  --workdir DIR           Working directory (default: /srv/<campaign>_<job_id>)
  --enable-ntuple BOOL    是否执行 ntuple 步骤 (true|false)
  --efficiency-ntuple BOOL 是否生成效率/acceptance full-GEN truth ntuple (需要 --enable-ntuple true)
  --shuffle-mixing BOOL   是否对多输入源启用确定性 shuffle mixing (true|false)
  --cleanup BOOL          是否清理中间文件 (true|false)
  --skip-to STEP          Skip to specified step (shower|mix|gensim|raw|reco|miniaod|ntuple)
  --stop-at STEP          Stop after specified step
  --miniaod-input PATH    Existing MiniAOD input for standalone ntuple nodes
  --transfer-miniaod BOOL Whether transfer step should upload MiniAOD (true|false)
  --max-events N          Limit events for fast local test (default: -1 = all)
  --config PATH           Node JSON config with exact storage paths
  -h, --help              Show this help

Input format examples:
  pool_jpsi_CSCO_g:0          Legacy format (pool:index)
  EOS:pool_jpsi_CSCO_g:0:0    Existing LHE from EOS storage
  GEN:pool_jpsi_CSCO_g:0:1234 LHE generated by DAG，可携带实际 seed

Examples:
  # JJP DPS: Two J/psi sources mixed
  $0 --inputs pool_jpsi_CSCO_g:0,pool_jpsi_CSCO_g:1 --modes normal,phi_mpi_off \\
     --analysis JJP --campaign JJP_DPS1 --job-id 0
EOF
    exit 1
}

# Parse arguments
INPUTS=""
MODES=""
ANALYSIS_TYPE=""
CAMPAIGN_NAME=""
JOB_ID=""
WORKDIR=""
CLEANUP="true"
ENABLE_NTUPLE="true"
EFFICIENCY_NTUPLE="true"
SHUFFLE_MIXING="false"
SKIP_TO=""
STOP_AT=""
MAX_EVENTS=-1
MINIAOD_INPUT=""
TRANSFER_MINIAOD="true"

while [[ $# -gt 0 ]]; do
    case $1 in
        --inputs)
            INPUTS="$2"
            shift 2
            ;;
        --modes)
            MODES="$2"
            shift 2
            ;;
        --analysis)
            ANALYSIS_TYPE="$2"
            shift 2
            ;;
        --campaign)
            CAMPAIGN_NAME="$2"
            shift 2
            ;;
        --job-id)
            JOB_ID="$2"
            shift 2
            ;;
        --workdir)
            WORKDIR="$2"
            shift 2
            ;;
        --enable-ntuple)
            ENABLE_NTUPLE="$2"
            shift 2
            ;;
        --efficiency-ntuple)
            EFFICIENCY_NTUPLE="$2"
            shift 2
            ;;
        --shuffle-mixing)
            SHUFFLE_MIXING="$2"
            shift 2
            ;;
        --cleanup)
            CLEANUP="$2"
            shift 2
            ;;
        --no-cleanup)
            CLEANUP="false"
            shift
            ;;
        --skip-to)
            SKIP_TO="$2"
            shift 2
            ;;
        --stop-at)
            STOP_AT="$2"
            shift 2
            ;;
        --max-events)
            MAX_EVENTS="$2"
            shift 2
            ;;
        --miniaod-input)
            MINIAOD_INPUT="$2"
            shift 2
            ;;
        --transfer-miniaod)
            TRANSFER_MINIAOD="$2"
            shift 2
            ;;
        --config)
            NODE_CONFIG="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            msg_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [[ -z "$INPUTS" ]] || [[ -z "$MODES" ]] || [[ -z "$ANALYSIS_TYPE" ]] || [[ -z "$CAMPAIGN_NAME" ]] || [[ -z "$JOB_ID" ]]; then
    msg_error "Missing required arguments"
    usage
fi

if [[ "${ENABLE_NTUPLE}" != "true" ]] && [[ "${ENABLE_NTUPLE}" != "false" ]]; then
    msg_error "--enable-ntuple must be true or false"
    exit 1
fi

if [[ "${EFFICIENCY_NTUPLE}" != "true" ]] && [[ "${EFFICIENCY_NTUPLE}" != "false" ]]; then
    msg_error "--efficiency-ntuple must be true or false"
    exit 1
fi

if [[ "${EFFICIENCY_NTUPLE}" == "true" ]]; then
    if [[ "${ANALYSIS_TYPE}" != "JJP" ]]; then
        msg_error "--efficiency-ntuple currently supports only JJP/JpsiJpsiPhi"
        exit 1
    fi
fi

if [[ "${SHUFFLE_MIXING}" != "true" ]] && [[ "${SHUFFLE_MIXING}" != "false" ]]; then
    msg_error "--shuffle-mixing must be true or false"
    exit 1
fi

if [[ "${CLEANUP}" != "true" ]] && [[ "${CLEANUP}" != "false" ]]; then
    msg_error "--cleanup must be true or false"
    exit 1
fi

if [[ "${TRANSFER_MINIAOD}" != "true" ]] && [[ "${TRANSFER_MINIAOD}" != "false" ]]; then
    msg_error "--transfer-miniaod must be true or false"
    exit 1
fi

if ! [[ "${MAX_EVENTS}" =~ ^-?[0-9]+$ ]]; then
    msg_error "--max-events must be an integer"
    exit 1
fi

if ! load_node_config "${NODE_CONFIG}"; then
    exit 1
fi

# Default workdir inside the worker scratch to avoid writing to AFS
if [[ -z "${WORKDIR}" ]]; then
    WORKDIR="${_CONDOR_SCRATCH_DIR:-$PWD}/${CAMPAIGN_NAME}_${JOB_ID}"
fi
export HOME="${WORKDIR}"
mkdir -p "${WORKDIR}"

# Parse inputs and modes
IFS=',' read -ra INPUT_SPECS <<< "$INPUTS"
IFS=',' read -ra SHOWER_MODES <<< "$MODES"

# Validate VOMS proxy early (needed for EOS/XRootD listing)
ensure_voms_proxy

LHE_FILES=()
if [[ "${SKIP_TO}" == "ntuple" ]]; then
    msg_info "Skipping LHE input resolution for standalone ntuple node"
else
    # Resolve LHE files from input specs.
    # Supports: file:/path/to.lhe, GEN:pool:idx, EOS:pool:idx:usage, pool:idx.
    declare -a parts  # Declare array outside loop (no 'local' in main script)
    for spec in "${INPUT_SPECS[@]}"; do
        if [[ "$spec" == file:* ]]; then
            # Local file path (from test_full_chain or local runs)
            lhe_file="${spec#file:}"
        elif [[ "$spec" == BLOCK:* ]]; then
            # Format: BLOCK:pool_name:block_namespace:block_idx
            # block_namespace is a group_id for grouped blocks or a seed for legacy blocks.
            IFS=':' read -ra parts <<< "$spec"
            pool_name="${parts[1]}"
            block_namespace="${parts[2]}"
            block_idx="${parts[3]}"
            storage_name="${POOL_STORAGE_NAME[$pool_name]:-$pool_name}"

            # Try grouped directory first, then flat block names within the exact generated pool path.
            lhe_file="${EOS_GENERATED_LHE_BASE}/${storage_name}/lhe_blocks/${block_namespace}/block_${block_namespace}_${block_idx}.lhe.gz"
            if ! check_remote_file "$lhe_file"; then
                lhe_file="${EOS_GENERATED_LHE_BASE}/${storage_name}/lhe_blocks/${block_namespace}/block_${block_namespace}_${block_idx}.lhe"
                if ! check_remote_file "$lhe_file"; then
                    lhe_file="${EOS_GENERATED_LHE_BASE}/${storage_name}/lhe_blocks/block_${block_namespace}_${block_idx}.lhe.gz"
                    if ! check_remote_file "$lhe_file"; then
                        lhe_file="${EOS_GENERATED_LHE_BASE}/${storage_name}/lhe_blocks/block_${block_namespace}_${block_idx}.lhe"
                        if ! check_remote_file "$lhe_file"; then
                            msg_error "Could not resolve LHE block file for: $spec"
                            exit 1
                        fi
                    fi
                fi
            fi
        elif [[ "$spec" == GEN:* ]]; then
            # Format: GEN:pool_name:lhe_job_idx[:seed]
            IFS=':' read -ra parts <<< "$spec"
            pool_name="${parts[1]}"
            lhe_job_idx="${parts[2]}"
            if [[ ${#parts[@]} -ge 4 ]]; then
                seed="${parts[3]}"
            else
                seed=$((100 + lhe_job_idx))
            fi
            # Try .lhe.gz first, then uncompressed .lhe, then fall back to pool listing.
            storage_name="${POOL_STORAGE_NAME[$pool_name]:-$pool_name}"
            lhe_file="${EOS_GENERATED_LHE_BASE}/${storage_name}/sample_${storage_name}_${seed}.lhe.gz"
            if ! check_remote_file "$lhe_file"; then
                lhe_file="${EOS_GENERATED_LHE_BASE}/${storage_name}/sample_${storage_name}_${seed}.lhe"
                if ! check_remote_file "$lhe_file"; then
                    if ! lhe_file=$(get_lhe_file "$pool_name" "$lhe_job_idx"); then
                        msg_error "Could not resolve LHE file for: $spec (pool: ${pool_name}, idx: ${lhe_job_idx})"
                        exit 1
                    fi
                fi
            fi
        elif [[ "$spec" == EOS:* ]]; then
            # Format: EOS:pool_name:job_id:usage_idx - existing LHE from EOS
            IFS=':' read -ra parts <<< "$spec"
            pool_name="${parts[1]}"
            job_id="${parts[2]}"
            if ! lhe_file=$(get_lhe_file "$pool_name" "$job_id"); then
                msg_error "Could not resolve LHE file for: $spec (pool: ${pool_name}, job: ${job_id})"
                exit 1
            fi
        else
            # Legacy format: pool_name:index
            pool_name="${spec%:*}"
            index="${spec#*:}"
            if ! lhe_file=$(get_lhe_file "$pool_name" "$index"); then
                msg_error "Could not resolve LHE file for: $spec (pool: ${pool_name}, idx: ${index})"
                exit 1
            fi
        fi

        if [[ -z "$lhe_file" ]] || ! check_remote_file "$lhe_file"; then
            msg_error "Could not resolve LHE file for: $spec (tried: ${lhe_file:-<none>})"
            exit 1
        fi
        LHE_FILES+=("$lhe_file")
    done
fi

# Print configuration
echo ""
echo "=============================================="
echo "MC Production Chain"
echo "=============================================="
echo "Campaign:     ${CAMPAIGN_NAME}"
echo "Job ID:       ${JOB_ID}"
echo "Analysis:     ${ANALYSIS_TYPE}"
echo "Work dir:     ${WORKDIR}"
echo "Do ntuple:    ${ENABLE_NTUPLE}"
echo "Eff ntuple:   ${EFFICIENCY_NTUPLE}"
echo "N sources:    ${#LHE_FILES[@]}"
for ((i=0; i<${#LHE_FILES[@]}; i++)); do
    mode_str="${SHOWER_MODES[$i]:-N/A}"
    echo "  Source $((i+1)): ${LHE_FILES[$i]} (mode: ${mode_str})"
done
echo "Max events:   ${MAX_EVENTS}"
echo "=============================================="
echo ""

# Create work directory
mkdir -p "${WORKDIR}"
cd "${WORKDIR}"

# Define step order
STEPS=("shower" "mix" "gensim" "raw" "reco" "miniaod")
if [[ "${ENABLE_NTUPLE}" == "true" ]]; then
    STEPS+=("ntuple")
fi
STEPS+=("transfer")

# Determine starting step
start_idx=0
if [[ -n "$SKIP_TO" ]]; then
    found_skip=0
    for ((i=0; i<${#STEPS[@]}; i++)); do
        if [[ "${STEPS[$i]}" == "$SKIP_TO" ]]; then
            start_idx=$i
            found_skip=1
            break
        fi
    done
    if [[ ${found_skip} -eq 0 ]]; then
        msg_error "Unknown --skip-to step: ${SKIP_TO}"
        exit 1
    fi
fi

# Determine ending step
end_idx=$((${#STEPS[@]} - 1))
if [[ -n "$STOP_AT" ]]; then
    found_stop=0
    for ((i=0; i<${#STEPS[@]}; i++)); do
        if [[ "${STEPS[$i]}" == "$STOP_AT" ]]; then
            end_idx=$i
            found_stop=1
            break
        fi
    done
    if [[ ${found_stop} -eq 0 ]]; then
        msg_error "Unknown --stop-at step: ${STOP_AT}"
        exit 1
    fi
fi

# Summarize planned steps (helps debugging skip/stop logic)
SELECTED_STEPS=()
for ((i=start_idx; i<=end_idx; i++)); do
    SELECTED_STEPS+=("${STEPS[$i]}")
done
msg_info "Planned steps: ${SELECTED_STEPS[*]}"

# Run steps using the selected list (avoids index/loop drift)
for step in "${SELECTED_STEPS[@]}"; do
    case "$step" in
        shower)
            run_shower "${LHE_FILES[@]}"
            ;;
        mix)
            run_mix
            ;;
        gensim)
            run_gensim
            ;;
        raw)
            run_raw
            ;;
        reco)
            run_reco
            ;;
        miniaod)
            run_miniaod
            ;;
        ntuple)
            run_ntuple
            ;;
        transfer)
            transfer_output
            ;;
    esac
done

# If mix was requested but output not found, fail early so tests surface the issue.
# Skip this check when cleanup removed intermediates (CLEANUP=true) or after transfer.
if [[ " ${SELECTED_STEPS[*]} " == *" mix "* ]] && [[ " ${SELECTED_STEPS[*]} " != *" transfer "* ]] && [[ "${CLEANUP}" != "true" ]]; then
    if [[ ! -f "${MIXED_HEPMC:-}" ]]; then
        msg_error "Expected mixed HepMC at ${MIXED_HEPMC:-<unset>} but not found"
        exit 1
    fi
fi

msg_step "Production Complete!"
echo "Campaign:  ${CAMPAIGN_NAME}"
echo "Job ID:    ${JOB_ID}"
if [[ -n "${LOCAL_OUTPUT_BASE:-}" ]]; then
    echo "Output:    ${LOCAL_OUTPUT_BASE}/output/${CAMPAIGN_NAME}/${JOB_ID}/"
else
    echo "Output:    ${EOS_OUTPUT}/${CAMPAIGN_NAME}/${JOB_ID}/"
fi
echo ""
