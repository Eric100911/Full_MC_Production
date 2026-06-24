#!/bin/bash
# ==============================================================================
# run_helac.sh - HELAC-Onia LHE 生成脚本
# ==============================================================================
# 主要约束：
# 1. 运行于 HTCondor worker 节点，依赖打包传入的 helac_package.tar.gz。
# 2. 物理配置以 workbook_v2.md 为准，默认使用更新后的 LDME 参数。
# 3. 支持 test-mode，把积分与非加权事件数压到小批量验证可接受的范围。
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OCTET_PDG_TOOL="${BASE_DIR}/common/octet_pdg.py"
COMMON_DIR="${BASE_DIR}/common"

if [[ -f "${COMMON_DIR}/compression_helpers.sh" ]]; then
    source "${COMMON_DIR}/compression_helpers.sh"
fi

# Default values
POOL_NAME=""
MY_SEED=100
PROCESS_STRING=""
CHARM_STATE=""
BOTTOM_STATE=""
EXTRA_GLUON="false"
JOB_SLUG=""
STAGEOUT_MODE="lhe"
PARTON_SHOWER_LINE=""
MIN_PT_CONIA=6.0
MIN_PT_BONIA=4.0
MIN_PT_Q=0.0
WORKDIR=$(pwd)
OUTPUT_DIR=""
CMASS="1.54845"
BMASS="4.73020"
# workbook_v2 / 用户补充要求：
# 1. 优先使用 gener = 0 (PHEGAS)
# 2. gener = 0 时推荐 nopt = nmc/10, nopt_step = nmc/10, noptlim = nmc
# 3. preunw 推荐取 nmc/10
# 4. 当 unwevt 较小时，nmc 仍需设置一个下限，避免只得到 header-only LHE
GENER=0
UNWEVT=10000000
NMC=100000
PREUNW=10000
NOPT=10000
NOPT_STEP=10000
NOPT_LIM=100000
FAST_TEST=0
TEST_MODE="false"
UNWEVT_OVERRIDE=0
COMPRESS_LHE="false"
LHE_COMPRESSION_LEVEL=1
LHE_SHUFFLE_SPLIT="false"
LHE_EVENTS_PER_BLOCK=1000
LHE_SHUFFLE_MODE="stratified"
LHE_N_STRATA="auto"
LHE_DROP_INCOMPLETE_LAST_BLOCK="false"
# Build locations (populated after unpacking helac_package.tar.gz)
HEPMC_SRC_TGZ=""
HELAC_SRC_TAR=""
HEPMC_PREFIX="${WORKDIR}/HepMC/HepMC-2.06.11"
JOB_LOG_DIR="${WORKDIR}/command_logs"
COMMAND_LOG_INDEX=0
LAST_STDOUT_LOG=""
LAST_STDERR_LOG=""
LOG_STAGEOUT_ATTEMPTED=0

# T2_CN_Beijing XRootD storage paths
# Canonical form: redirector + LFN (see common/node_config_defaults.json).
# Override TARGET_EOS_BASE to redirect output to a different storage area.
EOS_REDIRECTOR="cceos.ihep.ac.cn"
EOS_HOST="${EOS_REDIRECTOR}"
EOS_XRDFS_TARGET="root://${EOS_HOST}"
EOS_LFN_BASE="/store/user/chiw/MC_Production_v3"
EOS_BASE="${TARGET_EOS_BASE:-root://${EOS_REDIRECTOR}/${EOS_LFN_BASE}}"
EOS_PATH_BASE="${EOS_LFN_BASE}"
EOS_OUTPUT="${EOS_BASE}/output"
EOS_GENERATED_LHE_BASE="${EOS_BASE}/lhe_pools"
NODE_CONFIG=""

# ----------------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------------

msg_info() { echo "[INFO] $1"; }
msg_warn() { echo "[WARN] $1"; }
msg_error() { echo "[ERROR] $1" >&2; }

sanitize_log_label() {
    local raw_label="$1"
    printf '%s' "${raw_label}" | sed 's/[^A-Za-z0-9._-]/_/g'
}

bool_is_true() {
    case "${1,,}" in
        1|true|yes|y) return 0 ;;
        *) return 1 ;;
    esac
}

state_is_octet() {
    local state="${1^^}"
    [[ "${state}" == *8 ]]
}

validate_fock_state() {
    case "${1^^}" in
        3S11|3P01|3P11|3P21|3S18|1S08|3P08|3P18|3P28)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

build_helac_matrix_process() {
    local charm_state="$1"
    local bottom_state="$2"
    local extra_gluon="$3"
    local process="generate g g > cc~(${charm_state}) bb~(${bottom_state})"

    if bool_is_true "${extra_gluon}"; then
        process="${process} g"
    fi
    printf '%s\n' "${process}"
}

load_node_config() {
    local config_path="$1"
    [[ -n "${config_path}" ]] || return 0
    if [[ ! -s "${config_path}" ]]; then
        msg_error "Node config JSON not found or empty: ${config_path}"
        return 1
    fi

    local kind key value extra
    while IFS=$'\t' read -r kind key value extra; do
        [[ "${kind}" == "storage" ]] || continue
        case "${key}" in
            eos_redirector)
                EOS_REDIRECTOR="${value}"
                EOS_HOST="${EOS_REDIRECTOR}"
                EOS_XRDFS_TARGET="root://${EOS_HOST}"
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
    done < <(python3 - "${config_path}" <<'PYHELPER'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    cfg = json.load(handle)
storage = cfg.get("storage", {})
if not isinstance(storage, dict):
    raise SystemExit("storage must be an object")
for key in ("eos_redirector", "eos_lfn_base", "eos_base", "target_eos_base", "generated_lhe_base", "output_subdir"):
    value = storage.get(key)
    if value not in (None, ""):
        print(f"storage\t{key}\t{value}\t")
PYHELPER
    )
}

ensure_job_log_dir() {
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

run_logged() {
    local label="$1"
    shift

    ensure_job_log_dir

    COMMAND_LOG_INDEX=$((COMMAND_LOG_INDEX + 1))
    local safe_label=""
    safe_label=$(sanitize_log_label "${label}")
    local log_prefix=""
    printf -v log_prefix "%s/%03d_%s" "${JOB_LOG_DIR}" "${COMMAND_LOG_INDEX}" "${safe_label}"
    local stdout_log="${log_prefix}.stdout"
    local stderr_log="${log_prefix}.stderr"
    local rc=0
    local stdout_size=0
    local stderr_size=0

    LAST_STDOUT_LOG="${stdout_log}"
    LAST_STDERR_LOG="${stderr_log}"

    msg_info "执行 ${label}，完整日志写入 ${log_prefix}.[stdout|stderr]"
    if "$@" >"${stdout_log}" 2>"${stderr_log}"; then
        stdout_size=$(wc -c < "${stdout_log}" 2>/dev/null || echo 0)
        stderr_size=$(wc -c < "${stderr_log}" 2>/dev/null || echo 0)
        msg_info "${label} 完成 (stdout=${stdout_size} B, stderr=${stderr_size} B)"
        return 0
    fi

    rc=$?
    msg_error "${label} 失败 (rc=${rc})"
    show_log_tail "${label} stderr" "${stderr_log}" 80 >&2
    show_log_tail "${label} stdout" "${stdout_log}" 40
    return "${rc}"
}

run_logged_bash() {
    local label="$1"
    local command_str="$2"
    run_logged "${label}" bash -lc "${command_str}"
}

make_remote_dir() {
    local remote_subpath="$1"
    local target=""
    local host=""
    local path=""
    local url=""

    target=$(resolve_remote_target "${remote_subpath}") || return 1
    IFS=$'\t' read -r host path url <<< "${target}"

    xrdfs "${host}" mkdir -p "${path}" >/dev/null 2>&1 || {
        msg_warn "远端目录创建失败或已存在: ${path} (${host})"
        return 1
    }
    return 0
}

normalize_remote_path() {
    local path="$1"
    while [[ "${path}" == //* ]]; do
        path="${path#/}"
    done
    [[ "${path}" == /* ]] || path="/${path}"
    printf '%s\n' "${path}"
}

join_remote_spec() {
    local base="$1"
    local child="$2"
    base="${base%/}"
    child="${child#/}"
    if [[ -z "${base}" ]]; then
        printf '%s\n' "${child}"
    elif [[ -z "${child}" ]]; then
        printf '%s\n' "${base}"
    else
        printf '%s/%s\n' "${base}" "${child}"
    fi
}

resolve_remote_target() {
    local spec="$1"
    local rest=""
    local host=""
    local path=""
    local url=""

    if [[ "${spec}" == root://* ]]; then
        rest="${spec#root://}"
        host="root://${rest%%/*}"
        path=$(normalize_remote_path "/${rest#*/}")
    elif [[ "${spec}" == /eos/* ]]; then
        host="root://eosuser.cern.ch"
        path="${spec}"
    elif [[ "${spec}" == /store/* ]]; then
        host="root://${EOS_HOST}"
        path="${spec}"
    else
        host="${EOS_XRDFS_TARGET}"
        path=$(normalize_remote_path "${EOS_PATH_BASE}/${spec}")
    fi

    url="root://${host#root://}/${path}"
    printf '%s\t%s\t%s\n' "${host}" "${path}" "${url}"
}

remote_url_for_spec() {
    local target=""
    local host=""
    local path=""
    local url=""

    target=$(resolve_remote_target "$1") || return 1
    IFS=$'\t' read -r host path url <<< "${target}"
    printf '%s\n' "${url}"
}

stage_out_worker_logs() {
    if [[ "${LOG_STAGEOUT_ATTEMPTED}" -eq 1 ]]; then
        return 0
    fi
    LOG_STAGEOUT_ATTEMPTED=1

    if [[ "${SKIP_STAGEOUT:-0}" -eq 1 ]]; then
        msg_info "SKIP_STAGEOUT=1, skip log stageout"
        return 0
    fi

    ensure_job_log_dir

    local remote_log_dir=""
    local bundle_name="logs_${POOL_NAME}_${MY_SEED}.tar.gz"
    local bundle_path="${WORKDIR}/${bundle_name}"
    local manifest_path="${WORKDIR}/log_manifest_${POOL_NAME}_${MY_SEED}.txt"

    {
        echo "pool=${POOL_NAME}"
        echo "seed=${MY_SEED}"
        echo "process=${PROCESS_STRING}"
        echo "min_pt_conia=${MIN_PT_CONIA}"
        echo "min_pt_bonia=${MIN_PT_BONIA}"
        echo "min_pt_q=${MIN_PT_Q}"
        echo "unwevt=${UNWEVT}"
        echo "nmc=${NMC}"
        echo "preunw=${PREUNW}"
    } > "${manifest_path}"

    (
        cd "${WORKDIR}" && \
        tar -czf "${bundle_path}" \
            command_logs \
            log_manifest_"${POOL_NAME}"_"${MY_SEED}".txt \
            py8_onia_user.inp \
            sample_"${POOL_NAME}"_"${MY_SEED}"_converted.lhe \
            helac_run.log \
            HELAC-Onia-2.7.6/run_config.ho \
            HELAC-Onia-2.7.6/input/user.inp \
            HELAC-Onia-2.7.6/PROC_HO_0/results/results.out \
            HELAC-Onia-2.7.6/PROC_HO_0/results/results.lhe \
            >/dev/null 2>&1 || true
    )

    if [[ ! -f "${bundle_path}" ]]; then
        msg_warn "未生成日志 bundle，跳过远端日志传输"
        return 0
    fi

    remote_log_dir=$(join_remote_spec "${OUTPUT_DIR}" "logs")
    make_remote_dir "${remote_log_dir}" || true
    local remote_url
    remote_url=$(remote_url_for_spec "$(join_remote_spec "${remote_log_dir}" "${bundle_name}")")
    msg_info "上传完整日志到: ${remote_url}"
    if xrdcp --nopbar --force "${bundle_path}" "${remote_url}" >/dev/null 2>&1; then
        msg_info "完整日志已上传: ${remote_url}"
    else
        msg_warn "完整日志上传失败: ${remote_url}"
    fi
}

find_helac_calc_output_dir() {
    local run_dir="$1"
    local output_dir=""

    if [[ -n "${run_dir}" && -d "${run_dir}/P0_calc_0/output" ]]; then
        printf '%s\n' "${run_dir}/P0_calc_0/output"
        return 0
    fi

    output_dir=$(find . -path "./PROC_HO_*/P0_*/output" -type d 2>/dev/null | sort | tail -1)
    if [[ -n "${output_dir}" && -d "${output_dir}" ]]; then
        printf '%s\n' "${output_dir}"
        return 0
    fi

    return 1
}

find_helac_run_dir() {
    local log_path="$1"
    local run_dir=""

    if [[ -f "${log_path}" ]]; then
        run_dir=$(grep "INFO: Results are collected in" "${log_path}" | \
                  sed -r -e "s,^.*(PROC_HO_[0-9]+)\/.*$,\1,g" | head -1)
        if [[ -n "${run_dir}" && -d "${run_dir}" ]]; then
            printf '%s\n' "${run_dir}"
            return 0
        fi
    fi

    run_dir=$(find . -maxdepth 1 -type d -name "PROC_HO_*" | sort | tail -1)
    if [[ -n "${run_dir}" && -d "${run_dir}" ]]; then
        printf '%s\n' "${run_dir#./}"
        return 0
    fi

    return 1
}

stage_out_helac_output_archive() {
    local run_dir="$1"
    local calc_output_dir=""
    local safe_slug=""
    local archive_name=""
    local archive_path=""
    local remote_dir=""
    local remote_url=""

    if [[ "${SKIP_STAGEOUT:-0}" -eq 1 ]]; then
        echo "[INFO] SKIP_STAGEOUT=1, skip HELAC output archive stageout"
        return 0
    fi

    if ! calc_output_dir=$(find_helac_calc_output_dir "${run_dir}"); then
        echo "[ERROR] Could not find HELAC calc output directory under ${run_dir:-PROC_HO_*}"
        return 1
    fi

    safe_slug=$(sanitize_log_label "${JOB_SLUG:-${POOL_NAME}_${MY_SEED}}")
    archive_name="helac_output_${safe_slug}_${MY_SEED}.tar.gz"
    archive_path="${WORKDIR}/${archive_name}"

    echo "[INFO] Archiving HELAC output directory: ${calc_output_dir}"
    run_logged "tar_helac_output" tar -C "$(dirname "${calc_output_dir}")" -czf "${archive_path}" "$(basename "${calc_output_dir}")"

    remote_dir=$(join_remote_spec "${OUTPUT_DIR}" "${safe_slug}")
    make_remote_dir "${remote_dir}" || true
    remote_url=$(remote_url_for_spec "$(join_remote_spec "${remote_dir}" "${archive_name}")")
    echo "[INFO] Staging out HELAC output archive to: ${remote_url}"
    run_logged "xrdcp_helac_output_stageout" xrdcp --nopbar --force "${archive_path}" "${remote_url}" || {
        echo "Error: Failed to stage out HELAC output archive"
        return 1
    }

    echo "[INFO] HELAC output archive complete: ${remote_url}"
}

stage_out_forbidden_marker() {
    local marker_name=""
    local marker_path=""
    local remote_dir=""
    local remote_url=""
    local safe_slug=""

    safe_slug=$(sanitize_log_label "${JOB_SLUG:-${POOL_NAME}_${MY_SEED}}")
    marker_name="helac_forbidden_${safe_slug}_${MY_SEED}.txt"
    marker_path="${WORKDIR}/${marker_name}"

    cat > "${marker_path}" << EOF
status: forbidden
reason: HELAC-Onia reported no nonvanishing subprocesses
pool: ${POOL_NAME}
process: ${PROCESS_STRING}
charm_state: ${CHARM_STATE}
bottom_state: ${BOTTOM_STATE}
extra_gluon: ${EXTRA_GLUON}
job_slug: ${JOB_SLUG}
seed: ${MY_SEED}
EOF

    if [[ "${SKIP_STAGEOUT:-0}" -eq 1 ]]; then
        echo "[INFO] SKIP_STAGEOUT=1, skip forbidden-channel marker upload"
        return 0
    fi

    remote_dir=$(join_remote_spec "${OUTPUT_DIR}" "forbidden")
    make_remote_dir "${remote_dir}" || true
    remote_url=$(remote_url_for_spec "$(join_remote_spec "${remote_dir}" "${marker_name}")")
    echo "[INFO] Staging out forbidden-channel marker to: ${remote_url}"
    if xrdcp --nopbar --force "${marker_path}" "${remote_url}" >/dev/null 2>&1; then
        echo "[INFO] Forbidden-channel marker uploaded: ${remote_url}"
    else
        echo "[WARN] Failed to upload forbidden-channel marker: ${remote_url}"
    fi
}

upload_logs_on_exit() {
    local exit_code=$?
    set +e
    stage_out_worker_logs
    exit "${exit_code}"
}

trap upload_logs_on_exit EXIT

setup_build_env() {
    # Minimal environment for building HepMC/HELAC inside the worker node
    if ! command -v python >/dev/null 2>&1; then
        if command -v python3 >/dev/null 2>&1; then
            mkdir -p "${WORKDIR}/.local/bin"
            cat > "${WORKDIR}/.local/bin/python" << 'PYWRAP'
#!/bin/bash
exec python3 "$@"
PYWRAP
            chmod +x "${WORKDIR}/.local/bin/python"
            export PATH="${WORKDIR}/.local/bin:$PATH"
        elif [ -x "/cvmfs/sft.cern.ch/lcg/releases/Python/2.7.13-597a5/x86_64-centos7-gcc62-opt/bin/python" ]; then
            export PATH="/cvmfs/sft.cern.ch/lcg/releases/Python/2.7.13-597a5/x86_64-centos7-gcc62-opt/bin:$PATH"
        fi
    fi
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    source /cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh
    export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/releases/LCG_88b/Boost/1.62.0/x86_64-centos7-gcc62-opt/lib:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/contrib/gcc/6.2.0/x86_64-centos7-gcc62-opt/lib64:/opt/rh/gcc-toolset-12/root/usr/lib64:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/releases/LCG_88b/Boost/1.62.0/x86_64-centos7-gcc62-opt/lib:$LD_LIBRARY_PATH
    export PATH=/cvmfs/sft.cern.ch/lcg/contrib/gcc/6.2.0/x86_64-centos7-gcc62-opt/bin:/opt/rh/gcc-toolset-12/root/usr/bin:$PATH
    # 某些 el7 worker 不提供 C.UTF-8 locale，统一退回 C，避免构建阶段刷屏警告。
    export LANG=${LANG:-C}
    export LC_ALL=${LC_ALL:-C}
    unset PYTHONHOME PYTHONPATH
}

ensure_hepmc() {
    if [ -d "${HEPMC_PREFIX}/install" ]; then
        echo "[INFO] Reusing existing HepMC build at ${HEPMC_PREFIX}"
        return 0
    fi

    if [ -z "${HEPMC_SRC_TGZ}" ] || [ ! -f "${HEPMC_SRC_TGZ}" ]; then
        echo "Error: HepMC source tarball not found"
        return 1
    fi

    msg_info "Building HepMC from ${HEPMC_SRC_TGZ}..."
    mkdir -p "${WORKDIR}/HepMC"
    run_logged "untar_hepmc" tar -xzf "${HEPMC_SRC_TGZ}" -C "${WORKDIR}/HepMC"
    cd "${HEPMC_PREFIX}"
    mkdir -p build install
    cd build
    run_logged "configure_hepmc" "${HEPMC_PREFIX}/configure" --prefix="${HEPMC_PREFIX}/install" --with-momentum=GEV --with-length=MM
    run_logged "make_hepmc" make -j 2
    run_logged "check_hepmc" make check
    run_logged "install_hepmc" make install
    cd "${WORKDIR}"
}

ensure_helac() {
    if [ -d "${WORKDIR}/HELAC-Onia-2.7.6" ] && [ -x "${WORKDIR}/HELAC-Onia-2.7.6/ho_cluster" ]; then
        echo "[INFO] Reusing existing HELAC-Onia build"
        return 0
    fi

    if [ -z "${HELAC_SRC_TAR}" ] || [ ! -f "${HELAC_SRC_TAR}" ]; then
        echo "Error: HELAC-Onia source tarball not found"
        return 1
    fi

    msg_info "Unpacking HELAC-Onia from ${HELAC_SRC_TAR}..."
    run_logged "untar_helac" tar -xzf "${HELAC_SRC_TAR}" -C "${WORKDIR}"

    cd "${WORKDIR}/HELAC-Onia-2.7.6"

    # Keep HepMC optional on worker nodes (avoid linking HepMC2Plot)
    sed -i -r -e 's|^[[:space:]]*hepmc_path[[:space:]]*=.*|# hepmc_path is left unset for condor runs|' input/ho_configuration.txt

    # Fix heptoptagger interface to compile with newer gcc
    sed -i 's/HEPTopTagger::HEPTopTagger /HEPTopTagger /g' analysis/heptoptagger/heptoptagger_fjcore_interface.cc

    msg_info "Configuring HELAC-Onia..."
    run_logged "config_helac" ./config
    cd "${WORKDIR}"
}

normalize_prebuilt_helac_links() {
    local helac_dir="${WORKDIR}/HELAC-Onia-2.7.6"

    [[ -d "${helac_dir}" ]] || return 0
    cd "${helac_dir}"

    if [[ -L ho_cluster ]] && [[ "$(readlink ho_cluster)" = /* ]]; then
        ln -sfn cluster/bin/ho_cluster ho_cluster
    fi
    if [[ -L bin/ho_cluster ]] && [[ "$(readlink bin/ho_cluster)" = /* ]]; then
        ln -sfn ../cluster/bin/ho_cluster bin/ho_cluster
    fi
    if [[ -L Helac-Onia ]] && [[ "$(readlink Helac-Onia)" = /* ]]; then
        ln -sfn bin/Helac-Onia Helac-Onia
    fi
    if [[ -L addon/pp_NOnia_MPS/bin/HO_pp_NOnia_MPS ]] && [[ "$(readlink addon/pp_NOnia_MPS/bin/HO_pp_NOnia_MPS)" = /* ]]; then
        ln -sfn ../../../bin/HO_pp_NOnia_MPS addon/pp_NOnia_MPS/bin/HO_pp_NOnia_MPS
    fi

    cd "${WORKDIR}"
}

has_prebuilt_helac() {
    [[ -d "${WORKDIR}/HELAC-Onia-2.7.6" ]] && [[ -x "${WORKDIR}/HELAC-Onia-2.7.6/ho_cluster" ]]
}

write_py8_onia_config() {
    local pool_name="$1"
    local output_file="$2"

    case "$pool_name" in
        "pool_2jpsi_cs"|"pool_2jpsi_g")
            cat > "${output_file}" << 'EOF'
2
443 443
EOF
            ;;
        "pool_jpsi_CSCO_g")
            cat > "${output_file}" << 'EOF'
1
443
EOF
            ;;
        "pool_upsilon_CSCO_g")
            cat > "${output_file}" << 'EOF'
1
553
EOF
            ;;
        "pool_jpsi_upsilon_CSCO")
            cat > "${output_file}" << 'EOF'
2
443 553
EOF
            ;;
        "helac_matrix")
            cat > "${output_file}" << 'EOF'
2
443 553
EOF
            ;;
        "pool_gg")
            cat > "${output_file}" << 'EOF'
0
EOF
            ;;
        *)
            return 1
            ;;
    esac
}

compiler_supports_flag() {
    local flag="$1"
    local test_src="${WORKDIR}/.gfortran_flag_check.f"
    local test_obj="${WORKDIR}/.gfortran_flag_check.o"

    cat > "${test_src}" << 'EOF'
      end
EOF

    if gfortran "${flag}" -c "${test_src}" -o "${test_obj}" >/dev/null 2>&1; then
        rm -f "${test_src}" "${test_obj}"
        return 0
    fi

    rm -f "${test_src}" "${test_obj}"
    return 1
}

build_lhe_converter_if_needed() {
    if [[ -x "${WORKDIR}/lhe_pythia6_pythia8" ]]; then
        return 0
    fi
    if [[ ! -f "${WORKDIR}/lhe_pythia6_pythia8.f" ]]; then
        return 1
    fi

    local build_flags=("-O2")
    if compiler_supports_flag "-fallow-argument-mismatch"; then
        build_flags+=("-fallow-argument-mismatch")
    else
        echo "[WARN] 当前 gfortran 不支持 -fallow-argument-mismatch，改用兼容编译参数"
    fi

    msg_info "Building lhe_pythia6_pythia8 converter..."
    run_logged "build_lhe_converter" gfortran "${build_flags[@]}" -o "${WORKDIR}/lhe_pythia6_pythia8" "${WORKDIR}/lhe_pythia6_pythia8.f"
}

verify_lhe_octet_codes() {
    local lhe_file="$1"
    if [[ ! -f "${lhe_file}" ]]; then
        echo "[ERROR] LHE file not found for octet verification: ${lhe_file}"
        return 1
    fi
    if [[ ! -f "${OCTET_PDG_TOOL}" ]]; then
        echo "[ERROR] Octet PDG tool not found: ${OCTET_PDG_TOOL}"
        return 1
    fi

    PYTHONIOENCODING=UTF-8 python3 "${OCTET_PDG_TOOL}" scan "${lhe_file}" --fail-on-legacy
}

count_lhe_events() {
    local lhe_file="$1"
    local event_count="0"
    if [[ ! -f "${lhe_file}" ]]; then
        echo "0"
        return 0
    fi
    event_count=$(grep -c '^[[:space:]]*<event>' "${lhe_file}" 2>/dev/null || true)
    if [[ -z "${event_count}" ]]; then
        event_count="0"
    fi
    echo "${event_count}"
}

set_user_inp_value() {
    local user_inp="$1"
    local key="$2"
    local value="$3"

    if grep -q -E "^${key}[[:space:]]" "${user_inp}"; then
        sed -i -E "s|^(${key})[[:space:]].*$|\\1 ${value}|" "${user_inp}"
    else
        printf '%s %s\n' "${key}" "${value}" >> "${user_inp}"
    fi
}

prepare_runtime_user_inp() {
    local helac_dir="$1"
    local runtime_user_inp="${helac_dir}/input/user.inp"
    local template_user_inp="../input_templates/user.inp"

    if [[ -f "${template_user_inp}" ]]; then
        cp "${template_user_inp}" "${runtime_user_inp}"
    elif [[ ! -f "${runtime_user_inp}" ]]; then
        echo "[ERROR] 找不到可用的 user.inp 模板"
        return 1
    fi

    # worker 上显式重写 Monte Carlo 相关参数，避免旧模板把 run_config.ho 中的
    # 积分设置重新覆盖掉。
    sed -i -E \
        -e "s|^(cmass)[[:space:]].*$|\\1 ${CMASS}d0|" \
        -e "s|^(bmass)[[:space:]].*$|\\1 ${BMASS}d0|" \
        -e "s|^(minptq)[[:space:]].*$|\\1 ${MIN_PT_Q}d0|" \
        -e "s|^(minptconia)[[:space:]].*$|\\1 ${MIN_PT_CONIA}d0|" \
        -e "s|^(minptbonia)[[:space:]].*$|\\1 ${MIN_PT_BONIA}d0|" \
        -e "s|^(preunw)[[:space:]].*$|\\1 ${PREUNW}|" \
        -e "s|^(unwevt)[[:space:]].*$|\\1 ${UNWEVT}|" \
        -e "s|^(nmc)[[:space:]].*$|\\1 ${NMC}|" \
        -e "s|^(nopt)[[:space:]].*$|\\1 ${NOPT}|" \
        -e "s|^(nopt_step)[[:space:]].*$|\\1 ${NOPT_STEP}|" \
        -e "s|^(noptlim)[[:space:]].*$|\\1 ${NOPT_LIM}|" \
        -e "s|^(gener)[[:space:]].*$|\\1 ${GENER}|" \
        -e "s|^(ranhel)[[:space:]].*$|\\1 4|" \
        "${runtime_user_inp}"

    # Defaults needed by CrystalBall addons; existing ho_cluster pools ignore
    # keys they do not use.
    set_user_inp_value "${runtime_user_inp}" "lhapdf" "F"
    set_user_inp_value "${runtime_user_inp}" "beam2_pdf" "-1"
    set_user_inp_value "${runtime_user_inp}" "useMCFMrun" "T"
    set_user_inp_value "${runtime_user_inp}" "itmax" "1"
    set_user_inp_value "${runtime_user_inp}" "muF_over_ref" "1d0"
    set_user_inp_value "${runtime_user_inp}" "PDF_Hessian2MC" "F"
    set_user_inp_value "${runtime_user_inp}" "N_MCPDFs" "100"
    set_user_inp_value "${runtime_user_inp}" "Way_Hessian2MC" "1"
    set_user_inp_value "${runtime_user_inp}" "reweight_pdf" "F"
    set_user_inp_value "${runtime_user_inp}" "pdf_min" "21101"
    set_user_inp_value "${runtime_user_inp}" "pdf_max" "21140"
    set_user_inp_value "${runtime_user_inp}" "beam2_pdf_min" "-1"
    set_user_inp_value "${runtime_user_inp}" "beam2_pdf_max" "-1"
    set_user_inp_value "${runtime_user_inp}" "nPDF_id" "0"
    set_user_inp_value "${runtime_user_inp}" "reweight_npdf" "F"
    set_user_inp_value "${runtime_user_inp}" "npdf_min" "402"
    set_user_inp_value "${runtime_user_inp}" "npdf_max" "431"
    set_user_inp_value "${runtime_user_inp}" "npdf_isospin" "T"
    set_user_inp_value "${runtime_user_inp}" "include_ref" "T"
    set_user_inp_value "${runtime_user_inp}" "fermion_motion" "F"
    set_user_inp_value "${runtime_user_inp}" "pmax_fermion_motion" "0.26d0"
    set_user_inp_value "${runtime_user_inp}" "literature" "0"
    set_user_inp_value "${runtime_user_inp}" "fixtarget" "F"
    set_user_inp_value "${runtime_user_inp}" "topdrawer_output" "F"
    set_user_inp_value "${runtime_user_inp}" "gnuplot_output" "F"
    set_user_inp_value "${runtime_user_inp}" "root_output" "F"
    set_user_inp_value "${runtime_user_inp}" "hwu_output" "F"

    echo "[INFO] 运行时 user.inp 中的关键积分参数:"
    grep -E '^(cmass|bmass|minptq|minptconia|minptbonia|preunw|unwevt|nmc|nopt|nopt_step|noptlim|gener|ranhel)[[:space:]]' "${runtime_user_inp}"
}

pool_uses_crystalball_addon() {
    case "$1" in
        "pool_jpsi_CSCO_g"|"pool_upsilon_CSCO_g")
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

crystalball_state_for_pool() {
    case "$1" in
        "pool_jpsi_CSCO_g") echo "1" ;;
        "pool_upsilon_CSCO_g") echo "3" ;;
        *) return 1 ;;
    esac
}

crystalball_default_card_for_pool() {
    case "$1" in
        "pool_jpsi_CSCO_g") echo "crystalball_jpsi.inp" ;;
        "pool_upsilon_CSCO_g") echo "crystalball_Y1S.inp" ;;
        *) return 1 ;;
    esac
}

prepare_crystalball_addon_input() {
    local helac_dir="$1"
    local pool_name="$2"
    local addon_dir="${helac_dir}/addon/pp_psiX_CrystalBall"
    local state=""
    local crystalball_card=""
    local minpt_onia=""

    state=$(crystalball_state_for_pool "${pool_name}")
    crystalball_card=$(crystalball_default_card_for_pool "${pool_name}")

    prepare_runtime_user_inp "${addon_dir}"

    if [[ "${pool_name}" == "pool_upsilon_CSCO_g" ]]; then
        minpt_onia="${MIN_PT_BONIA}"
    else
        minpt_onia="${MIN_PT_CONIA}"
    fi

    printf '%s\n# CrystalBall state selected by run_helac.sh\n' "${state}" > "${addon_dir}/input/state.inp"
    cp "${helac_dir}/input/default.inp" "${addon_dir}/input/default.inp"
    for support_card in \
        decay_default.inp decay_param_default.inp decay_user.inp decay_param_user.inp \
        fragment_default.inp fragment_user.inp fragment_card_default.inp fragment_card_user.inp \
        shower_card_default.inp shower_card_user.inp py8_onia_default.inp py8_onia_user.inp; do
        if [[ -f "${helac_dir}/input/${support_card}" ]]; then
            cp "${helac_dir}/input/${support_card}" "${addon_dir}/input/${support_card}"
        fi
    done
    cp "${addon_dir}/input/${crystalball_card}" "${addon_dir}/input/crystalball.inp"
    ln -sfn ../../pdf "${addon_dir}/pdf"
    set_user_inp_value "${addon_dir}/input/user.inp" "minpt1c" "${minpt_onia}d0"
    set_user_inp_value "${addon_dir}/input/user.inp" "maxpt1c" "-1d0"
    set_user_inp_value "${addon_dir}/input/user.inp" "miny1c" "-2.4d0"
    set_user_inp_value "${addon_dir}/input/user.inp" "maxy1c" "2.4d0"

    echo "[INFO] CrystalBall addon input:"
    echo "  state.inp: ${state}"
    echo "  crystalball.inp: ${crystalball_card}"
    grep -E '^(minpt1c|maxpt1c|miny1c|maxy1c|itmax|nmc|preunw|unwevt)[[:space:]]' "${addon_dir}/input/user.inp"
}

build_crystalball_addon_if_needed() {
    local helac_dir="$1"
    local addon_dir="${helac_dir}/addon/pp_psiX_CrystalBall"
    local exe="${addon_dir}/bin/HO_pp_psiX_CrystalBall"

    if [[ -x "${exe}" ]]; then
        return 0
    fi

    mkdir -p "${helac_dir}/bin" "${addon_dir}/bin" "${addon_dir}/obj" "${addon_dir}/mod" "${addon_dir}/output" "${addon_dir}/tmp"
    msg_info "Building pp_psiX_CrystalBall addon..."
    run_logged "build_pp_psiX_CrystalBall" make -C "${addon_dir}" -f makefile_pp_psiX_CrystalBall HODIR="${helac_dir}" FC="${FC:-gfortran}" all
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --pool)
            POOL_NAME="$2"
            shift 2
            ;;
        --seed|-s)
            MY_SEED="$2"
            shift 2
            ;;
        --process)
            PROCESS_STRING="$2"
            shift 2
            ;;
        --charm-state)
            CHARM_STATE="${2^^}"
            shift 2
            ;;
        --bottom-state)
            BOTTOM_STATE="${2^^}"
            shift 2
            ;;
        --extra-gluon)
            EXTRA_GLUON="$2"
            shift 2
            ;;
        --job-slug)
            JOB_SLUG="$2"
            shift 2
            ;;
        --stageout-mode)
            STAGEOUT_MODE="$2"
            shift 2
            ;;
        --min-pt-conia)
            MIN_PT_CONIA="$2"
            shift 2
            ;;
        --min-pt-bonia)
            MIN_PT_BONIA="$2"
            shift 2
            ;;
        --min-pt-q)
            MIN_PT_Q="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --config)
            NODE_CONFIG="$2"
            shift 2
            ;;
        --unwevt)
            UNWEVT="$2"
            UNWEVT_OVERRIDE=1
            shift 2
            ;;
        --fast-test)
            FAST_TEST=1
            shift 1
            ;;
        --test-mode)
            TEST_MODE="$2"
            shift 2
            ;;
        --compress-lhe)
            COMPRESS_LHE="true"
            shift 1
            ;;
        --lhe-compression-level)
            LHE_COMPRESSION_LEVEL="$2"
            shift 2
            ;;
        --lhe-shuffle-split)
            LHE_SHUFFLE_SPLIT="true"
            shift 1
            ;;
        --lhe-events-per-block)
            LHE_EVENTS_PER_BLOCK="$2"
            shift 2
            ;;
        --lhe-shuffle-mode)
            LHE_SHUFFLE_MODE="$2"
            shift 2
            ;;
        --lhe-n-strata)
            LHE_N_STRATA="$2"
            shift 2
            ;;
        --lhe-drop-incomplete-last-block)
            LHE_DROP_INCOMPLETE_LAST_BLOCK="true"
            shift 1
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [[ -n "${CHARM_STATE}" || -n "${BOTTOM_STATE}" ]]; then
    if [[ -z "${CHARM_STATE}" || -z "${BOTTOM_STATE}" ]]; then
        echo "Error: --charm-state and --bottom-state must be provided together"
        exit 1
    fi
    if ! validate_fock_state "${CHARM_STATE}"; then
        echo "Error: Unsupported charm Fock state: ${CHARM_STATE}"
        exit 1
    fi
    if ! validate_fock_state "${BOTTOM_STATE}"; then
        echo "Error: Unsupported bottom Fock state: ${BOTTOM_STATE}"
        exit 1
    fi

    POOL_NAME="${POOL_NAME:-helac_matrix}"
    PROCESS_STRING=$(build_helac_matrix_process "${CHARM_STATE}" "${BOTTOM_STATE}" "${EXTRA_GLUON}")
    PARTON_SHOWER_LINE="set parton_shower = 1"
    if [[ -z "${JOB_SLUG}" ]]; then
        if bool_is_true "${EXTRA_GLUON}"; then
            JOB_SLUG="c${CHARM_STATE}_b${BOTTOM_STATE}_g"
        else
            JOB_SLUG="c${CHARM_STATE}_b${BOTTOM_STATE}_born"
        fi
    fi
    if state_is_octet "${CHARM_STATE}"; then
        CMASS="1.64845"
    fi
    if state_is_octet "${BOTTOM_STATE}"; then
        BMASS="4.83020"
    fi
fi

# Validate required arguments
if [ -z "$POOL_NAME" ]; then
    echo "Error: --pool is required"
    exit 1
fi

if ! load_node_config "${NODE_CONFIG}"; then
    exit 1
fi

# Set default process string based on pool name if not specified
if [ -z "$PROCESS_STRING" ]; then
    case "$POOL_NAME" in
        # =====================================================================
        # CSCO Pools (Color Singlet + Color Octet combined using define)
        # These are the PRIMARY pools recommended by workbook.md
        # =====================================================================
        "pool_jpsi_CSCO_g")
            PROCESS_STRING="addon/pp_psiX_CrystalBall state=J/psi"
            ;;
        "pool_upsilon_CSCO_g")
            PROCESS_STRING="addon/pp_psiX_CrystalBall state=Upsilon(1S)"
            ;;
        "pool_jpsi_upsilon_CSCO")
            # J/psi + Upsilon (CS only for now, as per workbook)
            PROCESS_STRING="generate g g > jpsi y(1s)"
            ;;
            
        # =====================================================================
        # Basic Single/Double Onia Pools (Color Singlet only)
        # =====================================================================
        "pool_gg")
            PROCESS_STRING="generate g g > g g"
            ;;
        "pool_2jpsi_cs")
            PROCESS_STRING="generate g g > cc~(3S11) cc~(3S11)"
            ;;
        "pool_2jpsi_g")
            PROCESS_STRING="generate g g > cc~(3S11) cc~(3S11) g"
            ;;
            
        *)
            echo "Error: Unknown pool name and no process string specified"
            echo "Available pools (CSCO - recommended):"
            echo "  - pool_jpsi_CSCO_g, pool_upsilon_CSCO_g, pool_jpsi_upsilon_CSCO"
            echo "Available pools (basic):"
            echo "  - pool_gg, pool_2jpsi_cs, pool_2jpsi_g"
            exit 1
            ;;
    esac
fi

# Set default output directory (XRootD path for T2_CN_Beijing)
if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="${EOS_GENERATED_LHE_BASE%/}/${POOL_NAME}"
fi

# Validate seed
if ! [[ "$MY_SEED" =~ ^[0-9]+$ ]]; then
    echo "Error: Seed must be a valid integer"
    exit 1
fi

if [ "$MY_SEED" -le 10 ] || [ "$MY_SEED" -ge 100000 ]; then
    echo "Error: Seed must be between 11 and 99999"
    exit 1
fi

# Apply fast-test presets (drastically fewer integration/event counts)
if [ "$TEST_MODE" = "true" ]; then
    FAST_TEST=1
fi

# 正式生产统一使用固定的 HELAC 积分参数；测试模式单独收缩。
if [ "$FAST_TEST" -eq 1 ]; then
    if [ "$UNWEVT_OVERRIDE" -eq 0 ]; then
        UNWEVT=100
    fi
    GENER=0
    if ! [[ "$UNWEVT" =~ ^[0-9]+$ ]] || [ "$UNWEVT" -le 0 ]; then
        echo "Error: --unwevt must be a positive integer"
        exit 1
    fi
    if [ "$UNWEVT" -ge 100000 ]; then
        NMC="$UNWEVT"
    else
        NMC=100000
    fi
    PREUNW=$(( NMC / 10 ))
    NOPT=$(( NMC / 10 ))
    NOPT_STEP=$(( NMC / 10 ))
    NOPT_LIM=$(( NMC ))
else
    GENER=0
    UNWEVT=100000
    PREUNW=500000
    NMC=5000000
    NOPT=500000
    NOPT_STEP=500000
    NOPT_LIM=5000000
fi

echo "=============================================="
echo "HELAC-Onia LHE Generation"
echo "=============================================="
echo "Pool:           $POOL_NAME"
echo "Process:        $PROCESS_STRING"
if [[ -n "${CHARM_STATE}" ]]; then
    echo "Charm state:    ${CHARM_STATE}"
    echo "Bottom state:   ${BOTTOM_STATE}"
    echo "Extra gluon:    ${EXTRA_GLUON}"
    echo "Job slug:       ${JOB_SLUG}"
fi
echo "Seed:           $MY_SEED"
echo "Charm mass:     ${CMASS} GeV"
echo "Bottom mass:    ${BMASS} GeV"
echo "Min pT (conia): $MIN_PT_CONIA GeV"
echo "Min pT (bonia): $MIN_PT_BONIA GeV"
echo "Min pT (q):     $MIN_PT_Q GeV"
echo "Unw. events:    $UNWEVT"
echo "Generator:      PHEGAS (gener=${GENER})"
echo "preunw:         $PREUNW"
echo "nmc:            $NMC"
echo "nopt:           $NOPT"
echo "nopt_step:      $NOPT_STEP"
echo "noptlim:        $NOPT_LIM"
if [ "$FAST_TEST" -eq 1 ]; then
    echo "Mode:           FAST TEST"
fi
echo "Output dir:     $OUTPUT_DIR"
echo "=============================================="

ensure_job_log_dir

# Check for HELAC package
if [ ! -f "helac_package.tar.gz" ]; then
    echo "Error: helac_package.tar.gz not found"
    exit 1
fi

# Prepare build environment and unpack archives
setup_build_env
run_logged "untar_helac_package" tar -xzf helac_package.tar.gz

# Locate source tarballs (either freshly unpacked or already present)
[ -f "${WORKDIR}/hepmc2.06.11.tgz" ] && HEPMC_SRC_TGZ="${WORKDIR}/hepmc2.06.11.tgz"
[ -f "${WORKDIR}/HELAC-Onia-2.7.6.tar.gz" ] && HELAC_SRC_TAR="${WORKDIR}/HELAC-Onia-2.7.6.tar.gz"

# Setup HepMC paths when the package carries a prebuilt install.
if [ -d "${HEPMC_PREFIX}/install" ]; then
    export PATH=${HEPMC_PREFIX}/install/bin:$PATH
    export LD_LIBRARY_PATH=${HEPMC_PREFIX}/install/lib:$LD_LIBRARY_PATH
fi

if has_prebuilt_helac; then
    echo "[INFO] Reusing prebuilt HELAC-Onia runtime"
    normalize_prebuilt_helac_links
else
    # Build dependencies from packaged sources.
    ensure_hepmc

    if [ -d "${HEPMC_PREFIX}/install" ]; then
        export PATH=${HEPMC_PREFIX}/install/bin:$PATH
        export LD_LIBRARY_PATH=${HEPMC_PREFIX}/install/lib:$LD_LIBRARY_PATH
    fi

    ensure_helac
fi

# Enter HELAC directory
cd HELAC-Onia-2.7.6

PY8_ONIA_CONFIG=""

if pool_uses_crystalball_addon "${POOL_NAME}"; then
    prepare_crystalball_addon_input "$(pwd)" "${POOL_NAME}"
    build_crystalball_addon_if_needed "$(pwd)"

    msg_info "Running HELAC-Onia CrystalBall addon..."
    run_logged_bash "helac_pp_psiX_CrystalBall" "cd '$(pwd)/addon/pp_psiX_CrystalBall' && ./bin/HO_pp_psiX_CrystalBall"
    HELAC_RUN_LOG="${LAST_STDOUT_LOG}"
    cp "${HELAC_RUN_LOG}" ../helac_run.log
    show_log_tail "CrystalBall addon stdout" "${HELAC_RUN_LOG}" 120

    RAW_LHE_FILE=$(find addon/pp_psiX_CrystalBall/output output \
        -name "sample_pp_psiX_crystalball.lhe" -type f 2>/dev/null | sort | tail -1)
    if [[ -z "${RAW_LHE_FILE}" ]]; then
        RAW_LHE_FILE=$(find addon/pp_psiX_CrystalBall/output output \
            -name "*.lhe" -type f ! -name "*_py8.lhe" 2>/dev/null | sort | tail -1)
    fi
    PY8_LHE_FILE=""
else
    # Create run configuration with LDME parameters
    # LDME values from:
    # - workbook_v2.md 中给出的 Helac-Onia 格式换算值
    cat > run_config.ho << EOF
set cmass = ${CMASS}d0
set bmass = ${BMASS}d0
set LDMEcc1S08 = 0.0023125d0
set LDMEcc3S18 = 0.0003528845833333333d0
set LDMEcc3P08 = 0.0040024d0
set LDMEcc3P18 = 0.004002404166666667d0
set LDMEcc3P28 = 0.0040024d0
set LDMEcc3S11 = 0.06444444444444444d0
set LDMEbb1S08 = 0.000021266d0
set LDMEbb3S18 = 0.001239275d0
set LDMEbb3P08 = 0.10807425d0
set LDMEbb3P18 = 0.1080741666666667d0
set LDMEbb3P28 = 0.10807425d0
set LDMEbb3S11 = 0.5155555555555555d0
set preunw = ${PREUNW}
set unwevt = ${UNWEVT}
set nmc = ${NMC}
set nopt = ${NOPT}
set nopt_step = ${NOPT_STEP}
set noptlim = ${NOPT_LIM}
set gener = ${GENER}
set seed = ${MY_SEED}
set minptconia = ${MIN_PT_CONIA}d0
set minptbonia = ${MIN_PT_BONIA}d0
set maxrapconia = 2.4
set minptq = ${MIN_PT_Q}
set ranhel = 4
${PARTON_SHOWER_LINE}
${PROCESS_STRING}
launch
exit
EOF

    # 强制同步 runtime user.inp，避免静态模板中的旧积分参数覆盖掉当前作业设置。
    prepare_runtime_user_inp "$(pwd)"

    PY8_ONIA_KIND="${POOL_NAME}"
    if [[ "${POOL_NAME}" == "helac_matrix" || "${STAGEOUT_MODE}" == "helac-output" ]]; then
        PY8_ONIA_KIND="helac_matrix"
    fi
    PY8_ONIA_CONFIG="${WORKDIR}/py8_onia_user.inp"
    if write_py8_onia_config "${PY8_ONIA_KIND}" "input/py8_onia_user.inp"; then
        cp "input/py8_onia_user.inp" "${PY8_ONIA_CONFIG}"
        echo "[INFO] HELAC input/py8_onia_user.inp:"
        cat "input/py8_onia_user.inp"
    else
        echo "[WARN] No py8_onia_user.inp rule for ${PY8_ONIA_KIND}; converter fallback may use raw LHE"
    fi

    msg_info "Running HELAC-Onia..."
    run_logged_bash "helac_ho_cluster" "cd '$(pwd)' && ./ho_cluster < run_config.ho"
    HELAC_RUN_LOG="${LAST_STDOUT_LOG}"
    cp "${HELAC_RUN_LOG}" ../helac_run.log

    if grep -q "No nonvanishing subprocesses are found" "${HELAC_RUN_LOG}"; then
        echo "[INFO] HELAC-Onia found no nonvanishing subprocesses; treating this channel as QCD-forbidden."
        stage_out_forbidden_marker
        exit 0
    fi

    # Find output LHE file
    if ! RUN_DIR=$(find_helac_run_dir "${HELAC_RUN_LOG}"); then
        echo "Error: Could not find HELAC run directory"
        exit 1
    fi
    echo "[INFO] HELAC run directory: ${RUN_DIR}"

    # Find the LHE file from the latest run directory
    if [[ -n "${RUN_DIR}" ]] && [[ -d "${RUN_DIR}/results" ]]; then
        RAW_LHE_FILE=$(find "${RUN_DIR}/results" -name "*.lhe" -type f ! -name "*_py8.lhe" | head -1)
        PY8_LHE_FILE=$(find "${RUN_DIR}/results" -name "*_py8.lhe" -type f | head -1)
    else
        RAW_LHE_FILE=$(find . -path "./PROC_HO_*/results/*.lhe" -type f ! -name "*_py8.lhe" | sort | tail -1)
        PY8_LHE_FILE=$(find . -path "./PROC_HO_*/results/*_py8.lhe" -type f | sort | tail -1)
    fi
fi

if [[ -n "${RAW_LHE_FILE}" && -f "${RAW_LHE_FILE}" ]]; then
    LHE_FILE="${RAW_LHE_FILE}"
elif [[ -n "${PY8_LHE_FILE}" && -f "${PY8_LHE_FILE}" ]]; then
    LHE_FILE="${PY8_LHE_FILE}"
else
    if pool_uses_crystalball_addon "${POOL_NAME}"; then
        echo "[ERROR] CrystalBall addon output files:"
        find addon/pp_psiX_CrystalBall/output output -maxdepth 1 -type f -printf '  %p\n' 2>/dev/null || true
    fi
    echo "Error: LHE file not found"
    exit 1
fi

echo "Found LHE file: $LHE_FILE"

RAW_EVENT_COUNT=$(count_lhe_events "${LHE_FILE}")
echo "[INFO] 原始 LHE 事件数: ${RAW_EVENT_COUNT}"
if (( RAW_EVENT_COUNT <= 0 )); then
    echo "[ERROR] HELAC 生成的原始 LHE 不包含任何 <event>，终止上传空文件"
    exit 1
fi

# workbook_v2 要求在 LHE 生成后单独调用 converter 完成 PDG 转换。
FINAL_LHE_FILE="${LHE_FILE}"
CONVERTED_LHE_FILE="${WORKDIR}/sample_${POOL_NAME}_${MY_SEED}_converted.lhe"

if [[ -f "${PY8_ONIA_CONFIG}" ]] && build_lhe_converter_if_needed; then
    echo "[INFO] Running lhe_pythia6_pythia8 converter..."
    if run_logged "lhe_pythia6_pythia8" "${WORKDIR}/lhe_pythia6_pythia8" "${LHE_FILE}" "${PY8_ONIA_CONFIG}" "${CONVERTED_LHE_FILE}"; then
        if [[ -f "${CONVERTED_LHE_FILE}" ]]; then
            CONVERTED_EVENT_COUNT=$(count_lhe_events "${CONVERTED_LHE_FILE}")
            echo "[INFO] 转换后 LHE 事件数: ${CONVERTED_EVENT_COUNT}"
            if (( CONVERTED_EVENT_COUNT > 0 )); then
                FINAL_LHE_FILE="${CONVERTED_LHE_FILE}"
                echo "[INFO] Converted LHE ready: ${FINAL_LHE_FILE}"
            else
                echo "[WARN] 转换后 LHE 不包含任何 <event>，回退到原始 LHE"
            fi
        fi
    else
        echo "[WARN] LHE converter failed, fallback to original LHE"
    fi
elif [[ -n "${PY8_LHE_FILE}" && -f "${PY8_LHE_FILE}" ]]; then
    FINAL_LHE_FILE="${PY8_LHE_FILE}"
    echo "[INFO] Reusing HELAC generated *_py8.lhe: ${FINAL_LHE_FILE}"
else
    echo "[WARN] No standalone converter available, fallback to original LHE"
fi

FINAL_EVENT_COUNT=$(count_lhe_events "${FINAL_LHE_FILE}")
echo "[INFO] 最终待上传 LHE 事件数: ${FINAL_EVENT_COUNT}"
if (( FINAL_EVENT_COUNT <= 0 )); then
    echo "[ERROR] 最终 LHE 不包含任何 <event>，拒绝上传空文件"
    exit 1
fi

if [[ -f "${OCTET_PDG_TOOL}" ]] && grep -q "\<9900" "${FINAL_LHE_FILE}"; then
    echo "[INFO] Standalone converter 后仍检测到旧编码，使用统一工具补做转换..."
    PYTHONIOENCODING=UTF-8 python3 "${OCTET_PDG_TOOL}" convert-file "${FINAL_LHE_FILE}" --in-place >/dev/null
fi

verify_lhe_octet_codes "${FINAL_LHE_FILE}"

# ---- LHE Stratified Shuffle-Split ----
SHUFFLE_SPLIT_DIR="${WORKDIR}/lhe_blocks"
if bool_is_true "${LHE_SHUFFLE_SPLIT}"; then
    SHUFFLE_SPLIT_BINARY="${SCRIPT_DIR}/lhe_shuffle_split"
    mkdir -p "${SHUFFLE_SPLIT_DIR}"
    # Derive a deterministic shuffle seed from the generation seed with an offset
    # so shuffles are reproducible but distinct from HELAC's internal RNG.
    RESOLVED_SHUFFLE_SEED=$(( MY_SEED * 1000 + 37 ))
    msg_info "Running LHE shuffle-split (seed=${RESOLVED_SHUFFLE_SEED}, mode=${LHE_SHUFFLE_MODE})..."
    SHUFFLE_SPLIT_ARGS=(
        --input "${FINAL_LHE_FILE}"
        --output-dir "${SHUFFLE_SPLIT_DIR}"
        --seed "${RESOLVED_SHUFFLE_SEED}"
        --events-per-block "${LHE_EVENTS_PER_BLOCK}"
        --mode "${LHE_SHUFFLE_MODE}"
        --n-strata "${LHE_N_STRATA}"
        --filename-prefix "${MY_SEED}_"
        --write-provenance
    )
    if bool_is_true "${LHE_DROP_INCOMPLETE_LAST_BLOCK}"; then
        SHUFFLE_SPLIT_ARGS+=(--drop-incomplete-last-block)
    fi
    if ! run_logged "lhe_shuffle_split" "${SHUFFLE_SPLIT_BINARY}" "${SHUFFLE_SPLIT_ARGS[@]}"; then
        echo "[ERROR] LHE shuffle-split failed"
        exit 1
    fi
    BLOCK_COUNT=$(ls "${SHUFFLE_SPLIT_DIR}"/block_*.lhe 2>/dev/null | wc -l)
    if (( BLOCK_COUNT == 0 )); then
        echo "[ERROR] Shuffle-split produced no block files"
        exit 1
    fi
    msg_ok "Shuffle-split: ${BLOCK_COUNT} blocks in ${SHUFFLE_SPLIT_DIR}"
fi

# Optionally compress LHE output before stageout (atomic: write .tmp then rename).
STAGEOUT_FILE="${FINAL_LHE_FILE}"
LHE_EXT="lhe"
if bool_is_true "${COMPRESS_LHE}" && command -v gzip >/dev/null 2>&1; then
    LHE_EXT="lhe.gz"
    STAGEOUT_FILE="${WORKDIR}/sample_${POOL_NAME}_${MY_SEED}.lhe.gz"
    echo "[INFO] Compressing LHE output (gzip level=${LHE_COMPRESSION_LEVEL})..."
    gzip -${LHE_COMPRESSION_LEVEL} -c "${FINAL_LHE_FILE}" > "${STAGEOUT_FILE}.tmp" \
        && mv "${STAGEOUT_FILE}.tmp" "${STAGEOUT_FILE}" \
        || { echo "[ERROR] Failed to compress LHE file"; exit 1; }
    echo "[INFO] Compressed: ${STAGEOUT_FILE}"
fi

# Create output directory and stage out LHE.
if [ -n "${LOCAL_OUTPUT_BASE:-}" ]; then
    echo "[INFO] Using local stageout to ${LOCAL_OUTPUT_BASE}"
    LOCAL_DIR="${LOCAL_OUTPUT_BASE}/${OUTPUT_DIR}"
    mkdir -p "${LOCAL_DIR}" || {
        echo "[ERROR] Failed to create local directory: ${LOCAL_DIR}"
        exit 1
    }

    LOCAL_OUTPUT="${LOCAL_DIR}/sample_${POOL_NAME}_${MY_SEED}.${LHE_EXT}"
    echo "[INFO] Staging out LHE file to: ${LOCAL_OUTPUT}"
    if cp "${STAGEOUT_FILE}" "${LOCAL_OUTPUT}"; then
        echo "[INFO] Local stageout successful"
        echo "Output: ${LOCAL_OUTPUT}"
    else
        echo "[ERROR] Failed to stage out LHE file locally"
        exit 1
    fi
    # Stage out shuffle-split blocks if enabled
    if bool_is_true "${LHE_SHUFFLE_SPLIT}"; then
        BLOCK_LOCAL_DIR="${LOCAL_DIR}/lhe_blocks"
        mkdir -p "${BLOCK_LOCAL_DIR}"
        cp "${SHUFFLE_SPLIT_DIR}"/block_*.lhe "${SHUFFLE_SPLIT_DIR}"/shuffle_split_manifest.json "${BLOCK_LOCAL_DIR}/" 2>/dev/null || true
        msg_ok "Staged ${BLOCK_COUNT} shuffle-split blocks to ${BLOCK_LOCAL_DIR}"
    fi
elif [[ "${STAGEOUT_MODE}" == "helac-output" ]]; then
    stage_out_helac_output_archive "${RUN_DIR}"
    echo "HELAC output generation complete!"
elif [ "${SKIP_STAGEOUT:-0}" -eq 1 ]; then
    echo "[INFO] SKIP_STAGEOUT=1, skip XRootD stageout"
else
    # Create remote directory and copy to T2_CN_Beijing storage via XRootD
    echo "Creating remote directory on T2_CN_Beijing..."
    make_remote_dir "${OUTPUT_DIR}" || {
        echo "Warning: mkdir failed (directory may already exist)"
    }

    OUTPUT_FILE=$(remote_url_for_spec "$(join_remote_spec "${OUTPUT_DIR}" "sample_${POOL_NAME}_${MY_SEED}.${LHE_EXT}")")
    echo "Staging out LHE file to: ${OUTPUT_FILE}"
    run_logged "xrdcp_lhe_stageout" xrdcp --nopbar --force "${STAGEOUT_FILE}" "${OUTPUT_FILE}" || {
        echo "Error: Failed to stage out LHE file"
        exit 1
    }
    echo "LHE generation complete!"
    echo "Output: $OUTPUT_FILE"
    # Stage out shuffle-split blocks if enabled
    if bool_is_true "${LHE_SHUFFLE_SPLIT}"; then
        BLOCK_REMOTE_DIR="${OUTPUT_DIR}/lhe_blocks"
        make_remote_dir "${BLOCK_REMOTE_DIR}" || true
        for bf in "${SHUFFLE_SPLIT_DIR}"/block_*.lhe "${SHUFFLE_SPLIT_DIR}"/shuffle_split_manifest.json; do
            bname=$(basename "$bf")
            BLOCK_OUTPUT=$(remote_url_for_spec "$(join_remote_spec "${BLOCK_REMOTE_DIR}" "${bname}")")
            xrdcp --nopbar --force "$bf" "${BLOCK_OUTPUT}" || \
                echo "[WARN] Failed to stage block: ${bname}" >&2
        done
        msg_ok "Staged ${BLOCK_COUNT} shuffle-split blocks to ${BLOCK_REMOTE_DIR}"
    fi
fi
echo "=============================================="

# Return to work directory
cd "$WORKDIR"

# Cleanup (optional, saves disk space on worker)
# rm -rf HELAC-Onia-2.7.6 HepMC
