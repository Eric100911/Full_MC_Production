#!/bin/bash
# ==============================================================================
# submit_lhe_matrix.sh - workbook_v2 LHE 小批量矩阵测试
# ==============================================================================
# 覆盖所有真实生成池：
#   pool_jpsi_CSCO_g
#   pool_upsilon_CSCO_g
#   pool_gg
#   pool_2jpsi_cs
#   pool_2jpsi_g
#   pool_jpsi_upsilon_CSCO
#
# 设计目标：
# 1. 每个 pool 在 HTCondor 上提交 1 个 fast-test LHE 生成作业。
# 2. 生成完成后把远端 LHE 拉回本地临时目录，用统一工具检查八重态编码。
# 3. worker 侧严格使用 bundle + proxy bundle 解压运行，不依赖 AFS 运行时读取。
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/paths.sh"
LOG_DIR="${SCRIPT_DIR}/log"

mkdir -p "${LOG_DIR}"

msg_info() { printf '[INFO] %s\n' "$1"; }
msg_warn() { printf '[WARN] %s\n' "$1"; }
msg_error() { printf '[ERROR] %s\n' "$1" >&2; }

OUTPUT_DIR="${WORKSPACE_ROOT}/tests/generated/lhe_matrix_$(date +%Y%m%d_%H%M%S)"
DO_SUBMIT=0
DO_WAIT=0

POOLS=(
    "pool_jpsi_CSCO_g"
    "pool_upsilon_CSCO_g"
    "pool_gg"
    "pool_2jpsi_cs"
    "pool_2jpsi_g"
    "pool_jpsi_upsilon_CSCO"
)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --submit)
            DO_SUBMIT=1
            shift
            ;;
        --wait)
            DO_WAIT=1
            shift
            ;;
        -h|--help)
            cat << EOF
用法: $0 [--submit] [--wait] [--output-dir DIR]

默认行为：
  1. 生成本轮 worker runtime bundle 与 proxy bundle
  2. 打印每个 LHE pool 的提交计划

加 --submit 后：
  - 逐个提交所有 LHE pool 的 fast-test 作业

加 --wait 后：
  - 等待所有 cluster 离开队列
  - 下载输出 LHE 并检查是否仍残留旧的 9900xxxx 编码
EOF
            exit 0
            ;;
        *)
            msg_error "未知参数: $1"
            exit 1
            ;;
    esac
done

mkdir -p "${OUTPUT_DIR}"

if ! X509_USER_PROXY="$(resolve_proxy_path)"; then
    msg_error "找不到可用代理文件，请先运行 ./check_proxy.sh --init"
    exit 1
fi
export X509_USER_PROXY
msg_info "使用本地代理: ${X509_USER_PROXY}"

msg_info "执行八重态 PDG 映射自检 ..."
"${SCRIPT_DIR}/test_octet_pdg_tool.sh"

msg_info "准备 worker runtime bundle ..."
RUNTIME_JSON="${OUTPUT_DIR}/runtime_assets.json"
python3 "${WORKSPACE_ROOT}/dag_generator.py" prepare-runtime --output-dir "${OUTPUT_DIR}" > "${RUNTIME_JSON}"

LHE_BUNDLE_PATH=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["lhe_bundle_path"])' "${RUNTIME_JSON}")
LHE_BUNDLE_NAME=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["lhe_bundle_name"])' "${RUNTIME_JSON}")
PROXY_BUNDLE_PATH=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["proxy_bundle_path"])' "${RUNTIME_JSON}")
PROXY_BUNDLE_NAME=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["proxy_bundle_name"])' "${RUNTIME_JSON}")

declare -A SEEDS=(
    ["pool_jpsi_CSCO_g"]=91011
    ["pool_upsilon_CSCO_g"]=91021
    ["pool_gg"]=91031
    ["pool_2jpsi_cs"]=91041
    ["pool_2jpsi_g"]=91051
    ["pool_jpsi_upsilon_CSCO"]=91061
)

declare -A MIN_PT_CONIA=(
    ["pool_jpsi_CSCO_g"]=6.0
    ["pool_upsilon_CSCO_g"]=6.0
    ["pool_gg"]=0.0
    ["pool_2jpsi_cs"]=6.0
    ["pool_2jpsi_g"]=6.0
    ["pool_jpsi_upsilon_CSCO"]=6.0
)

declare -A MIN_PT_BONIA=(
    ["pool_jpsi_CSCO_g"]=4.0
    ["pool_upsilon_CSCO_g"]=4.0
    ["pool_gg"]=0.0
    ["pool_2jpsi_cs"]=4.0
    ["pool_2jpsi_g"]=4.0
    ["pool_jpsi_upsilon_CSCO"]=4.0
)

declare -A MIN_PT_Q=(
    ["pool_jpsi_CSCO_g"]=0.0
    ["pool_upsilon_CSCO_g"]=0.0
    ["pool_gg"]=4.0
    ["pool_2jpsi_cs"]=0.0
    ["pool_2jpsi_g"]=0.0
    ["pool_jpsi_upsilon_CSCO"]=0.0
)

echo ""
echo "=============================================="
echo "LHE 小批量矩阵测试计划"
echo "=============================================="
for pool in "${POOLS[@]}"; do
    echo "Pool: ${pool}  Seed: ${SEEDS[${pool}]}"
done
echo "Runtime bundle: ${LHE_BUNDLE_PATH}"
echo "Proxy bundle:   ${PROXY_BUNDLE_PATH}"
echo "=============================================="
echo ""

if [[ ${DO_SUBMIT} -eq 0 ]]; then
    msg_warn "当前为仅准备模式；如需真正提交，请加 --submit"
    exit 0
fi

case "${OUTPUT_DIR}" in
    /tmp/*|/var/tmp/*)
        msg_error "提交模式下不能把 bundle 放在 ${OUTPUT_DIR}；schedd 无法从 submit host 的本地 /tmp 读取这些文件。请改用 AFS 工作区路径。"
        exit 1
        ;;
esac

declare -a SUBMITTED_POOLS=()
declare -a SUBMITTED_SEEDS=()
declare -a SUBMITTED_CLUSTERS=()

submit_one() {
    local pool="$1"
    local seed="$2"
    local submit_output=""
    local cluster_id=""

    submit_output=$(condor_submit "${WORKSPACE_ROOT}/processing/templates/lhe_gen_test.sub" \
        -append "pool = ${pool}" \
        -append "seed = ${seed}" \
        -append "min_pt_conia = ${MIN_PT_CONIA[${pool}]}" \
        -append "min_pt_bonia = ${MIN_PT_BONIA[${pool}]}" \
        -append "min_pt_q = ${MIN_PT_Q[${pool}]}" \
        -append "lhe_bundle_path = ${LHE_BUNDLE_PATH}" \
        -append "lhe_bundle_name = ${LHE_BUNDLE_NAME}" \
        -append "proxy_bundle_path = ${PROXY_BUNDLE_PATH}" \
        -append "proxy_bundle_name = ${PROXY_BUNDLE_NAME}")

    printf '%s\n' "${submit_output}"
    cluster_id=$(printf '%s\n' "${submit_output}" | sed -n 's/.*cluster \([0-9][0-9]*\).*/\1/p' | tail -1)
    if [[ -z "${cluster_id}" ]]; then
        msg_error "无法从 condor_submit 输出解析 cluster id: ${pool}"
        exit 1
    fi

    SUBMITTED_POOLS+=("${pool}")
    SUBMITTED_SEEDS+=("${seed}")
    SUBMITTED_CLUSTERS+=("${cluster_id}")
    msg_info "已提交 ${pool}, cluster=${cluster_id}, seed=${seed}"
}

for pool in "${POOLS[@]}"; do
    submit_one "${pool}" "${SEEDS[${pool}]}"
done

if [[ ${DO_WAIT} -eq 0 ]]; then
    msg_warn "已提交但未等待；如需等待并校验输出，请加 --wait"
    exit 0
fi

wait_one_cluster() {
    local cluster_id="$1"
    local queue_snapshot=""
    local hold_snapshot=""
    while true; do
        hold_snapshot=$(condor_q "${cluster_id}" -hold -autoformat ClusterId ProcId HoldReason 2>/dev/null || true)
        if [[ -n "${hold_snapshot}" ]]; then
            msg_error "cluster ${cluster_id} 进入 hold:"
            printf '%s\n' "${hold_snapshot}" >&2
            return 1
        fi
        queue_snapshot=$(condor_q "${cluster_id}" -autoformat ClusterId 2>/dev/null || true)
        if [[ -z "${queue_snapshot}" ]]; then
            break
        fi
        sleep 30
    done
}

for cluster_id in "${SUBMITTED_CLUSTERS[@]}"; do
    msg_info "等待 cluster ${cluster_id} 离开队列 ..."
    wait_one_cluster "${cluster_id}" || exit 1
done

for idx in "${!SUBMITTED_POOLS[@]}"; do
    pool="${SUBMITTED_POOLS[$idx]}"
    seed="${SUBMITTED_SEEDS[$idx]}"
    cluster_id="${SUBMITTED_CLUSTERS[$idx]}"
    log_file="${WORKSPACE_ROOT}/log/lhe_${pool}_${seed}_${cluster_id}.log"
    remote_url="root://cceos.ihep.ac.cn//eos/ihep/cms/store/user/xcheng/MC_Production_v3/lhe_pools/${pool}/sample_${pool}_${seed}.lhe"
    local_copy="/tmp/${USER}/${pool}_${seed}.lhe"

    if [[ ! -f "${log_file}" ]]; then
        msg_error "缺少 condor 事件日志: ${log_file}"
        exit 1
    fi
    if ! rg -q "return value 0" "${log_file}"; then
        msg_error "作业未正常退出: ${log_file}"
        exit 1
    fi

    msg_info "下载并检查 ${remote_url}"
    xrdcp --nopbar --force "${remote_url}" "${local_copy}" >/dev/null
    python3 "${WORKSPACE_ROOT}/common/octet_pdg.py" scan "${local_copy}" --fail-on-legacy
done

msg_info "LHE 小批量矩阵测试完成"
