#!/bin/bash
# ==============================================================================
# run_all_tests.sh - 重构后测试总入口
# ==============================================================================
# 默认执行：
#   1. 静态环境校验
#   2. 生成 JJP_DPS2_CS + JJP_DPS2_G + JUP_DPS1 的测试 DAG
# 可选：
#   3. 直接提交 DAG 到 HTCondor
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

DO_SUBMIT=0
DO_WAIT=0
JOBS=1
MAX_EVENTS=5
SCAN_EXISTING=1
ENABLE_NTUPLE=0
CMSSW15_RUNTIME_TARBALL=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --submit)
            DO_SUBMIT=1
            shift
            ;;
        --wait)
            DO_WAIT=1
            shift
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --max-events)
            MAX_EVENTS="$2"
            shift 2
            ;;
        --no-scan-existing)
            SCAN_EXISTING=0
            shift
            ;;
        --enable-ntuple)
            ENABLE_NTUPLE=1
            shift
            ;;
        --cmssw15-runtime-tarball)
            CMSSW15_RUNTIME_TARBALL="$2"
            shift 2
            ;;
        -h|--help)
            cat << EOF
用法: $0 [--submit] [--wait] [--jobs N] [--max-events N] [--no-scan-existing] [--enable-ntuple] [--cmssw15-runtime-tarball PATH]
EOF
            exit 0
            ;;
        *)
            echo "未知参数: $1" >&2
            exit 1
            ;;
    esac
done

CMD=(
    "${SCRIPT_DIR}/submit_tests.sh"
    --jobs "${JOBS}"
    --max-events "${MAX_EVENTS}"
)

if [[ ${DO_SUBMIT} -eq 1 ]]; then
    CMD+=(--submit)
fi

if [[ ${DO_WAIT} -eq 1 ]]; then
    CMD+=(--wait)
fi

if [[ ${SCAN_EXISTING} -eq 0 ]]; then
    CMD+=(--no-scan-existing)
fi

if [[ ${ENABLE_NTUPLE} -eq 1 ]]; then
    CMD+=(--enable-ntuple)
fi

if [[ -n "${CMSSW15_RUNTIME_TARBALL}" ]]; then
    CMD+=(--cmssw15-runtime-tarball "${CMSSW15_RUNTIME_TARBALL}")
fi

echo "[INFO] 执行测试入口: ${CMD[*]}"
echo "[INFO] 先执行八重态 PDG 映射自检"
"${SCRIPT_DIR}/test_octet_pdg_tool.sh"

echo "[INFO] 检查 GEN-SIM 顶点涂抹配置"
"${BASE_DIR}/tools/check_gensim_vtxsmeared_config.py"

echo "[INFO] 检查 block coordinator 的 TPS 重复输入与 SPS SubDAG"
python3 "${SCRIPT_DIR}/test_coordinate_lhe_blocks.py"

echo "[INFO] 检查 DAGMan 提交批次配置"
python3 "${SCRIPT_DIR}/test_dagman_config.py"

"${CMD[@]}"
