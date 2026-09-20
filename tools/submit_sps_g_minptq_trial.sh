#!/bin/bash
# SPS double-J/psi NLO* HELAC-only minptq trial.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/paths.sh"

POOL="pool_2jpsi_g"
SEED=91051
MIN_PT_Q=0.0
HELAC_DIAGNOSTICS_MODE="legacy"
TRIAL_NMC=(5000000 5000000 5000000 10000000 20000000 1000000 10000000 20000000)
TRIAL_UNWEVT=(100000 200000 500000 100000 100000 100000 200000 400000)
TRIAL_LABELS=(
    "nmc5e6_unwevt1e5"
    "nmc5e6_unwevt2e5"
    "nmc5e6_unwevt5e5"
    "nmc1e7_unwevt1e5"
    "nmc2e7_unwevt1e5"
    "nmc1e6_unwevt1e5"
    "nmc1e7_unwevt2e5"
    "nmc2e7_unwevt4e5"
)
OUTPUT_BASE="root://eosuser.cern.ch///eos/user/c/chiw/JpsiJpsiPhi_LHE_trials/SPS_G_minptq_0p0_nmc_unwevt"
BASELINE_OUTPUT="root://eosuser.cern.ch///eos/user/c/chiw/JpsiJpsiPhi_LHE_trials/SPS_G_minptq/minptq_0p0"
OUTPUT_DIR="${WORKSPACE_ROOT}/generated/SPS_G_minptq_trial_$(date +%Y%m%d_%H%M%S)"
DO_SUBMIT=0
DO_WAIT=0
REUSE_BASELINE=1

msg_info() { printf '[INFO] %s\n' "$1"; }
msg_warn() { printf '[WARN] %s\n' "$1" >&2; }
msg_error() { printf '[ERROR] %s\n' "$1" >&2; }

usage() {
    cat <<EOF
Usage: $0 [--submit] [--wait] [--output-dir DIR] [--output-base URL] [--seed N]
          [--helac-diagnostics MODE] [--rerun-baseline]

Prepare the minptq=0.0 comparison matrix:
  (nmc, unwevt) =
  (5e6, 1e5), (5e6, 2e5), (5e6, 5e5), (1e7, 1e5),
  (2e7, 1e5), (1e6, 1e5), (1e7, 2e5), (2e7, 4e5)

The default is preparation only. --wait implies --submit.
--rerun-baseline submits the (5e6, 1e5) point into the new output base
instead of reusing the earlier baseline output.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --submit)
            DO_SUBMIT=1
            shift
            ;;
        --wait)
            DO_WAIT=1
            DO_SUBMIT=1
            shift
            ;;
        --output-dir)
            [[ $# -ge 2 ]] || { msg_error "--output-dir requires a value"; exit 64; }
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --output-base)
            [[ $# -ge 2 ]] || { msg_error "--output-base requires a value"; exit 64; }
            OUTPUT_BASE="${2%/}"
            shift 2
            ;;
        --seed)
            [[ $# -ge 2 ]] || { msg_error "--seed requires a value"; exit 64; }
            SEED="$2"
            shift 2
            ;;
        --helac-diagnostics)
            [[ $# -ge 2 ]] || { msg_error "--helac-diagnostics requires a value"; exit 64; }
            HELAC_DIAGNOSTICS_MODE="$2"
            shift 2
            ;;
        --rerun-baseline)
            REUSE_BASELINE=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            msg_error "Unknown option: $1"
            usage >&2
            exit 64
            ;;
    esac
done

case "${HELAC_DIAGNOSTICS_MODE}" in
    legacy|none|slim|full)
        ;;
    *)
        msg_error "--helac-diagnostics must be one of legacy, none, slim, full"
        exit 64
        ;;
esac

if ! [[ "${SEED}" =~ ^[0-9]+$ ]] || (( SEED <= 10 || SEED >= 100000 )); then
    msg_error "Seed must be an integer between 11 and 99999"
    exit 64
fi
if [[ "${OUTPUT_BASE}" != root://* ]]; then
    msg_error "--output-base must be an XRootD URL beginning with root://"
    exit 64
fi

mkdir -p "${OUTPUT_DIR}/logs" "${OUTPUT_DIR}/node_configs/lhe_generation"
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"
LOG_ROOT="${OUTPUT_DIR}/logs"
CONFIG_DIR="${OUTPUT_DIR}/node_configs/lhe_generation"

if ! X509_USER_PROXY="$(resolve_proxy_path)"; then
    msg_error "No usable X509 proxy found"
    exit 66
fi
export X509_USER_PROXY

RUNTIME_BUILD_LOG="${OUTPUT_DIR}/prepare_runtime.log"
msg_info "Preparing HELAC runtime and proxy bundles in ${OUTPUT_DIR}"
python3 "${WORKSPACE_ROOT}/dag_generator.py" prepare-runtime \
    --output-dir "${OUTPUT_DIR}" \
    --proxy-path "${X509_USER_PROXY}" \
    >"${RUNTIME_BUILD_LOG}" 2>&1

LHE_BUNDLE_PATH="${OUTPUT_DIR}/lhe_runtime_bundle.tar.gz"
PROXY_BUNDLE_PATH="${OUTPUT_DIR}/proxy_bundle.tar.gz"
LHE_WRAPPER_PATH="${WORKSPACE_ROOT}/lhe_generation/condor_wrappers/run_lhe_gen.sh"
SUBMIT_TEMPLATE="${WORKSPACE_ROOT}/processing/templates/lhe_gen_trial.sub"

for required_file in "${LHE_BUNDLE_PATH}" "${PROXY_BUNDLE_PATH}" \
    "${LHE_WRAPPER_PATH}" "${SUBMIT_TEMPLATE}"; do
    if [[ ! -f "${required_file}" ]]; then
        msg_error "Required trial asset is missing: ${required_file}"
        exit 66
    fi
done

write_config() {
    local index="$1"
    local label="${TRIAL_LABELS[${index}]}"
    local config_path="${CONFIG_DIR}/LHE_SPS_G_${label}.json"
    local output_dir="${OUTPUT_BASE}/${label}"
    if (( index == 0 && REUSE_BASELINE == 1 )); then
        output_dir="${BASELINE_OUTPUT}"
    fi

    python3 - "${config_path}" "${output_dir}" "${TRIAL_NMC[${index}]}" \
        "${TRIAL_UNWEVT[${index}]}" "${SEED}" "${HELAC_DIAGNOSTICS_MODE}" <<'PY'
import json
import sys

config_path, output_dir, nmc, unwevt, seed, helac_diagnostics = sys.argv[1:]
payload = {
    "pool": "pool_2jpsi_g",
    "seed": int(seed),
    "min_pt_conia": 6.0,
    "min_pt_bonia": 4.0,
    "min_pt_q": 0.0,
    "nmc": int(nmc),
    "unwevt": int(unwevt),
    "helac_diagnostics": helac_diagnostics,
    "test_mode": False,
    "local_output_base": "",
    "compress_lhe": False,
    "lhe_shuffle_split": False,
    "output_dir": output_dir,
    "storage": {},
}
with open(config_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
    printf '%s\n' "${config_path}"
}

declare -a CONFIG_PATHS=()
declare -a LABELS=()
for index in "${!TRIAL_NMC[@]}"; do
    CONFIG_PATHS+=("$(write_config "${index}")")
    LABELS+=("${TRIAL_LABELS[${index}]}")
done

msg_info "Prepared ${#CONFIG_PATHS[@]} HELAC configurations"
for index in "${!TRIAL_NMC[@]}"; do
    output_dir="${OUTPUT_BASE}/${LABELS[${index}]}"
    if (( index == 0 && REUSE_BASELINE == 1 )); then
        output_dir="${BASELINE_OUTPUT}"
    fi
    printf '  nmc=%s unwevt=%s minptq=%s seed=%s output=%s\n' \
        "${TRIAL_NMC[${index}]}" "${TRIAL_UNWEVT[${index}]}" \
        "${MIN_PT_Q}" "${SEED}" "${output_dir}"
done

if (( DO_SUBMIT == 0 )); then
    msg_warn "Preparation only; use --submit to submit these jobs"
    msg_info "Configs: ${CONFIG_DIR}"
    exit 0
fi

declare -a SUBMITTED_CLUSTERS=()
submit_one() {
    local index="$1"
    local nmc="${TRIAL_NMC[${index}]}"
    local unwevt="${TRIAL_UNWEVT[${index}]}"
    local label="${LABELS[${index}]}"
    local config_path="${CONFIG_PATHS[${index}]}"
    local job_flavour="tomorrow"
    local submit_output=""
    local cluster_id=""

    if (( index == 7 )); then
        job_flavour="testmatch"
    fi

    submit_output=$(condor_submit "${SUBMIT_TEMPLATE}" \
        -append "lhe_wrapper_path = ${LHE_WRAPPER_PATH}" \
        -append "lhe_bundle_path = ${LHE_BUNDLE_PATH}" \
        -append "lhe_bundle_name = lhe_runtime_bundle.tar.gz" \
        -append "proxy_bundle_path = ${PROXY_BUNDLE_PATH}" \
        -append "proxy_bundle_name = proxy_bundle.tar.gz" \
        -append "config_path = ${config_path}" \
        -append "config_name = $(basename "${config_path}")" \
        -append "pool = ${POOL}" \
        -append "min_pt_q_label = ${label}" \
        -append "log_root = ${LOG_ROOT}" \
        -append "job_flavour = ${job_flavour}")
    printf '%s\n' "${submit_output}"
    cluster_id=$(printf '%s\n' "${submit_output}" | sed -n \
        's/.*cluster \([0-9][0-9]*\).*/\1/p' | tail -1)
    if [[ -z "${cluster_id}" ]]; then
        msg_error "Could not parse cluster ID for nmc=${nmc}, unwevt=${unwevt}"
        exit 1
    fi
    SUBMITTED_CLUSTERS+=("${cluster_id}")
    msg_info "Submitted nmc=${nmc}, unwevt=${unwevt}, JobFlavour=${job_flavour}, cluster=${cluster_id}"
}

for index in "${!TRIAL_NMC[@]}"; do
    if (( index == 0 && REUSE_BASELINE == 1 )); then
        msg_info "Reusing completed baseline nmc=5000000, unwevt=100000"
        continue
    fi
    submit_one "${index}"
done

if (( DO_WAIT == 0 )); then
    msg_warn "Submitted without waiting; monitor the recorded clusters"
    exit 0
fi

wait_one_cluster() {
    local cluster_id="$1"
    local queue_snapshot=""
    while true; do
        if condor_q "${cluster_id}" -hold -autoformat ClusterId ProcId HoldReason \
            2>/dev/null | grep -q .; then
            msg_error "Cluster ${cluster_id} entered hold"
            condor_q "${cluster_id}" -hold \
                -autoformat ClusterId ProcId HoldReason 2>/dev/null || true
            return 1
        fi
        queue_snapshot="$(condor_q "${cluster_id}" -autoformat ClusterId 2>/dev/null || true)"
        [[ -z "${queue_snapshot}" ]] && break
        sleep 60
    done
    msg_info "Cluster ${cluster_id} left the queue"
}

for cluster_id in "${SUBMITTED_CLUSTERS[@]}"; do
    wait_one_cluster "${cluster_id}"
done

msg_info "All HELAC trial jobs completed; inspect ${LOG_ROOT} and ${OUTPUT_BASE}"
