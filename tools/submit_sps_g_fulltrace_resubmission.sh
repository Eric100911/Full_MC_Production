#!/usr/bin/env bash
# Submit the fixed-bundle full-trace HELAC resubmission set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/paths.sh"

POOL="pool_2jpsi_g"
SEED=91051
OUTPUT_BASE="root://eosuser.cern.ch///eos/user/c/chiw/JpsiJpsiPhi_LHE_trials/SPS_G_fulltrace_fixed_archive"
OUTPUT_DIR="${WORKSPACE_ROOT}/generated/SPS_G_fulltrace_fixed_archive_$(date +%Y%m%d_%H%M%S)"
DO_SUBMIT=0

MIN_PT_Q_VALUES=(0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.8)
NMC_VALUES=(5000000 5000000 5000000 10000000 20000000 1000000 10000000 20000000 20000000)
UNWEVT_VALUES=(100000 200000 500000 100000 100000 100000 200000 400000 100000)
LABELS=(
    nmc5e6_unwevt1e5
    nmc5e6_unwevt2e5
    nmc5e6_unwevt5e5
    nmc1e7_unwevt1e5
    nmc2e7_unwevt1e5
    nmc1e6_unwevt1e5
    nmc1e7_unwevt2e5
    nmc2e7_unwevt4e5
    minptq_0p8_nmc2e7_unwevt1e5
)
JOB_FLAVOURS=(tomorrow tomorrow tomorrow tomorrow testmatch tomorrow tomorrow testmatch testmatch)

msg_info() { printf '[INFO] %s\n' "$1"; }
msg_warn() { printf '[WARN] %s\n' "$1" >&2; }
msg_error() { printf '[ERROR] %s\n' "$1" >&2; }

usage() {
    cat <<EOF
Usage: $0 [--submit] [--output-dir DIR] [--output-base URL] [--seed N]

Prepare or submit the fixed-bundle full-trace HELAC resubmission:
  - eight minptq=0.0 matrix points;
  - supplementary minptq=0.8, nmc=20000000, unwevt=100000;
  - testmatch for all nmc=20000000 points.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --submit)
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
msg_info "Preparing fixed HELAC runtime and proxy bundles in ${OUTPUT_DIR}"
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

declare -a CONFIG_PATHS=()
for index in "${!LABELS[@]}"; do
    config_path="${CONFIG_DIR}/LHE_SPS_G_${LABELS[${index}]}.json"
    output_dir="${OUTPUT_BASE}/${LABELS[${index}]}"
    python3 - "${config_path}" "${output_dir}" "${MIN_PT_Q_VALUES[${index}]}" \
        "${NMC_VALUES[${index}]}" "${UNWEVT_VALUES[${index}]}" "${SEED}" <<'PY'
import json
import sys

config_path, output_dir, min_pt_q, nmc, unwevt, seed = sys.argv[1:]
payload = {
    "pool": "pool_2jpsi_g",
    "seed": int(seed),
    "min_pt_conia": 6.0,
    "min_pt_bonia": 4.0,
    "min_pt_q": float(min_pt_q),
    "nmc": int(nmc),
    "unwevt": int(unwevt),
    "helac_diagnostics": "full",
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
    CONFIG_PATHS+=("${config_path}")
done

msg_info "Prepared ${#CONFIG_PATHS[@]} fixed-bundle full-trace configurations"
for index in "${!LABELS[@]}"; do
    printf '  minptq=%s nmc=%s unwevt=%s flavour=%s output=%s\n' \
        "${MIN_PT_Q_VALUES[${index}]}" "${NMC_VALUES[${index}]}" \
        "${UNWEVT_VALUES[${index}]}" "${JOB_FLAVOURS[${index}]}" \
        "${OUTPUT_BASE}/${LABELS[${index}]}"
done

if (( DO_SUBMIT == 0 )); then
    msg_warn "Preparation only; use --submit to submit these jobs"
    msg_info "Configs: ${CONFIG_DIR}"
    exit 0
fi

CLUSTER_RECORD="${OUTPUT_DIR}/submitted_clusters.txt"
: >"${CLUSTER_RECORD}"
for index in "${!CONFIG_PATHS[@]}"; do
    config_path="${CONFIG_PATHS[${index}]}"
    label="${LABELS[${index}]}"
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
        -append "job_flavour = ${JOB_FLAVOURS[${index}]}")
    printf '%s\n' "${submit_output}"
    cluster_id=$(printf '%s\n' "${submit_output}" | sed -n \
        's/.*cluster \([0-9][0-9]*\).*/\1/p' | tail -1)
    if [[ -z "${cluster_id}" ]]; then
        msg_error "Could not parse cluster ID for ${label}"
        exit 1
    fi
    printf '%s\t%s\t%s\t%s\n' "${cluster_id}" "${label}" \
        "${MIN_PT_Q_VALUES[${index}]}" "${JOB_FLAVOURS[${index}]}" \
        >>"${CLUSTER_RECORD}"
    msg_info "Submitted minptq=${MIN_PT_Q_VALUES[${index}]}, nmc=${NMC_VALUES[${index}]}, unwevt=${UNWEVT_VALUES[${index}]}, flavour=${JOB_FLAVOURS[${index}]}, cluster=${cluster_id}"
done

msg_info "Cluster records: ${CLUSTER_RECORD}"
msg_info "Logs: ${LOG_ROOT}"
