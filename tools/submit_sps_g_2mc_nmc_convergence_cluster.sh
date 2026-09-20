#!/usr/bin/env bash
# Submit the first-stage 2m_c-cut convergence scan as one Condor cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/paths.sh"

POOL="pool_2jpsi_g"
SEED=91051
UNWEVT=100000
MIN_M_QQP=3.0969
MIN_M_Q_BEAM=3.0969
OUTPUT_BASE="root://eosuser.cern.ch///eos/user/c/chiw/JpsiJpsiPhi_LHE_trials/SPS_G_2mc_nmc_convergence_minptq_0p0_unwevt1e5_fulltrace"
OUTPUT_DIR="${WORKSPACE_ROOT}/generated/SPS_G_2mc_nmc_convergence_minptq_0p0_unwevt1e5_fulltrace_$(date +%Y%m%d_%H%M%S)"
DO_SUBMIT=0
NMC_VALUES=(500000 2000000 5000000 10000000)
LABELS=(nmc0p5e6 nmc2e6 nmc5e6 nmc1e7)
JOB_FLAVOURS=(tomorrow tomorrow tomorrow testmatch)

usage() {
    cat <<EOF
Usage: $0 [--submit] [--output-dir DIR] [--output-base URL] [--seed N]

Prepare or submit one four-process Condor cluster:
  nmc=(500000, 2000000, 5000000, 10000000), unwevt=100000,
  minptq=0, minmqqp=minmqbeam=3.0969 GeV.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --submit) DO_SUBMIT=1; shift ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        --output-base) OUTPUT_BASE="${2%/}"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 64 ;;
    esac
done

if ! [[ "${SEED}" =~ ^[0-9]+$ ]] || (( SEED <= 10 || SEED >= 100000 )); then
    echo "Seed must be an integer between 11 and 99999" >&2
    exit 64
fi
if [[ "${OUTPUT_BASE}" != root://* ]]; then
    echo "--output-base must be an XRootD URL" >&2
    exit 64
fi

mkdir -p "${OUTPUT_DIR}/logs" "${OUTPUT_DIR}/node_configs/lhe_generation"
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"
CONFIG_DIR="${OUTPUT_DIR}/node_configs/lhe_generation"
LOG_ROOT="${OUTPUT_DIR}/logs"
if ! X509_USER_PROXY="$(resolve_proxy_path)"; then
    echo "No usable X509 proxy found" >&2
    exit 66
fi
export X509_USER_PROXY

python3 "${WORKSPACE_ROOT}/dag_generator.py" prepare-runtime \
    --output-dir "${OUTPUT_DIR}" --proxy-path "${X509_USER_PROXY}" \
    >"${OUTPUT_DIR}/prepare_runtime.log" 2>&1

LHE_BUNDLE_PATH="${OUTPUT_DIR}/lhe_runtime_bundle.tar.gz"
PROXY_BUNDLE_PATH="${OUTPUT_DIR}/proxy_bundle.tar.gz"
LHE_WRAPPER_PATH="${WORKSPACE_ROOT}/lhe_generation/condor_wrappers/run_lhe_gen.sh"
SUBMIT_TEMPLATE="${WORKSPACE_ROOT}/processing/templates/lhe_gen_trial.sub"
SUBMIT_FILE="${OUTPUT_DIR}/lhe_convergence_cluster.sub"
ITEMDATA_FILE="${OUTPUT_DIR}/lhe_convergence_cluster.itemdata"
for required_file in "${LHE_BUNDLE_PATH}" "${PROXY_BUNDLE_PATH}" "${LHE_WRAPPER_PATH}" "${SUBMIT_TEMPLATE}"; do
    [[ -f "${required_file}" ]] || { echo "Missing: ${required_file}" >&2; exit 66; }
done

: >"${ITEMDATA_FILE}"
for index in "${!NMC_VALUES[@]}"; do
    label="${LABELS[${index}]}_unwevt1e5"
    config_path="${CONFIG_DIR}/LHE_SPS_G_${label}.json"
    output_dir="${OUTPUT_BASE}/${label}"
    python3 - "${config_path}" "${output_dir}" "${NMC_VALUES[${index}]}" "${UNWEVT}" "${SEED}" <<'PY'
import json
import sys
config_path, output_dir, nmc, unwevt, seed = sys.argv[1:]
payload = {
    "pool": "pool_2jpsi_g", "seed": int(seed),
    "min_pt_conia": 6.0, "min_pt_bonia": 4.0, "min_pt_q": 0.0,
    "min_m_qqp": 3.0969, "min_m_q_beam": 3.0969,
    "nmc": int(nmc), "unwevt": int(unwevt),
    "helac_diagnostics": "full", "test_mode": False,
    "local_output_base": "", "compress_lhe": False,
    "lhe_shuffle_split": False, "output_dir": output_dir, "storage": {},
}
with open(config_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
    printf '%s\t%s\t%s\t%s\n' "${config_path}" "$(basename "${config_path}")" \
        "${label}" "${JOB_FLAVOURS[${index}]}" >>"${ITEMDATA_FILE}"
done

# Remove the template's Queue 1 and append one queue item-data table.  The
# per-row config_path and job_flavour macros produce four processes in one cluster.
sed '/^[[:space:]]*[Qq]ueue[[:space:]]/d' "${SUBMIT_TEMPLATE}" >"${SUBMIT_FILE}"
{
    echo "lhe_wrapper_path = ${LHE_WRAPPER_PATH}"
    echo "lhe_bundle_path = ${LHE_BUNDLE_PATH}"
    echo "lhe_bundle_name = lhe_runtime_bundle.tar.gz"
    echo "proxy_bundle_path = ${PROXY_BUNDLE_PATH}"
    echo "proxy_bundle_name = proxy_bundle.tar.gz"
    echo "pool = ${POOL}"
    echo "log_root = ${LOG_ROOT}"
    echo "queue config_path,config_name,min_pt_q_label,job_flavour from ${ITEMDATA_FILE}"
} >>"${SUBMIT_FILE}"

echo "Prepared four one-cluster configurations:"
for index in "${!NMC_VALUES[@]}"; do
    printf '  nmc=%s unwevt=%s minptq=0 minmqqp=minmqbeam=3.0969 flavour=%s\n' \
        "${NMC_VALUES[${index}]}" "${UNWEVT}" "${JOB_FLAVOURS[${index}]}"
done
if (( DO_SUBMIT == 0 )); then
    echo "Preparation only; submit file: ${SUBMIT_FILE}" >&2
    exit 0
fi

submit_output="$(condor_submit "${SUBMIT_FILE}")"
printf '%s\n' "${submit_output}"
cluster_id="$(printf '%s\n' "${submit_output}" | sed -n 's/.*cluster \([0-9][0-9]*\).*/\1/p' | tail -1)"
[[ -n "${cluster_id}" ]] || { echo "Could not parse Condor cluster ID" >&2; exit 1; }
for index in "${!NMC_VALUES[@]}"; do
    printf '%s.%s\t%s\t%s\t0.0\t3.0969\t3.0969\t%s\n' "${cluster_id}" "${index}" \
        "${NMC_VALUES[${index}]}" "${UNWEVT}" "${JOB_FLAVOURS[${index}]}" \
        >>"${OUTPUT_DIR}/submitted_clusters.txt"
done
echo "Submitted one cluster: ${cluster_id}.0-${cluster_id}.3"
echo "Records: ${OUTPUT_DIR}/submitted_clusters.txt"
