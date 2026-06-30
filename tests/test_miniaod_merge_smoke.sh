#!/bin/bash
# ==============================================================================
# test_miniaod_merge_smoke.sh - CMSSW/XRootD smoke test for run_miniaod_merge.sh
# ==============================================================================
# This is intentionally not part of the default lightweight test suite.  It
# requires CVMFS, a valid X509 proxy, XRootD access to IHEP EOS, and enough time
# to create/use a CMSSW_12_4_14 project and run edmCopyPickMerge.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

REMOTE_HOST="${REMOTE_HOST:-cceos.ihep.ac.cn:1094}"
REMOTE_XRDFS_TARGET="${REMOTE_XRDFS_TARGET:-root://cceos.ihep.ac.cn:1094/}"
REMOTE_CAMPAIGN_DIR="${REMOTE_CAMPAIGN_DIR:-/store/user/xcheng/MC_Production_v3/output/JUP_DPS1}"
REMOTE_URL_PREFIX="${REMOTE_URL_PREFIX:-root://cceos.ihep.ac.cn:1094///}"
REMOTE_JOB_REGEX="${REMOTE_JOB_REGEX:-^47[0-9]$}"
N_INPUTS="${N_INPUTS:-2}"
VALIDATION="${VALIDATION:-none}"
EXPECTED_EVENTS="${EXPECTED_EVENTS:-}"

WORKDIR="${WORKDIR:-$(mktemp -d /tmp/chiw/miniaod_merge_smoke_XXXXX)}"
KEEP_WORKDIR="${KEEP_WORKDIR:-0}"

cleanup() {
    if [[ "${KEEP_WORKDIR}" == "1" ]]; then
        echo "[INFO] Preserving workdir: ${WORKDIR}"
        return 0
    fi
    rm -rf "${WORKDIR}"
}
trap cleanup EXIT

msg() { printf '[INFO] %s\n' "$*"; }
die() { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

proxy_path="${X509_USER_PROXY:-/tmp/x509up_u$(id -u)}"
[[ -s "${proxy_path}" ]] || die "No X509 proxy found at ${proxy_path}; run voms-proxy-init first"
if command -v voms-proxy-info >/dev/null 2>&1; then
    voms-proxy-info --file "${proxy_path}" --timeleft >/dev/null || \
        die "X509 proxy is not valid: ${proxy_path}"
fi
[[ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]] || die "CVMFS CMS setup is unavailable"
command -v xrdfs >/dev/null 2>&1 || die "xrdfs not found"
command -v xrdcp >/dev/null 2>&1 || die "xrdcp not found"

mkdir -p "${WORKDIR}/proxy/credentials" "${WORKDIR}/worker" "${WORKDIR}/output"
install -m 600 "${proxy_path}" "${WORKDIR}/proxy/credentials/x509_user_proxy"
tar -czf "${WORKDIR}/proxy_bundle.tar.gz" -C "${WORKDIR}/proxy" credentials

msg "Discovering MiniAOD inputs in root://${REMOTE_HOST}/${REMOTE_CAMPAIGN_DIR}"
mapfile -t jobs < <(
    xrdfs "${REMOTE_XRDFS_TARGET}" ls "${REMOTE_CAMPAIGN_DIR}" |
    awk -F/ '{print $NF}' |
    grep -E "${REMOTE_JOB_REGEX}" |
    sort |
    head -n "${N_INPUTS}"
)

[[ "${#jobs[@]}" -ge "${N_INPUTS}" ]] || die "Found ${#jobs[@]} matching jobs; need ${N_INPUTS}"

inputs_json="${WORKDIR}/inputs.json"
python3 - "${inputs_json}" "${REMOTE_URL_PREFIX}" "${REMOTE_CAMPAIGN_DIR}" "${jobs[@]}" <<'PY'
import json
import sys

out = sys.argv[1]
prefix = sys.argv[2]
campaign_dir = sys.argv[3].strip("/")
jobs = sys.argv[4:]
payload = [
    {
        "job_id": f"REMOTE_{job}",
        "url": f"{prefix}{campaign_dir}/{job}/output_MINIAOD.root",
    }
    for job in jobs
]
with open(out, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
for item in payload:
    print(item["url"])
PY

for url in $(python3 -c 'import json,sys; print("\n".join(i["url"] for i in json.load(open(sys.argv[1]))))' "${inputs_json}"); do
    msg "Checking remote file: ${url}"
    xrdcp --nopbar --force "${url}" /dev/null >/dev/null 2>&1 || \
        die "Cannot read remote MiniAOD: ${url}"
done

config="${WORKDIR}/worker/miniaod_merge_config.json"
python3 - "${config}" "${inputs_json}" "${WORKDIR}/output/merged_MINIAOD.root" "${VALIDATION}" "${EXPECTED_EVENTS}" <<'PY'
import json
import sys

config_path, inputs_path, output_url, validation, expected = sys.argv[1:]
with open(inputs_path, encoding="utf-8") as handle:
    inputs = json.load(handle)
payload = {
    "campaign": "JUP_DPS1",
    "job_id": "SMOKE_MERGE",
    "input_miniaods": inputs,
    "expected_events": int(expected) if expected else None,
    "output_url": output_url,
    "max_size": 5000000,
    "validation": validation,
}
with open(config_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY

cp "${WORKDIR}/proxy_bundle.tar.gz" "${WORKDIR}/worker/"

msg "Running production MiniAOD merge wrapper"
(
    cd "${WORKDIR}/worker"
    bash "${BASE_DIR}/processing/condor_wrappers/run_miniaod_merge.sh" \
        proxy_bundle.tar.gz miniaod_merge_config.json
) 2>&1 | tee "${WORKDIR}/merge_wrapper.log"

[[ -s "${WORKDIR}/output/merged_MINIAOD.root" ]] || die "Merged MiniAOD was not produced"
manifest="${WORKDIR}/output/merge_manifest_JUP_DPS1_SMOKE_MERGE.json"
[[ -s "${manifest}" ]] || die "Merge manifest was not produced"

python3 - "${manifest}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
assert payload["status"] == "ok"
assert payload["campaign"] == "JUP_DPS1"
assert payload["job_id"] == "SMOKE_MERGE"
assert payload["size_bytes"] > 0
assert len(payload["components"]) >= 2
PY

msg "MiniAOD merge smoke passed"
msg "Merged output: ${WORKDIR}/output/merged_MINIAOD.root"
msg "Manifest: ${manifest}"
