#!/bin/bash
# ==============================================================================
# test_subdag_final_and_archive.sh - local mocks for SubDAG final inventory and
# submit-side log archival.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PASS=0
FAIL=0

pass() { echo "[OK] $*"; PASS=$((PASS + 1)); }
fail() { echo "[ERROR] $*" >&2; FAIL=$((FAIL + 1)); }

WORKDIR=$(mktemp -d /tmp/chiw/subdag_final_archive_XXXXX)
cleanup() {
    rm -rf "${WORKDIR}"
}
trap cleanup EXIT

mkdir -p "${WORKDIR}/proxy/credentials" "${WORKDIR}/worker" "${WORKDIR}/mock_bin"
touch "${WORKDIR}/proxy/credentials/x509_user_proxy"

cat > "${WORKDIR}/mock_bin/voms-proxy-info" <<'EOF'
#!/bin/bash
set -euo pipefail
if [[ "${MOCK_PROXY_EXPIRED:-0}" == "1" ]]; then
    exit 1
fi
for argument in "$@"; do
    if [[ "${argument}" == "--timeleft" ]]; then
        echo 7200
        exit 0
    fi
done
exit 0
EOF
chmod +x "${WORKDIR}/mock_bin/voms-proxy-info"

python3 - "${BASE_DIR}" "${WORKDIR}/proxy/credentials/x509_user_proxy" \
    "${WORKDIR}" <<'PY'
import os
import stat
import sys

sys.path.insert(0, sys.argv[1])
import dag_generator

bundle_path, bundle_name = dag_generator.build_proxy_bundle(sys.argv[3], sys.argv[2])
assert bundle_name == "proxy_bundle.tar.gz"
assert stat.S_IMODE(os.stat(bundle_path).st_mode) == 0o600
PY
pass "Proxy bundle is built atomically with private permissions"

touch_nonempty() {
    mkdir -p "$(dirname "$1")"
    printf '%s\n' "$2" > "$1"
}

touch_nonempty "${WORKDIR}/outputs/block0/output_MINIAOD.root" block0
touch_nonempty "${WORKDIR}/outputs/block1/output_MINIAOD.root" block1
touch_nonempty "${WORKDIR}/outputs/merge0/output_MINIAOD.root" merge0
touch_nonempty "${WORKDIR}/outputs/merge0/output_ntuple.root" ntuple0

cat > "${WORKDIR}/worker/final_config.json" <<EOF
{
  "campaign": "MOCK_FINAL",
  "job_index": 3,
  "event_id_scheme": "run1-cantor-job-block-lumi-v1",
  "output_url": "${WORKDIR}/inventory/subdag_inventory.json",
  "blocks": [
    {
      "block_index": 0,
      "job_id": "JOB000003_BLOCK000000",
      "miniaod_url": "${WORKDIR}/outputs/block0/output_MINIAOD.root"
    },
    {
      "block_index": 1,
      "job_id": "JOB000003_BLOCK000001",
      "miniaod_url": "${WORKDIR}/outputs/block1/output_MINIAOD.root"
    },
    {
      "block_index": 2,
      "job_id": "JOB000003_BLOCK000002",
      "miniaod_url": "${WORKDIR}/outputs/missing/output_MINIAOD.root"
    }
  ],
  "merge_groups": [
    {
      "merge_index": 0,
      "job_id": "JOB000003_MERGE000000",
      "merged_miniaod_url": "${WORKDIR}/outputs/merge0/output_MINIAOD.root"
    }
  ],
  "ntuples": [
    {
      "job_id": "JOB000003_MERGE000000",
      "ntuple_url": "${WORKDIR}/outputs/merge0/output_ntuple.root"
    }
  ]
}
EOF

cp "${WORKDIR}/proxy_bundle.tar.gz" "${WORKDIR}/worker/"
(
    cd "${WORKDIR}/worker"
    bash "${BASE_DIR}/processing/condor_wrappers/run_subdag_final_inventory.sh" \
        proxy_bundle.tar.gz final_config.json > "${WORKDIR}/final.log" 2>&1
)

python3 - "${WORKDIR}/inventory/subdag_inventory.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    payload = json.load(handle)
assert payload["campaign"] == "MOCK_FINAL"
assert payload["job_index"] == 3
assert payload["event_id_scheme"] == "run1-cantor-job-block-lumi-v1"
assert payload["status"] == "partial"
assert payload["missing_blocks"] == [2]
assert payload["missing_merges"] == []
assert payload["missing_ntuples"] == []
assert payload["blocks"][0]["miniaod_url_stat"]["exists"] is True
assert payload["blocks"][2]["miniaod_url_stat"]["exists"] is False
PY
[[ ! -e "${WORKDIR}/worker/subdag_inventory.json" ]] || {
    fail "Final inventory leaked into the worker directory"
    exit 1
}
pass "Final inventory records local output stats and partial status"

LOG_ROOT="${WORKDIR}/logs"
for stage in processing miniaod_merge ntuple final; do
    mkdir -p "${LOG_ROOT}/MOCK_FINAL/${stage}/job_000003"
    printf '%s log\n' "${stage}" > "${LOG_ROOT}/MOCK_FINAL/${stage}/job_000003/${stage}.stdout"
done

PATH="${WORKDIR}/mock_bin:${PATH}" \
bash "${BASE_DIR}/tools/archive_subdag_logs.sh" \
    --campaign MOCK_FINAL \
    --job-index 3 \
    --log-root "${LOG_ROOT}" \
    --target-eos-base "${WORKDIR}/archive_target" \
    --proxy-bundle "${WORKDIR}/proxy_bundle.tar.gz" \
    > "${WORKDIR}/archive.log" 2>&1

ARCHIVE_DIR="${WORKDIR}/archive_target/output/MOCK_FINAL/job_000003_logs"
ARCHIVE="${ARCHIVE_DIR}/logs_MOCK_FINAL_job_000003.tar.gz"
MANIFEST="${ARCHIVE_DIR}/logs_MOCK_FINAL_job_000003.json"
[[ -s "${ARCHIVE}" ]] || { fail "Log archive missing"; exit 1; }
[[ -s "${MANIFEST}" ]] || { fail "Log archive manifest missing"; exit 1; }
tar -tzf "${ARCHIVE}" | grep -q 'MOCK_FINAL/processing/job_000003/processing.stdout'
tar -tzf "${ARCHIVE}" | grep -q 'MOCK_FINAL/miniaod_merge/job_000003/miniaod_merge.stdout'
pass "Submit-side log archive contains staged log directories"

python3 - "${MANIFEST}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
assert payload["campaign"] == "MOCK_FINAL"
assert payload["job_index"] == 3
assert payload["job_component"] == "job_000003"
assert payload["size_bytes"] > 0
assert payload["proxy_timeleft_seconds"] == 7200
PY
pass "Log archive manifest is valid"

STATUS="${LOG_ROOT}/MOCK_FINAL/final/job_000003/log_archive_status.json"
python3 - "${STATUS}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
assert payload["status"] == "ok"
assert payload["phase"] == "complete"
assert payload["proxy_timeleft_seconds"] == 7200
PY
pass "Log archive writes a persistent success status"

set +e
MOCK_PROXY_EXPIRED=1 PATH="${WORKDIR}/mock_bin:${PATH}" \
bash "${BASE_DIR}/tools/archive_subdag_logs.sh" \
    --campaign MOCK_EXPIRED \
    --job-index 4 \
    --log-root "${LOG_ROOT}" \
    --target-eos-base "${WORKDIR}/expired_target" \
    --proxy-bundle "${WORKDIR}/proxy_bundle.tar.gz" \
    > "${WORKDIR}/expired.log" 2>&1
expired_rc=$?
set -e
[[ "${expired_rc}" -ne 0 ]] || {
    fail "Expired proxy did not make the POST script fail"
    exit 1
}
[[ ! -e "${WORKDIR}/expired_target" ]] || {
    fail "Expired proxy unexpectedly reached archive upload"
    exit 1
}
python3 - "${LOG_ROOT}/MOCK_EXPIRED/final/job_000004/log_archive_status.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
assert payload["status"] == "failed"
assert payload["phase"] == "proxy_validation"
assert payload["exit_code"] == 2
PY
pass "Expired proxy is a hard failure with persistent diagnostics"

PATH="${WORKDIR}/mock_bin:${PATH}" \
bash "${BASE_DIR}/tools/archive_subdag_logs.sh" \
    --campaign MOCK_NO_LOGS \
    --job-index 5 \
    --log-root "${LOG_ROOT}" \
    --target-eos-base "${WORKDIR}/no_logs_target" \
    --proxy-bundle "${WORKDIR}/proxy_bundle.tar.gz" \
    > "${WORKDIR}/no_logs.log" 2>&1
python3 - "${LOG_ROOT}/MOCK_NO_LOGS/final/job_000005/log_archive_status.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
assert payload["status"] == "failed"
assert payload["phase"] == "log_discovery"
assert payload["exit_code"] == 1
PY
pass "Non-credential archival errors remain fail-soft and are recorded"

echo "[INFO] ${PASS} checks passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]
