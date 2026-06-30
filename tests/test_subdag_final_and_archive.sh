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

mkdir -p "${WORKDIR}/proxy/credentials" "${WORKDIR}/worker"
touch "${WORKDIR}/proxy/credentials/x509_user_proxy"
tar -czf "${WORKDIR}/proxy_bundle.tar.gz" -C "${WORKDIR}/proxy" credentials

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
assert payload["status"] == "partial"
assert payload["missing_blocks"] == [2]
assert payload["missing_merges"] == []
assert payload["missing_ntuples"] == []
assert payload["blocks"][0]["miniaod_url_stat"]["exists"] is True
assert payload["blocks"][2]["miniaod_url_stat"]["exists"] is False
PY
pass "Final inventory records local output stats and partial status"

LOG_ROOT="${WORKDIR}/logs"
for stage in processing miniaod_merge ntuple final; do
    mkdir -p "${LOG_ROOT}/MOCK_FINAL/${stage}/job_000003"
    printf '%s log\n' "${stage}" > "${LOG_ROOT}/MOCK_FINAL/${stage}/job_000003/${stage}.stdout"
done

bash "${BASE_DIR}/tools/archive_subdag_logs.sh" \
    --campaign MOCK_FINAL \
    --job-index 3 \
    --log-root "${LOG_ROOT}" \
    --target-eos-base "${WORKDIR}/archive_target" \
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
PY
pass "Log archive manifest is valid"

echo "[INFO] ${PASS} checks passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]
