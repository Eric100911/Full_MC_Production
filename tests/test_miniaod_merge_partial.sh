#!/bin/bash
# Local merge-wrapper regression for merge-eligible partial processing outputs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKDIR=$(mktemp -d /tmp/chiw/miniaod_merge_partial_XXXXX)
trap 'rm -rf "${WORKDIR}"' EXIT

mkdir -p \
    "${WORKDIR}/proxy/credentials" \
    "${WORKDIR}/worker/merge_work/CMSSW_12_4_14/src" \
    "${WORKDIR}/mock_bin" \
    "${WORKDIR}/inputs"
touch "${WORKDIR}/proxy/credentials/x509_user_proxy"
tar -czf "${WORKDIR}/worker/proxy_bundle.tar.gz" \
    -C "${WORKDIR}/proxy" credentials

cat > "${WORKDIR}/mock_bin/scramv1" <<'EOF'
#!/bin/bash
exit 0
EOF
cat > "${WORKDIR}/mock_bin/edmCopyPickMerge" <<'EOF'
#!/bin/bash
set -euo pipefail
for argument in "$@"; do
    case "${argument}" in
        outputFile=*) printf 'mock merged ROOT\n' > "${argument#outputFile=}" ;;
    esac
done
EOF
cat > "${WORKDIR}/mock_bin/edmFileUtil" <<'EOF'
#!/bin/bash
printf '{"events": 15}\n'
EOF
chmod +x \
    "${WORKDIR}/mock_bin/scramv1" \
    "${WORKDIR}/mock_bin/edmCopyPickMerge" \
    "${WORKDIR}/mock_bin/edmFileUtil"

printf 'complete\n' > "${WORKDIR}/inputs/complete.root"
printf 'partial\n' > "${WORKDIR}/inputs/partial.root"
cat > "${WORKDIR}/inputs/complete.json" <<EOF
{
  "status": "ok",
  "complete": true,
  "merge_eligible": true,
  "actual_mixed_hepmc_events": 10,
  "actual_miniaod_events": 10,
  "miniaod_count_source": "edmFileUtil",
  "miniaod_url": "${WORKDIR}/inputs/complete.root"
}
EOF
cat > "${WORKDIR}/inputs/partial.json" <<EOF
{
  "status": "partial",
  "complete": false,
  "merge_eligible": true,
  "actual_mixed_hepmc_events": 10,
  "actual_miniaod_events": 5,
  "missing_miniaod_events": 5,
  "miniaod_loss_fraction": 0.5,
  "partial_reason": "miniaod_event_count_shortfall",
  "miniaod_count_source": "edmFileUtil",
  "miniaod_url": "${WORKDIR}/inputs/partial.root"
}
EOF
cat > "${WORKDIR}/worker/merge_config.json" <<EOF
{
  "campaign": "MOCK_PARTIAL",
  "job_id": "MERGE000000",
  "input_miniaods": [
    {
      "job_id": "COMPLETE",
      "url": "${WORKDIR}/inputs/complete.root",
      "manifest_url": "${WORKDIR}/inputs/complete.json"
    },
    {
      "job_id": "PARTIAL",
      "url": "${WORKDIR}/inputs/partial.root",
      "manifest_url": "${WORKDIR}/inputs/partial.json"
    }
  ],
  "output_url": "${WORKDIR}/output/merged.root",
  "max_size": 5000000,
  "validation": "event-count",
  "require_processing_manifests": true
}
EOF

(
    cd "${WORKDIR}/worker"
    PATH="${WORKDIR}/mock_bin:${PATH}" \
        bash "${BASE_DIR}/processing/condor_wrappers/run_miniaod_merge.sh" \
        proxy_bundle.tar.gz merge_config.json > "${WORKDIR}/merge.log" 2>&1
)

python3 - "${WORKDIR}/output/merge_manifest_MOCK_PARTIAL_MERGE000000.json" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
assert payload["status"] == "partial"
assert payload["complete"] is False
assert payload["merge_eligible"] is True
assert payload["partial_components"] == ["PARTIAL"]
assert payload["expected_events"] == 15
assert payload["actual_events"] == 15
assert payload["processing_manifest_counts"][1]["miniaod_loss_fraction"] == 0.5
PY

echo "partial MiniAOD merge test passed"
