#!/bin/bash
# Archive submit-side Condor logs for one block SubDAG.  This script is meant
# to be used as a DAGMan SCRIPT POST. Packaging and upload failures remain
# fail-soft, including credential problems. The status JSON is the durable
# signal for operators; physics products must not be invalidated by archival.

set -u

campaign=""
job_index=""
log_root=""
target_eos_base=""
proxy_bundle=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --campaign) campaign="${2:-}"; shift 2 ;;
        --job-index) job_index="${2:-}"; shift 2 ;;
        --log-root) log_root="${2:-}"; shift 2 ;;
        --target-eos-base) target_eos_base="${2:-}"; shift 2 ;;
        --proxy-bundle) proxy_bundle="${2:-}"; shift 2 ;;
        *) echo "WARN: unknown argument: $1" >&2; shift ;;
    esac
done

[[ -n "${campaign}" ]] || { echo "WARN: missing --campaign" >&2; exit 0; }
[[ -n "${job_index}" ]] || { echo "WARN: missing --job-index" >&2; exit 0; }
[[ -n "${log_root}" ]] || { echo "WARN: missing --log-root" >&2; exit 0; }
[[ -n "${target_eos_base}" ]] || { echo "WARN: missing --target-eos-base" >&2; exit 0; }

job_component=$(printf 'job_%06d' "${job_index}" 2>/dev/null || echo "job_${job_index}")
archive_name="logs_${campaign}_${job_component}.tar.gz"
status_path="${log_root}/${campaign}/final/${job_component}/log_archive_status.json"
proxy_timeleft=""

write_status() {
    local status="$1"
    local phase="$2"
    local message="$3"
    local exit_code="$4"
    python3 - "${status_path}" "${campaign}" "${job_index}" "${job_component}" \
        "${status}" "${phase}" "${message}" "${exit_code}" "${proxy_timeleft}" <<'PY' \
        || echo "WARN: failed to write archive status: ${status_path}" >&2
import json
import os
import sys
from datetime import datetime, timezone

(
    path,
    campaign,
    job_index,
    job_component,
    status,
    phase,
    message,
    exit_code,
    proxy_timeleft,
) = sys.argv[1:]
payload = {
    "campaign": campaign,
    "job_index": int(job_index),
    "job_component": job_component,
    "status": status,
    "phase": phase,
    "message": message,
    "exit_code": int(exit_code),
    "proxy_timeleft_seconds": (
        int(proxy_timeleft) if proxy_timeleft.isdigit() else None
    ),
    "updated_at": datetime.now(timezone.utc).isoformat(),
}
os.makedirs(os.path.dirname(path), exist_ok=True)
temporary = f"{path}.tmp.{os.getpid()}"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
os.chmod(temporary, 0o600)
os.replace(temporary, path)
PY
}

fail_soft() {
    local phase="$1"
    shift
    write_status "failed" "${phase}" "$*" 1
    echo "WARN: $*" >&2
    exit 0
}

fail_proxy() {
    local message="$*"
    write_status "failed" "proxy_validation" "${message}" 2
    echo "WARN: ${message}" >&2
    exit 0
}

[[ -n "${proxy_bundle}" ]] || fail_proxy "missing --proxy-bundle"
[[ -r "${proxy_bundle}" && -s "${proxy_bundle}" ]] \
    || fail_proxy "proxy bundle is missing, unreadable, or empty: ${proxy_bundle}"

workdir=$(mktemp -d "${TMPDIR:-/tmp}/subdag_log_archive.XXXXXX") \
    || fail_soft "setup" "mktemp failed"
trap 'rm -rf "${workdir}"' EXIT

archive_path="${workdir}/${archive_name}"
manifest_path="${workdir}/${archive_name%.tar.gz}.json"
credential_dir="${workdir}/credential"
mkdir -m 700 "${credential_dir}" || fail_proxy "failed to create private credential directory"
tar -xzf "${proxy_bundle}" \
    -C "${credential_dir}" \
    --no-same-owner \
    --no-same-permissions \
    credentials/x509_user_proxy \
    || fail_proxy "failed to extract proxy bundle: ${proxy_bundle}"
install -m 600 \
    "${credential_dir}/credentials/x509_user_proxy" \
    "${credential_dir}/x509_user_proxy" \
    || fail_proxy "failed to install proxy from bundle: ${proxy_bundle}"
rm -rf "${credential_dir}/credentials"
export X509_USER_PROXY="${credential_dir}/x509_user_proxy"

command -v voms-proxy-info >/dev/null 2>&1 \
    || fail_proxy "voms-proxy-info is unavailable; refusing unverified proxy"
voms-proxy-info --file "${X509_USER_PROXY}" --exists --valid 0:10 \
    || fail_proxy "bundled proxy is expired or has less than 10 minutes remaining"
proxy_timeleft=$(voms-proxy-info --file "${X509_USER_PROXY}" --timeleft 2>/dev/null) \
    || fail_proxy "could not determine bundled proxy lifetime"
[[ "${proxy_timeleft}" =~ ^[0-9]+$ ]] \
    || fail_proxy "invalid bundled proxy lifetime reported: ${proxy_timeleft}"

found=0
tar_entries=()
for stage in processing miniaod_merge ntuple final; do
    if [[ -d "${log_root}/${campaign}/${stage}/${job_component}" ]]; then
        found=1
        tar_entries+=("${campaign}/${stage}/${job_component}")
    fi
done

if [[ "${found}" -eq 0 ]]; then
    fail_soft "log_discovery" \
        "no log directories found under ${log_root}/${campaign} for ${job_component}"
fi

if ! tar -czf "${archive_path}" -C "${log_root}" "${tar_entries[@]}" \
    2>"${workdir}/tar.stderr"; then
    fail_soft "archive_creation" \
        "archive creation failed: $(cat "${workdir}/tar.stderr" 2>/dev/null)"
fi

if [[ ! -s "${archive_path}" ]]; then
    fail_soft "archive_creation" \
        "archive creation failed: $(cat "${workdir}/tar.stderr" 2>/dev/null)"
fi

archive_size=$(stat -c '%s' "${archive_path}" 2>/dev/null || wc -c < "${archive_path}")
cat > "${manifest_path}" <<EOF
{
  "campaign": "${campaign}",
  "job_index": ${job_index},
  "job_component": "${job_component}",
  "archive": "${archive_name}",
  "size_bytes": ${archive_size},
  "proxy_timeleft_seconds": ${proxy_timeleft}
}
EOF

remote_dir="${target_eos_base%/}/output/${campaign}/${job_component}_logs"
if [[ "${remote_dir}" == root://* ]]; then
    remote_no_scheme="${remote_dir#root://}"
    host="${remote_no_scheme%%/*}"
    path="/${remote_no_scheme#*/}"
    xrdfs "${host}" mkdir -p "${path}" \
        || fail_soft "remote_mkdir" "xrdfs mkdir failed for ${remote_dir}"
    xrdcp --nopbar -f "${archive_path}" "${remote_dir}/${archive_name}" \
        || fail_soft "archive_upload" "xrdcp archive failed"
    xrdcp --nopbar -f "${manifest_path}" "${remote_dir}/$(basename "${manifest_path}")" \
        || fail_soft "manifest_upload" "xrdcp manifest failed"
else
    mkdir -p "${remote_dir}" \
        || fail_soft "remote_mkdir" "mkdir failed for ${remote_dir}"
    cp -f "${archive_path}" "${remote_dir}/${archive_name}" \
        || fail_soft "archive_upload" "copy archive failed"
    cp -f "${manifest_path}" "${remote_dir}/$(basename "${manifest_path}")" \
        || fail_soft "manifest_upload" "copy manifest failed"
fi

write_status "ok" "complete" "log archive uploaded" 0
echo "Log archive uploaded: ${remote_dir}/${archive_name}"
exit 0
