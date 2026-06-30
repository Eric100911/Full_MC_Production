#!/bin/bash
# Archive submit-side Condor logs for one block SubDAG.  This script is meant
# to be used as a DAGMan SCRIPT POST.  It exits 0 even when archival fails so
# physics output success is not made dependent on log upload.

set -u

campaign=""
job_index=""
log_root=""
target_eos_base=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --campaign) campaign="${2:-}"; shift 2 ;;
        --job-index) job_index="${2:-}"; shift 2 ;;
        --log-root) log_root="${2:-}"; shift 2 ;;
        --target-eos-base) target_eos_base="${2:-}"; shift 2 ;;
        *) echo "WARN: unknown argument: $1" >&2; shift ;;
    esac
done

fail_soft() {
    echo "WARN: $*" >&2
    exit 0
}

[[ -n "${campaign}" ]] || fail_soft "missing --campaign"
[[ -n "${job_index}" ]] || fail_soft "missing --job-index"
[[ -n "${log_root}" ]] || fail_soft "missing --log-root"
[[ -n "${target_eos_base}" ]] || fail_soft "missing --target-eos-base"

job_component=$(printf 'job_%06d' "${job_index}" 2>/dev/null || echo "job_${job_index}")
archive_name="logs_${campaign}_${job_component}.tar.gz"
workdir=$(mktemp -d "${TMPDIR:-/tmp}/subdag_log_archive.XXXXXX") || fail_soft "mktemp failed"
trap 'rm -rf "${workdir}"' EXIT

archive_path="${workdir}/${archive_name}"
manifest_path="${workdir}/${archive_name%.tar.gz}.json"

found=0
tar_entries=()
for stage in processing miniaod_merge ntuple final; do
    if [[ -d "${log_root}/${campaign}/${stage}/${job_component}" ]]; then
        found=1
        tar_entries+=("${campaign}/${stage}/${job_component}")
    fi
done

if [[ "${found}" -eq 0 ]]; then
    fail_soft "no log directories found under ${log_root}/${campaign} for ${job_component}"
fi

tar -czf "${archive_path}" -C "${log_root}" "${tar_entries[@]}" \
    2>"${workdir}/tar.stderr" || true

if [[ ! -s "${archive_path}" ]]; then
    fail_soft "archive creation failed: $(cat "${workdir}/tar.stderr" 2>/dev/null)"
fi

archive_size=$(stat -c '%s' "${archive_path}" 2>/dev/null || wc -c < "${archive_path}")
cat > "${manifest_path}" <<EOF
{
  "campaign": "${campaign}",
  "job_index": ${job_index},
  "job_component": "${job_component}",
  "archive": "${archive_name}",
  "size_bytes": ${archive_size}
}
EOF

remote_dir="${target_eos_base%/}/output/${campaign}/${job_component}_logs"
if [[ "${remote_dir}" == root://* ]]; then
    remote_no_scheme="${remote_dir#root://}"
    host="${remote_no_scheme%%/*}"
    path="/${remote_no_scheme#*/}"
    xrdfs "${host}" mkdir -p "${path}" || fail_soft "xrdfs mkdir failed for ${remote_dir}"
    xrdcp --nopbar -f "${archive_path}" "${remote_dir}/${archive_name}" || fail_soft "xrdcp archive failed"
    xrdcp --nopbar -f "${manifest_path}" "${remote_dir}/$(basename "${manifest_path}")" || fail_soft "xrdcp manifest failed"
else
    mkdir -p "${remote_dir}" || fail_soft "mkdir failed for ${remote_dir}"
    cp -f "${archive_path}" "${remote_dir}/${archive_name}" || fail_soft "copy archive failed"
    cp -f "${manifest_path}" "${remote_dir}/$(basename "${manifest_path}")" || fail_soft "copy manifest failed"
fi

echo "Log archive uploaded: ${remote_dir}/${archive_name}"
exit 0
