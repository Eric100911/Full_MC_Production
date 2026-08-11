#!/bin/bash
set -euo pipefail
STAGE="${1:?missing stage}"
PROC_ID="${2:?missing ProcId}"
TASKS_PATH="${3:?missing task manifest}"
PROXY_BUNDLE="${4:?missing proxy bundle}"
NTUPLE_BUNDLE="${5:-}"
case "${STAGE}" in merge|ntuple) ;; *) echo "ERROR: unsupported stage" >&2; exit 2;; esac
[[ "${PROC_ID}" =~ ^[0-9]+$ ]] || { echo "ERROR: invalid ProcId" >&2; exit 2; }
[[ -s "${TASKS_PATH}" && -s "${PROXY_BUNDLE}" ]] || { echo "ERROR: missing task/proxy input" >&2; exit 2; }
WORKDIR=$(mktemp -d "/tmp/ihep_split_${STAGE}_${PROC_ID}_XXXXXX")
trap 'rm -rf "${WORKDIR}"' EXIT
CONFIG="${WORKDIR}/task_config.json"
META="${WORKDIR}/task_meta.json"
python3 - "${TASKS_PATH}" "${PROC_ID}" "${CONFIG}" "${META}" <<'PY'
import json
from pathlib import Path
import sys
tasks = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
index = int(sys.argv[2])
if not isinstance(tasks, list) or index >= len(tasks):
    raise SystemExit(f"ProcId {index} outside task count {len(tasks)}")
task = tasks[index]
Path(sys.argv[3]).write_text(json.dumps(task["config"], indent=2) + "\n", encoding="utf-8")
Path(sys.argv[4]).write_text(json.dumps(task, indent=2) + "\n", encoding="utf-8")
PY
cp "${PROXY_BUNDLE}" "${WORKDIR}/proxy_bundle.tar.gz"
WRAPPER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [[ "${STAGE}" == "merge" ]]; then
    cp "${WRAPPER_DIR}/run_miniaod_merge.sh" "${WORKDIR}/"
    COMMAND=$(printf 'cd %q && bash run_miniaod_merge.sh proxy_bundle.tar.gz task_config.json' "${WORKDIR}")
    /cvmfs/cms.cern.ch/common/cmssw-el8 -B /tmp -B /scratchfs2 --command-to-run "${COMMAND}"
    exit $?
fi
[[ -s "${NTUPLE_BUNDLE}" ]] || { echo "ERROR: missing ntuple bundle" >&2; exit 2; }
cp "${WRAPPER_DIR}/run_ntuple_only.sh" "${WORKDIR}/"
cp "${NTUPLE_BUNDLE}" "${WORKDIR}/ntuple_runtime_bundle.tar.gz"
COMMAND=$(printf 'cd %q && bash run_ntuple_only.sh proxy_bundle.tar.gz ntuple_runtime_bundle.tar.gz task_config.json' "${WORKDIR}")
/cvmfs/cms.cern.ch/common/cmssw-el9 -B /tmp -B /scratchfs2 --command-to-run "${COMMAND}"
tar -xzf "${WORKDIR}/proxy_bundle.tar.gz" -C "${WORKDIR}"
PROXY_TARGET="${WORKDIR}/credentials/x509_user_proxy"
[[ -s "${PROXY_TARGET}" ]] || { echo "ERROR: missing extracted proxy after ntuple container" >&2; exit 2; }
export X509_USER_PROXY="${PROXY_TARGET}"
python3 - "${META}" "${WORKDIR}" <<'PY'
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
task = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
payload = {"status": "ok", "campaign": task["campaign"], "job_id": task["job_id"],
           "ntuple_url": task["ntuple_url"]}
local = Path(sys.argv[2]) / f"split_ntuple_manifest_{task['campaign']}_{task['job_id']}.json"
local.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
destination = task["manifest_url"]
if destination.startswith("root://"):
    match = re.match(r"^(root://[^/]+)/(.*)$", destination)
    endpoint, path = match.group(1) + "/", "/" + match.group(2).lstrip("/")
    subprocess.run(["xrdfs", endpoint, "mkdir", "-p", path.rsplit("/", 1)[0]], check=True)
    subprocess.run(["xrdcp", "--nopbar", "-f", str(local), destination], check=True)
else:
    target = Path(destination[5:] if destination.startswith("file:") else destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(local, target)
PY
echo "IHEP split task complete: stage=${STAGE} ProcId=${PROC_ID}"
