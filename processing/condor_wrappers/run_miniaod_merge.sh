#!/bin/bash
# ==============================================================================
# run_miniaod_merge.sh - HTCondor wrapper for edmCopyPickMerge MiniAOD merging.
# ==============================================================================

set -euo pipefail

PROXY_BUNDLE="${1:?missing proxy bundle}"
CONFIG_NAME="${2:?missing merge config JSON}"
CONFIG_PATH="${PWD}/${CONFIG_NAME}"

if [[ ! -s "${CONFIG_PATH}" ]]; then
    echo "FATAL: merge config JSON not found or empty: ${CONFIG_PATH}" >&2
    exit 1
fi

echo "=== MiniAOD merge wrapper ==="
echo "Host: $(hostname)"
echo "Date: $(date -Iseconds)"
echo "Working dir: $(pwd)"
echo "Config: ${CONFIG_NAME}"

echo "--- Extracting proxy bundle ---"
tar -xzf "${PROXY_BUNDLE}"
PROXY_TARGET="/tmp/x509up_u$(id -u)"
install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"

python3 - "${CONFIG_PATH}" <<'PY'
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def run(cmd, *, env=None, cwd=None):
    print("+", " ".join(str(part) for part in cmd), flush=True)
    return subprocess.run(cmd, check=True, env=env, cwd=cwd)


def remote_parts(url):
    if not url.startswith("root://"):
        return None
    rest = url[len("root://"):]
    host, path = rest.split("/", 1)
    return host, "/" + path


def xrd_mkdir(url):
    parts = remote_parts(url)
    if parts is None:
        return
    host, path = parts
    run(["xrdfs", host, "mkdir", "-p", path])


def xrd_stat_size(url):
    parts = remote_parts(url)
    if parts is None:
        path = Path(url)
        return path.stat().st_size if path.exists() else None
    host, path = parts
    proc = subprocess.run(
        ["xrdfs", host, "stat", path],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if proc.returncode != 0:
        print(proc.stdout, file=sys.stderr)
        return None
    for line in proc.stdout.splitlines():
        fields = line.strip().split()
        if len(fields) >= 2 and fields[0].lower().startswith("size"):
            try:
                return int(fields[-1])
            except ValueError:
                pass
    return None


def stage_out(local_path, destination):
    if destination.startswith("root://"):
        parent = destination.rsplit("/", 1)[0]
        xrd_mkdir(parent)
        run(["xrdcp", "--nopbar", "-f", str(local_path), destination])
    else:
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)


def cmssw_script(workdir, input_list, output_file, max_size):
    return f"""#!/bin/bash
set -euo pipefail
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc10
cd {workdir}
if [[ ! -d CMSSW_12_4_14/src ]]; then
  scramv1 project CMSSW CMSSW_12_4_14
fi
cd CMSSW_12_4_14/src
eval $(scramv1 runtime -sh)
cd {workdir}
edmCopyPickMerge inputFiles_load={input_list} outputFile={output_file} maxSize={max_size}
if command -v edmFileUtil >/dev/null 2>&1; then
  edmFileUtil -j {output_file} > {workdir}/merged_MINIAOD_edmFileUtil.json 2> {workdir}/merged_MINIAOD_edmFileUtil.stderr || \\
  edmFileUtil {output_file} > {workdir}/merged_MINIAOD_edmFileUtil.txt 2>> {workdir}/merged_MINIAOD_edmFileUtil.stderr || true
fi
"""


def find_event_count(payload):
    candidates = []

    def visit(obj, path):
        if isinstance(obj, dict):
            for key, value in obj.items():
                visit(value, path + [str(key)])
        elif isinstance(obj, list):
            for index, value in enumerate(obj):
                visit(value, path + [str(index)])
        elif isinstance(obj, int) and obj >= 0:
            lowered = "/".join(path).lower()
            if "event" in lowered and ("count" in lowered or lowered.endswith("events")):
                candidates.append(obj)

    visit(payload, [])
    if not candidates:
        return None
    return max(candidates)


def merged_event_count(workdir):
    json_path = workdir / "merged_MINIAOD_edmFileUtil.json"
    if json_path.exists() and json_path.stat().st_size > 0:
        try:
            return find_event_count(json.loads(json_path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            return None
    return None


def main():
    cfg_path = Path(sys.argv[1])
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    required = [
        "campaign",
        "job_id",
        "input_miniaods",
        "output_url",
        "max_size",
        "validation",
    ]
    missing = [key for key in required if key not in cfg or cfg[key] in (None, "")]
    if missing:
        raise SystemExit(f"missing merge config keys: {', '.join(missing)}")
    inputs = list(cfg["input_miniaods"])
    if not inputs:
        raise SystemExit("input_miniaods must be non-empty")

    workdir = Path.cwd() / "merge_work"
    workdir.mkdir(exist_ok=True)
    input_list = workdir / "merge_inputs.txt"
    with input_list.open("w", encoding="utf-8") as handle:
        for item in inputs:
            url = str(item["url"] if isinstance(item, dict) else item)
            if url.startswith("root://"):
                handle.write(url + "\n")
            elif url.startswith("file:"):
                handle.write(url + "\n")
            else:
                handle.write("file:" + url + "\n")

    output_file = workdir / "merged_MINIAOD.root"
    script = workdir / "run_edmCopyPickMerge.sh"
    script.write_text(
        cmssw_script(workdir, input_list, output_file, int(cfg.get("max_size", 5000000))),
        encoding="utf-8",
    )
    script.chmod(0o755)
    run(["/bin/bash", str(script)])
    if not output_file.exists() or output_file.stat().st_size == 0:
        raise SystemExit("merged MiniAOD missing or empty")
    actual_events = merged_event_count(workdir)
    expected_events = cfg.get("expected_events")
    if cfg.get("validation") == "event-count":
        if actual_events is None:
            raise SystemExit("event-count validation requested but edmFileUtil event count was unavailable")
        if expected_events is not None and int(expected_events) > 0 and actual_events != int(expected_events):
            raise SystemExit(
                f"merged event count mismatch: actual={actual_events} expected={expected_events}"
            )

    output_url = str(cfg["output_url"])
    stage_out(output_file, output_url)
    size = xrd_stat_size(output_url)
    if size is None or size <= 0:
        raise SystemExit(f"merged MiniAOD remote validation failed: {output_url}")

    manifest = {
        "campaign": cfg["campaign"],
        "job_id": cfg["job_id"],
        "status": "ok",
        "output_url": output_url,
        "size_bytes": size,
        "expected_events": expected_events,
        "actual_events": actual_events,
        "validation": cfg.get("validation"),
        "components": inputs,
    }
    manifest_path = workdir / f"merge_manifest_{cfg['campaign']}_{cfg['job_id']}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    manifest_url = output_url.rsplit("/", 1)[0] + "/" + manifest_path.name
    stage_out(manifest_path, manifest_url)
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
PY

echo "=== MiniAOD merge completed successfully ==="
