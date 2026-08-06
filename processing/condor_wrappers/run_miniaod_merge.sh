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
        universal_newlines=True,
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


def read_url_text(url, workdir):
    if not url:
        return None
    if url.startswith("root://"):
        local_path = workdir / ("manifest_" + str(abs(hash(url))) + ".json")
        proc = subprocess.run(
            ["xrdcp", "--nopbar", "-f", url, str(local_path)],
            check=False,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if proc.returncode != 0:
            print(proc.stdout, file=sys.stderr)
            return None
        return local_path.read_text(encoding="utf-8")
    if url.startswith("file:"):
        path = Path(url[len("file:"):])
    else:
        path = Path(url)
    if path.exists():
        return path.read_text(encoding="utf-8")
    return None


def expected_events_from_manifests(inputs, workdir, strict=False):
    actuals = []
    provenance = []
    for item in inputs:
        if not isinstance(item, dict):
            if strict:
                raise SystemExit("strict merge requires object input records")
            return None, []
        text = read_url_text(item.get("manifest_url", ""), workdir)
        if not text:
            if strict:
                raise SystemExit(f"required processing manifest unavailable: {item.get('manifest_url', '')}")
            return None, []
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            if strict:
                raise SystemExit(f"invalid processing manifest JSON: {item.get('manifest_url', '')}")
            return None, []
        if strict:
            status = payload.get("status")
            accepted_complete = (
                status == "ok"
                and payload.get("complete") is True
                and not payload.get("failure_reason")
            )
            accepted_partial = (
                status == "partial"
                and payload.get("merge_eligible") is True
                and not payload.get("failure_reason")
            )
            if not (accepted_complete or accepted_partial):
                raise SystemExit(
                    f"processing manifest is not merge-eligible: {item.get('manifest_url', '')}"
                )
            if payload.get("miniaod_url") != item.get("url"):
                raise SystemExit(
                    f"processing manifest URL mismatch: {payload.get('miniaod_url')} != {item.get('url')}"
                )
            if not str(payload.get("miniaod_count_source") or "").startswith("edmFileUtil"):
                raise SystemExit("strict merge requires an edmFileUtil MiniAOD count")
            actual_miniaod = int(payload.get("actual_miniaod_events", 0))
            actual_mixed = int(payload.get("actual_mixed_hepmc_events", 0))
            invalid_counts = (
                actual_miniaod <= 0
                or actual_mixed <= 0
                or actual_miniaod > actual_mixed
                or (status == "ok" and actual_miniaod != actual_mixed)
            )
            if invalid_counts:
                raise SystemExit(
                    f"invalid processing counts: mixed={actual_mixed} miniaod={actual_miniaod}"
                )
        if "actual_miniaod_events" not in payload:
            return None, []
        actual_miniaod = int(payload["actual_miniaod_events"])
        actuals.append(actual_miniaod)
        provenance.append({
            "job_id": item.get("job_id"),
            "manifest_url": item.get("manifest_url"),
            "miniaod_url": item.get("url"),
            "actual_mixed_hepmc_events": int(payload.get("actual_mixed_hepmc_events", 0)),
            "actual_miniaod_events": actual_miniaod,
            "miniaod_count_source": payload.get("miniaod_count_source"),
            "status": payload.get("status"),
            "complete": payload.get("complete"),
            "merge_eligible": payload.get("merge_eligible"),
            "partial_reason": payload.get("partial_reason"),
            "missing_miniaod_events": int(payload.get("missing_miniaod_events", 0)),
            "miniaod_loss_fraction": float(payload.get("miniaod_loss_fraction", 0.0)),
        })
    if len(actuals) != len(inputs):
        return None, []
    return sum(actuals), provenance


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

    strict_manifests = bool(cfg.get("require_processing_manifests", False))
    manifest_expected_events, processing_manifest_counts = expected_events_from_manifests(
        inputs, workdir, strict=strict_manifests
    )
    expected_events = (
        manifest_expected_events
        if manifest_expected_events is not None
        else cfg.get("expected_events")
    )

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

    partial_components = [
        item.get("job_id")
        for item in processing_manifest_counts
        if item.get("status") == "partial"
    ]
    manifest = {
        "campaign": cfg["campaign"],
        "job_id": cfg["job_id"],
        "status": "partial" if partial_components else "ok",
        "complete": not partial_components,
        "merge_eligible": True,
        "partial_components": partial_components,
        "output_url": output_url,
        "size_bytes": size,
        "expected_events": expected_events,
        "expected_events_source": "input_manifests" if manifest_expected_events is not None else "config",
        "require_processing_manifests": strict_manifests,
        "packing_weight_events": cfg.get("packing_weight_events"),
        "actual_events": actual_events,
        "validation": cfg.get("validation"),
        "components": inputs,
        "processing_manifest_counts": processing_manifest_counts,
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
