#!/bin/bash
# ==============================================================================
# run_subdag_final_inventory.sh - worker-side final inventory for block SubDAGs.
# ==============================================================================

set -euo pipefail

PROXY_BUNDLE="${1:?missing proxy bundle}"
CONFIG_NAME="${2:?missing final config JSON}"
CONFIG_PATH="${PWD}/${CONFIG_NAME}"

if [[ ! -s "${CONFIG_PATH}" ]]; then
    echo "FATAL: final inventory config JSON not found or empty: ${CONFIG_PATH}" >&2
    exit 1
fi

echo "=== SubDAG final inventory wrapper ==="
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
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def remote_parts(url):
    if not url.startswith("root://"):
        return None
    rest = url[len("root://"):]
    host, path = rest.split("/", 1)
    return host, "/" + path


def stat_url(url):
    if not url:
        return {"url": url, "exists": False, "size_bytes": None}
    parts = remote_parts(url)
    if parts is None:
        path = Path(url)
        return {
            "url": url,
            "exists": path.exists() and path.stat().st_size > 0,
            "size_bytes": path.stat().st_size if path.exists() else None,
        }
    host, path = parts
    proc = subprocess.run(
        ["xrdfs", host, "stat", path],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode != 0:
        return {"url": url, "exists": False, "size_bytes": None, "error": proc.stdout.strip()}
    size = None
    for line in proc.stdout.splitlines():
        fields = line.strip().split()
        if len(fields) >= 2 and fields[0].lower().startswith("size"):
            try:
                size = int(fields[-1])
            except ValueError:
                pass
    return {"url": url, "exists": bool(size and size > 0), "size_bytes": size}


def xrd_mkdir(url):
    parts = remote_parts(url)
    if parts is None:
        Path(url).mkdir(parents=True, exist_ok=True)
        return
    host, path = parts
    subprocess.run(["xrdfs", host, "mkdir", "-p", path], check=True)


def stage_out(local_path, destination):
    if destination.startswith("root://"):
        xrd_mkdir(destination.rsplit("/", 1)[0])
        subprocess.run(["xrdcp", "--nopbar", "-f", str(local_path), destination], check=True)
    else:
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)


def remove_url(url):
    parts = remote_parts(url)
    if parts is None:
        path = Path(url[len("file:"):] if url.startswith("file:") else url)
        path.unlink(missing_ok=True)
        return
    host, path = parts
    subprocess.run(["xrdfs", host, "rm", path], check=True)


def read_json_url(url):
    if not url:
        return None
    parts = remote_parts(url)
    if parts is None:
        path = Path(url[len("file:"):] if url.startswith("file:") else url)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
    fd, local_name = tempfile.mkstemp(prefix="inventory_manifest_", suffix=".json", dir="/tmp")
    os.close(fd)
    try:
        proc = subprocess.run(
            ["xrdcp", "--nopbar", "-f", url, local_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            return None
        return json.loads(Path(local_name).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    finally:
        Path(local_name).unlink(missing_ok=True)


def annotate(records, key):
    annotated = []
    for record in records:
        item = dict(record)
        item[key + "_stat"] = stat_url(item.get(key, ""))
        annotated.append(item)
    return annotated


def annotate_actual_counts(blocks):
    annotated = []
    for block in blocks:
        item = dict(block)
        payload = read_json_url(item.get("processing_manifest_url", ""))
        item["processing_manifest"] = payload
        item["actual_miniaod_events"] = (
            payload.get("actual_miniaod_events") if isinstance(payload, dict) else None
        )
        annotated.append(item)
    return annotated


def processing_manifest_is_eligible(payload):
    if not isinstance(payload, dict) or payload.get("failure_reason"):
        return False
    if payload.get("status") == "ok":
        return True
    return (
        payload.get("status") == "partial"
        and payload.get("merge_eligible") is True
        and int(payload.get("actual_miniaod_events") or 0) > 0
    )


def main():
    cfg = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    output_url = cfg["output_url"]
    blocks = annotate_actual_counts(annotate(cfg.get("blocks", []), "miniaod_url"))
    merge_groups = annotate(cfg.get("merge_groups", []), "merged_miniaod_url")
    ntuples = annotate(cfg.get("ntuples", []), "ntuple_url")

    missing_blocks = [item["block_index"] for item in blocks if not item["miniaod_url_stat"]["exists"]]
    missing_processing_manifests = [
        item["block_index"] for item in blocks
        if not processing_manifest_is_eligible(item.get("processing_manifest"))
    ]
    partial_processing_manifests = [
        item["block_index"] for item in blocks
        if isinstance(item.get("processing_manifest"), dict)
        and item["processing_manifest"].get("status") == "partial"
        and processing_manifest_is_eligible(item["processing_manifest"])
    ]
    missing_merges = [item["merge_index"] for item in merge_groups if not item["merged_miniaod_url_stat"]["exists"]]
    missing_ntuples = [item["job_id"] for item in ntuples if not item["ntuple_url_stat"]["exists"]]
    audit_passed = not (
        missing_blocks
        or missing_processing_manifests
        or missing_merges
        or missing_ntuples
    )
    status = (
        "partial" if partial_processing_manifests or not audit_passed
        else "ok" if audit_passed
        else "partial"
    )

    cleanup = {
        "requested": bool(cfg.get("cleanup_components", False)),
        "performed": False,
        "removed_component_miniaods": [],
        "post_cleanup_missing": [],
    }
    if cleanup["requested"] and audit_passed:
        for item in blocks:
            remove_url(item["miniaod_url"])
            cleanup["removed_component_miniaods"].append(item["miniaod_url"])
        cleanup["performed"] = True
        cleanup["post_cleanup_missing"] = [
            item["block_index"]
            for item in blocks
            if stat_url(item["miniaod_url"])["exists"]
        ]
        if cleanup["post_cleanup_missing"]:
            status = "cleanup_failed"

    manifest = {
        "tool": "run_subdag_final_inventory",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "campaign": cfg["campaign"],
        "job_index": cfg["job_index"],
        "event_id_scheme": cfg.get("event_id_scheme", "unspecified"),
        "status": status,
        "expected_blocks": len(blocks),
        "missing_blocks": missing_blocks,
        "missing_processing_manifests": missing_processing_manifests,
        "partial_processing_manifests": partial_processing_manifests,
        "audit_passed": audit_passed,
        "actual_miniaod_events": sum(
            int(item.get("actual_miniaod_events") or 0) for item in blocks
        ),
        "missing_merges": missing_merges,
        "missing_ntuples": missing_ntuples,
        "component_cleanup": cleanup,
        "blocks": blocks,
        "merge_groups": merge_groups,
        "ntuples": ntuples,
    }
    manifest_text = json.dumps(manifest, indent=2) + "\n"
    print(manifest_text, end="", flush=True)
    fd, local_name = tempfile.mkstemp(
        prefix="subdag_inventory_", suffix=".json", dir="/tmp"
    )
    os.close(fd)
    local = Path(local_name)
    try:
        local.write_text(manifest_text, encoding="utf-8")
        stage_out(local, output_url)
    finally:
        local.unlink(missing_ok=True)
    if not audit_passed or status == "cleanup_failed":
        raise SystemExit(f"final retained-output audit failed with status={status}")


if __name__ == "__main__":
    main()
PY

echo "=== SubDAG final inventory completed ==="
