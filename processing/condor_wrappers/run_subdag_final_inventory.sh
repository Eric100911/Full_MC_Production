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


def annotate(records, key):
    annotated = []
    for record in records:
        item = dict(record)
        item[key + "_stat"] = stat_url(item.get(key, ""))
        annotated.append(item)
    return annotated


def main():
    cfg = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    output_url = cfg["output_url"]
    blocks = annotate(cfg.get("blocks", []), "miniaod_url")
    merge_groups = annotate(cfg.get("merge_groups", []), "merged_miniaod_url")
    ntuples = annotate(cfg.get("ntuples", []), "ntuple_url")

    missing_blocks = [item["block_index"] for item in blocks if not item["miniaod_url_stat"]["exists"]]
    missing_merges = [item["merge_index"] for item in merge_groups if not item["merged_miniaod_url_stat"]["exists"]]
    missing_ntuples = [item["job_id"] for item in ntuples if not item["ntuple_url_stat"]["exists"]]
    status = "ok" if not missing_blocks and not missing_merges and not missing_ntuples else "partial"

    manifest = {
        "tool": "run_subdag_final_inventory",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "campaign": cfg["campaign"],
        "job_index": cfg["job_index"],
        "event_id_scheme": cfg.get("event_id_scheme", "unspecified"),
        "status": status,
        "expected_blocks": len(blocks),
        "missing_blocks": missing_blocks,
        "missing_merges": missing_merges,
        "missing_ntuples": missing_ntuples,
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


if __name__ == "__main__":
    main()
PY

echo "=== SubDAG final inventory completed ==="
