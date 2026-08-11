#!/usr/bin/env python3
"""Recover split_ntuple_manifest sidecars for a split workspace.

The post-container xrdfs mkdir in run_ihep_split_task.sh fails with
'Operation not permitted', so the split_ntuple_manifest is not staged even
though the ntuple ROOT was uploaded. This script verifies every ntuple ROOT is
positive-size on EOS, then writes the exact planned sidecar from an
authenticated login node (run with X509_USER_PROXY set).

Usage: python3 recover_ntuple_sidecars.py <workspace> [--force]
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

EOS = "root://cceos.ihep.ac.cn:1094"


def run(cmd):
    return subprocess.run(cmd, check=False, capture_output=True,
                          universal_newlines=True, timeout=60)


def lfn_of(url):
    m = re.match(r"^root://[^/]+/(.*)$", url)
    return "/" + m.group(1) if m else None


def stat_ok(url):
    lfn = lfn_of(url)
    if not lfn:
        return False
    r = run(["xrdfs", EOS + "/", "stat", lfn])
    if r.returncode:
        return False
    m = re.search(r"Size:\s*(\d+)", r.stdout)
    return bool(m) and int(m.group(1)) > 0


def main():
    workspace = Path(sys.argv[1]).resolve()
    force = "--force" in sys.argv[2:]
    root_meta = json.loads((workspace / "split_workspace.json").read_text(encoding="utf-8"))
    recovered, missing_root = [], []
    for campaign, info in root_meta["campaigns"].items():
        tasks_path = Path(info["ntuple_tasks"])
        tasks = json.loads(tasks_path.read_text(encoding="utf-8"))
        for task in tasks:
            url = task["ntuple_url"]
            manifest_url = task["manifest_url"]
            if not force:
                lfn = lfn_of(manifest_url)
                if lfn and stat_ok(manifest_url):
                    continue  # already present
            if not stat_ok(url):
                missing_root.append((task["job_id"], url))
                continue
            payload = {"status": "ok", "campaign": task["campaign"],
                       "job_id": task["job_id"], "ntuple_url": url}
            fd, tmp = tempfile.mkstemp(suffix=".json", dir="/tmp")
            os.close(fd)
            Path(tmp).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            r = subprocess.run(["xrdcp", "--nopbar", "-f", tmp, manifest_url],
                               check=False, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=90)
            Path(tmp).unlink(missing_ok=True)
            if r.returncode == 0:
                recovered.append(task["job_id"])
            else:
                print(f"  FAILED upload {task['job_id']}: {manifest_url}", file=sys.stderr)
    print(f"recovered {len(recovered)} sidecars")
    for j in recovered:
        print("  OK", j)
    if missing_root:
        print(f"{len(missing_root)} ntuples missing/empty ROOT (need rerun):")
        for j, u in missing_root:
            print("  MISSING", j, u)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
