#!/usr/bin/env python3
"""Build CS-campaign IHEP handoff workspaces, driven by the job-inventory cache.

Reads ``generated/cs_handoff_jobs.json`` (scanned once into a cache — the single
source of the job list, never hardcoded) and, per selected job, (1) copies the
coordinator manifest to the IHEP repo, then (2) invokes the site-local
``ihep_build_batch.py``. Builds run in the background on IHEP by default because
the embedded mix audit walks every component block sidecar (~1h for 2000 blocks).

Usage:
  python3 build_cs_handoff.py --campaign JJP_SPS_CS --job-index 0 [--dry-run]
  python3 build_cs_handoff.py --all [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

AFS_BASE = Path("/afs/cern.ch/user/c/chiw/condor/Full_MC_Production")
CACHE = AFS_BASE / "generated" / "cs_handoff_jobs.json"

IH_PFX = "/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production"
IH_REPO = f"{IH_PFX}/repo"
BUILD = f"{IH_REPO}/skills/cms-continuous-production/scripts/ihep_build_batch.py"
PROXY = f"{IH_PFX}/credentials/20260811T141811Z/credentials/x509_user_proxy"
LOG_DIR = f"{IH_PFX}/build_logs"  # persistent; /tmp is periodically cleaned on IHEP

SSH = ["ssh", "-o", "ConnectTimeout=15", "-o", "ControlMaster=no", "ihep"]


def load_jobs() -> list[dict]:
    return json.loads(CACHE.read_text(encoding="utf-8"))


def select_jobs(jobs, campaign, job_index, all_jobs):
    sel = []
    for j in jobs:
        if all_jobs:
            sel.append(j)
        elif campaign and j["campaign"] == campaign and j["job_index"] == job_index:
            sel.append(j)
    return sel


def run(cmd, check=True):
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       universal_newlines=True)
    if check and r.returncode != 0:
        print(r.stdout, file=sys.stderr)
        raise SystemExit(f"command failed (rc={r.returncode}): {' '.join(cmd)}")
    return r


def build_job(job, dry_run):
    batch = f"{job['campaign'].lower()}_job{job['job_index']}"
    groups = ",".join(str(i) for i in range(job["merge_groups"]))
    remote_dir = f"{IH_REPO}/handoffs_{batch}"
    remote_manifest = f"{remote_dir}/coord_manifest_{job['campaign']}_{job['job_index']}.json"
    log = f"{LOG_DIR}/build_{batch}.log"

    if dry_run:
        print(f"[dry-run] {job['campaign']} job_{job['job_index']} -> batch={batch} "
              f"groups={job['merge_groups']} manifest={job['coord_manifest']}")
        return

    # 1. copy manifest to IHEP repo
    run(SSH + [f"mkdir -p {remote_dir}"])
    r = subprocess.run(["scp", "-o", "ConnectTimeout=15", "-o", "ControlMaster=no",
                        job["coord_manifest"], f"ihep:{remote_manifest}"],
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       universal_newlines=True)
    if r.returncode != 0:
        print(r.stdout, file=sys.stderr)
        raise SystemExit(f"scp manifest failed: {job['coord_manifest']}")

    # 2. background build on IHEP (mix audit walks all blocks, ~1h)
    run(SSH + [f"mkdir -p {LOG_DIR}"])
    inner = (f"cd {IH_REPO} && python3 {BUILD} --coord {remote_manifest} "
             f"--ih-pfx {IH_PFX} --proxy {PROXY} {batch} {groups}")
    run(SSH + [f"nohup bash -lc '{inner}' > {log} 2>&1 & echo started"],
        check=True)
    print(f"[started] {job['campaign']} job_{job['job_index']} -> {batch} (log {log})")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--campaign", default="", help="filter by campaign")
    ap.add_argument("--job-index", type=int, default=-1, help="filter by job index")
    ap.add_argument("--all", action="store_true", help="build every cached job")
    ap.add_argument("--dry-run", action="store_true", help="print actions, don't run")
    args = ap.parse_args()

    if not args.all and (not args.campaign or args.job_index < 0):
        ap.error("use --all, or --campaign X --job-index N")

    jobs = load_jobs()
    sel = select_jobs(jobs, args.campaign, args.job_index, args.all)
    if not sel:
        ap.error("no jobs matched the selection")
    for j in sel:
        build_job(j, args.dry_run)
    print(f"done: {len(sel)} job(s) {'(dry-run)' if args.dry_run else 'started'}")


if __name__ == "__main__":
    main()
