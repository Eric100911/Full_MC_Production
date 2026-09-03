#!/usr/bin/env python3
"""Audit CS-campaign handoff progress via tps_cutover_inventory.py, cache-driven.

Reads ``generated/cs_handoff_jobs.json`` (the scanned job-inventory cache) and, per
selected job, runs the existing ``tps_cutover_inventory.py`` on IHEP (fast EOS
access) with INV_COORD pointed at the copied manifest. Reuses the inventory tool —
no per-campaign EOS-layout hardcoding.

Usage:
  python3 audit_cs_handoff.py --campaign JJP_SPS_CS --job-index 0
  python3 audit_cs_handoff.py --all
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
INVENTORY = f"{IH_REPO}/skills/cms-continuous-production/scripts/tps_cutover_inventory.py"
PROXY = f"{IH_PFX}/credentials/20260811T141811Z/credentials/x509_user_proxy"
WORK = f"{IH_PFX}/inventory_cs"

SSH = ["ssh", "-o", "ConnectTimeout=15", "ihep"]


def load_jobs() -> list[dict]:
    return json.loads(CACHE.read_text(encoding="utf-8"))


def select_jobs(jobs, campaign, job_index, all_jobs):
    if all_jobs:
        return list(jobs)
    return [j for j in jobs
            if j["campaign"] == campaign and j["job_index"] == job_index]


def audit_job(job):
    batch = f"{job['campaign'].lower()}_job{job['job_index']}"
    remote_manifest = (f"{IH_REPO}/handoffs_{batch}/"
                       f"coord_manifest_{job['campaign']}_{job['job_index']}.json")
    env = (f"INV_COORD={remote_manifest} INV_WORK={WORK} INV_WORKERS=8 "
           f"INV_TIMEOUT=60 INV_TMP=/tmp/chiw "
           f"X509_USER_PROXY={PROXY}")
    inner = f"cd {IH_REPO} && {env} python3 {INVENTORY}"
    r = subprocess.run(SSH + [inner], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       universal_newlines=True)
    print(f"=== {job['campaign']} job_{job['job_index']} ===")
    # print only the SUMMARY block from the inventory tool
    lines = r.stdout.splitlines()
    in_summary = False
    for line in lines:
        if line.startswith("SUMMARY:"):
            in_summary = True
            continue
        if in_summary and line.strip():
            print(line)
    if r.returncode != 0:
        print(f"(inventory exited rc={r.returncode})", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--campaign", default="", help="filter by campaign")
    ap.add_argument("--job-index", type=int, default=-1, help="filter by job index")
    ap.add_argument("--all", action="store_true", help="audit every cached job")
    args = ap.parse_args()

    if not args.all and (not args.campaign or args.job_index < 0):
        ap.error("use --all, or --campaign X --job-index N")

    jobs = load_jobs()
    sel = select_jobs(jobs, args.campaign, args.job_index, args.all)
    if not sel:
        ap.error("no jobs matched the selection")
    for j in sel:
        audit_job(j)


if __name__ == "__main__":
    main()
