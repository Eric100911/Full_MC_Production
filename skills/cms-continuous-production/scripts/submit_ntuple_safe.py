#!/usr/bin/env python3
"""Submit ntuple HepJobs pinned to AVX2-safe nodes, driven by the job-inventory cache.

Reads ``generated/cs_handoff_jobs.json`` and, per selected job, locates its built
workspace under IHEP ``runs/<ts>/<batch>/``, rebuilds the proxy bundle, then submits
every ntuple task with ``-wn`` pinned round-robin across the 11 AVX2-safe nodes
(ams017/018/019/020 + cws047/048/049/051/052/053/054). Avoids the ~50% rc=132 waste
seen when ntuple runs unconstrained onto Ivy Bridge cws050/cws055.

Usage:
  python3 submit_ntuple_safe.py --campaign JJP_SPS_CS --job-index 0 [--dry-run]
  python3 submit_ntuple_safe.py --all [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

AFS_BASE = Path("/afs/cern.ch/user/c/chiw/condor/Full_MC_Production")
CACHE = AFS_BASE / "generated" / "cs_handoff_jobs.json"

IH_PFX = "/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production"
SAFE_NODES = ["ams017", "ams018", "ams019", "ams020",
              "cws047", "cws048", "cws049", "cws051", "cws052", "cws053", "cws054"]

SSH = ["ssh", "-o", "ConnectTimeout=15", "ihep"]


def ssh(cmd: str) -> str:
    r = subprocess.run(SSH + [cmd], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       universal_newlines=True)
    return r.stdout


def load_jobs() -> list[dict]:
    return json.loads(CACHE.read_text(encoding="utf-8"))


def select_jobs(jobs, campaign, job_index, all_jobs):
    if all_jobs:
        return list(jobs)
    return [j for j in jobs
            if j["campaign"] == campaign and j["job_index"] == job_index]


def find_workspace(batch: str) -> str | None:
    """Return the newest runs/<ts>/<batch> workspace path on IHEP, or None."""
    out = ssh(f"ls -td {IH_PFX}/runs/*/{batch} 2>/dev/null | head -1").strip()
    return out or None


def submit_job(job, dry_run, proxy):
    batch = f"{job['campaign'].lower()}_job{job['job_index']}"
    n = job["merge_groups"]  # ntuple count == merge group count
    ws = find_workspace(batch)
    if not ws:
        print(f"[skip] {job['campaign']} job_{job['job_index']}: workspace not found "
              f"for batch {batch}", file=sys.stderr)
        return
    if dry_run:
        print(f"[dry-run] {job['campaign']} job_{job['job_index']} -> {ws} "
              f"ntuples={n} safe_nodes={len(SAFE_NODES)}")
        return

    # rebuild proxy bundle inside the workspace
    script = (
        f'cd "{ws}" && '
        f'PROXY_SOURCE="{proxy}"; '
        f'TMP=$(mktemp -d "$PWD/.proxy.XXXXXX"); '
        f'mkdir -p "$TMP/credentials"; '
        f'install -m 600 "$PROXY_SOURCE" "$TMP/credentials/x509_user_proxy"; '
        f'tar -czf "$PWD/bundles/proxy_bundle.tar.gz.tmp" -C "$TMP" credentials; '
        f'chmod 600 "$PWD/bundles/proxy_bundle.tar.gz.tmp"; '
        f'mv -f "$PWD/bundles/proxy_bundle.tar.gz.tmp" "$PWD/bundles/proxy_bundle.tar.gz"; '
        f'rm -rf "$TMP"; '
        f'mkdir -p "$PWD/logs/ntuple/{job["campaign"]}"'
    )
    ssh(script)

    # submit each ntuple task pinned round-robin to a safe node
    run_script = f'{ws}/scripts/run_ntuple_{job["campaign"]}.sh'
    ok = 0
    for idx in range(n):
        node = f"{SAFE_NODES[idx % len(SAFE_NODES)]}.ihep.ac.cn"
        cmd = (f'export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH; '
               f'hep_sub "{run_script}" -g cms -gwn CMS -wt mid '
               f'-wn "{node}" -argu "{idx}" -n 1 '
               f'-o "{ws}/logs/ntuple/{job["campaign"]}/ntuple_{idx}.out" '
               f'-e "{ws}/logs/ntuple/{job["campaign"]}/ntuple_{idx}.err" '
               f'>/dev/null 2>&1 && echo ok')
        out = ssh(cmd).strip()
        if out == "ok":
            ok += 1
        else:
            print(f"[fail] {job['campaign']} job_{job['job_index']} idx={idx} -> {node}",
                  file=sys.stderr)
    print(f"[submitted] {job['campaign']} job_{job['job_index']}: {ok}/{n} ntuples")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--campaign", default="", help="filter by campaign")
    ap.add_argument("--job-index", type=int, default=-1, help="filter by job index")
    ap.add_argument("--all", action="store_true", help="submit every cached job")
    ap.add_argument("--dry-run", action="store_true", help="print actions, don't submit")
    ap.add_argument("--proxy", default=os.environ.get("X509_USER_PROXY", ""),
                    help="current proxy path on the IHEP login node")
    args = ap.parse_args()

    if not args.all and (not args.campaign or args.job_index < 0):
        ap.error("use --all, or --campaign X --job-index N")

    jobs = load_jobs()
    sel = select_jobs(jobs, args.campaign, args.job_index, args.all)
    if not sel:
        ap.error("no jobs matched the selection")
    if not args.proxy:
        ap.error("--proxy or X509_USER_PROXY is required")
    for j in sel:
        submit_job(j, args.dry_run, args.proxy)


if __name__ == "__main__":
    main()
