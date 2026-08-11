#!/usr/bin/env python3
"""Build an IHEP-side merge-ntuple handoff for a batch of groups (fast, site-local).

Runs entirely on IHEP: filters the coordinator manifest, runs audit-split --stage mix,
generates the merge-ntuple workspace, and runs the pre-submit merge audit.

The coordinator manifest is the single authority: campaign, job index, and the EOS
output layout are derived from it, so no campaign-specific constants are needed.

Usage:
  python3 ihep_build_batch.py --coord <coord_manifest.json> <batch> <groups>
  python3 ihep_build_batch.py --coord ... --ts 20260806T074300Z --batch ...

Site paths default from environment (IH_PFX, IH_TS, IH_EOS, IH_LHE_POOL_LFN) and can
be overridden per flag. Run from the Full_MC_Production repo on the IHEP login node.
"""
import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_IH_PFX = "/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production"
DEFAULT_EOS_HOST = "cceos.ihep.ac.cn:1094"
DEFAULT_LHE_POOL_LFN = "/store/user/chiw/MC_Production_v3"


def root_parts(url):
    """Split a root:// URL into (host, /lfn); tolerate the triple-slash form."""
    m = re.match(r"^root://([^/]+)/(.*)$", url)
    if not m:
        raise ValueError(f"not a root:// URL: {url}")
    return m.group(1), "/" + m.group(2).lstrip("/")


def default_target_eos_base(coord, host):
    """Derive target_eos_base from the first mixed block's miniaod_url.

    miniaod_url layout: <base>/output/<campaign>/<job_id>/output_MINIAOD.root,
    so stripping the trailing campaign and job components yields <base>/output,
    and dropping the literal /output segment gives the storage base.
    """
    _, lfn = root_parts(coord["mixed_blocks"][0]["miniaod_url"])
    campaign_out = lfn.rsplit("/", 2)[0]          # <base>/output/<campaign>
    base = campaign_out.rsplit("/output", 1)[0]   # <base>
    return f"root://{host}///{base.lstrip('/')}"


def storage_from_coord(coord, target_eos_base, host, lhe_pool_lfn):
    """Build the storage config injected into the filtered coordinator manifest.

    target_eos_base is derived from the coordinator's output URLs; lhe_pool_lfn
    covers the legacy existing-LHE base (not present in the coordinator manifest).
    """
    eos_output = f"{target_eos_base}/output"
    pool_lfn = lhe_pool_lfn.lstrip("/")
    return {
        "eos_redirector": host,
        "eos_lfn_base": lhe_pool_lfn,
        "eos_base": f"root://{host}///{pool_lfn}",
        "eos_host": host,
        "eos_xrdfs_target": host,
        "eos_path_base": lhe_pool_lfn,
        "existing_lhe_base": f"root://{host}///{pool_lfn}",
        "target_eos_base": target_eos_base,
        "eos_output": eos_output,
        "generated_lhe_base": f"{target_eos_base}/lhe_pools",
        "output_subdir": "output",
        "lhe_pool_directories": {},
    }


def main():
    parser = argparse.ArgumentParser(
        description="Build an IHEP-side merge-ntuple handoff for a batch of groups.")
    parser.add_argument("batch", help="batch name (e.g. batch21)")
    parser.add_argument("groups", help="comma-separated merge group indices")
    parser.add_argument("--coord", required=True,
                        help="path to the frozen coordinator manifest (authority)")
    parser.add_argument("--ih-pfx", default=os.environ.get("IH_PFX", DEFAULT_IH_PFX),
                        help="IHEP scratchfs2 workspace root")
    parser.add_argument("--ts", default=os.environ.get("IH_TS", ""),
                        help="workspace timestamp (inbox/runs/credentials layout); "
                             "defaults to a fresh UTC stamp")
    parser.add_argument("--repo", default="", help="Full_MC_Production clone on IHEP")
    parser.add_argument("--proxy", default="", help="x509 proxy for worker bundles")
    parser.add_argument("--campaign", default="", help="override coordinator campaign")
    parser.add_argument("--job-index", type=int, default=-1,
                        help="override coordinator job index")
    parser.add_argument("--target-eos-base", default="",
                        help="override derived EOS storage base")
    parser.add_argument("--eos-host", default=os.environ.get("IH_EOS", DEFAULT_EOS_HOST),
                        help="IHEP EOS XRootD host")
    parser.add_argument("--lhe-pool-base",
                        default=os.environ.get("IH_LHE_POOL_LFN", DEFAULT_LHE_POOL_LFN),
                        help="legacy existing-LHE pool LFN base")
    parser.add_argument("--analysis-type", default="JJP",
                        help="analysis type stamped into the filtered manifest")
    parser.add_argument("--cmssw15-tarball",
                        default="common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz")
    parser.add_argument("--hepjob-group", default="cms")
    parser.add_argument("--merge-walltime", default="short",
                        choices=("test", "short", "mid", "long", "special"))
    parser.add_argument("--ntuple-walltime", default="mid",
                        choices=("test", "short", "mid", "long", "special"))
    parser.add_argument("--output-mode", default="mix-only")
    parser.add_argument("--efficiency-ntuple", action="store_true")
    parser.add_argument("--no-cleanup", dest="cleanup", action="store_false", default=True)
    args = parser.parse_args()

    coord = json.loads(Path(args.coord).read_text(encoding="utf-8"))
    campaign = args.campaign or coord["campaign"]
    job_index = args.job_index if args.job_index >= 0 else int(coord["job_index"])
    target_eos_base = args.target_eos_base or default_target_eos_base(coord, args.eos_host)
    storage = storage_from_coord(coord, target_eos_base, args.eos_host, args.lhe_pool_base)
    ts = args.ts or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    ih_pfx = args.ih_pfx
    repo = args.repo or f"{ih_pfx}/repo"
    proxy = args.proxy or f"{ih_pfx}/credentials/{ts}/credentials/x509_user_proxy"

    groups = [int(x) for x in args.groups.split(",")]
    sel_groups = [g for g in coord["merge_groups"] if g["merge_index"] in groups]
    sel_job_ids = {g["job_id"] for g in sel_groups}
    sel_ntuples = [t for t in coord["ntuples"] if t["job_id"] in sel_job_ids]
    sel_blocks = set()
    for g in sel_groups:
        for c in g["components"]:
            sel_blocks.add(c["block_index"])

    filtered = dict(coord)
    filtered["merge_groups"] = sel_groups
    filtered["ntuples"] = sel_ntuples
    filtered["mixed_blocks"] = [b for b in coord["mixed_blocks"] if b["index"] in sel_blocks]
    filtered["output_mode"] = args.output_mode
    filtered["analysis_type"] = args.analysis_type
    filtered["efficiency_ntuple"] = args.efficiency_ntuple
    filtered["cleanup"] = args.cleanup
    filtered["target_eos_base"] = target_eos_base
    filtered["storage"] = storage

    handoff = Path(f"{ih_pfx}/handoffs/{args.batch}")
    manifest_path = handoff / f"plan_subdags/{campaign}/job_{job_index}" \
        / f"coord_manifest_{campaign}_{job_index}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(filtered, indent=2))

    run_dir = Path(f"{ih_pfx}/runs/{ts}/{args.batch}")
    run_dir.mkdir(parents=True, exist_ok=True)
    inbox_dir = Path(f"{ih_pfx}/inbox/{ts}/{args.batch}")
    inbox_dir.mkdir(parents=True, exist_ok=True)
    split_manifest = inbox_dir / "split-manifest.json"

    env = dict(os.environ)
    env["X509_USER_PROXY"] = proxy

    # 1. mix audit (site-local fast)
    r = subprocess.run(["python3", "dag_generator.py", "audit-split", "--stage", "mix",
                        "--workspace", str(handoff), "--output", str(split_manifest)],
                       cwd=repo, env=env, capture_output=True, text=True)
    if r.returncode:
        print("MIX AUDIT FAILED:", r.stderr[-500:])
        return 1
    print(f"[mix audit ok] {args.batch}")

    # 2. generate merge-ntuple workspace
    r = subprocess.run(["python3", "dag_generator.py", "generate", "--output-mode", "merge-ntuple",
                        "--split-manifest", str(split_manifest), "--output-dir", str(run_dir),
                        "--cmssw15-runtime-tarball", args.cmssw15_tarball,
                        "--hepjob-group", args.hepjob_group,
                        "--hepjob-merge-walltime", args.merge_walltime,
                        "--hepjob-ntuple-walltime", args.ntuple_walltime,
                        "--proxy-path", proxy],
                       cwd=repo, env=env, capture_output=True, text=True)
    if r.returncode:
        print("WORKSPACE GEN FAILED:", r.stderr[-500:])
        return 1
    print(f"[workspace ok] {run_dir}")

    # 3. pre-submit merge audit
    r = subprocess.run(["python3", "dag_generator.py", "audit-split", "--stage", "merge",
                        "--workspace", str(run_dir)],
                       cwd=repo, env=env, capture_output=True, text=True)
    try:
        d = json.loads(r.stdout)
        c = d["campaigns"][campaign]
        print(f"[pre-submit merge audit] complete={d.get('complete')} "
              f"passed={c['passed']} failed={c['failed']}")
    except (ValueError, KeyError):
        print("[pre-submit merge audit] parse failed:", r.stdout[-300:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
