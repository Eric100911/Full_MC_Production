#!/usr/bin/env python3
"""Build an IHEP-side TPS merge-ntuple handoff for a batch of groups (fast, site-local).

Runs entirely on IHEP: filters the coordinator manifest, runs audit-split --stage mix,
generates the merge-ntuple workspace, and runs the pre-submit merge audit.

Usage: python3 ihep_build_batch.py <batch_name> <comma-separated-groups>
"""
import json
import os
import subprocess
import sys
from pathlib import Path

TS = "20260806T074300Z"
IH_PFX = "/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production"
COORD_PATH = f"{IH_PFX}/tps_cutover/inventory/coord_manifest_JJP_TPS_MC_v4_1_6.json"
REPO = f"{IH_PFX}/repo"
PROXY = f"{IH_PFX}/credentials/{TS}/credentials/x509_user_proxy"

STORAGE = {
    "eos_redirector": "cceos.ihep.ac.cn:1094",
    "eos_lfn_base": "/store/user/chiw/MC_Production_v3",
    "eos_base": "root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3",
    "eos_host": "cceos.ihep.ac.cn:1094",
    "eos_xrdfs_target": "cceos.ihep.ac.cn:1094",
    "eos_path_base": "/store/user/chiw/MC_Production_v3",
    "existing_lhe_base": "root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3",
    "target_eos_base": "root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full",
    "eos_output": "root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/output",
    "generated_lhe_base": "root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/lhe_pools",
    "output_subdir": "output",
    "lhe_pool_directories": {},
}


def main():
    batch = sys.argv[1]
    groups = [int(x) for x in sys.argv[2].split(",")]
    coord = json.loads(Path(COORD_PATH).read_text(encoding="utf-8"))

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
    filtered["output_mode"] = "mix-only"
    filtered["analysis_type"] = "JJP"
    filtered["efficiency_ntuple"] = False
    filtered["cleanup"] = True
    filtered["target_eos_base"] = "root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full"
    filtered["storage"] = STORAGE

    handoff = Path(f"{IH_PFX}/handoffs/{batch}")
    manifest_path = handoff / "plan_subdags/JJP_TPS_MC_v4_1/job_6/coord_manifest_JJP_TPS_MC_v4_1_6.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(filtered, indent=2))

    run_dir = Path(f"{IH_PFX}/runs/{TS}/{batch}")
    run_dir.mkdir(parents=True, exist_ok=True)
    split_manifest = f"{IH_PFX}/inbox/{TS}/{batch}/split-manifest.json"
    Path(f"{IH_PFX}/inbox/{TS}/{batch}").mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env["X509_USER_PROXY"] = PROXY

    # 1. mix audit (site-local fast)
    r = subprocess.run(["python3", "dag_generator.py", "audit-split", "--stage", "mix",
                        "--workspace", str(handoff), "--output", split_manifest],
                       cwd=REPO, env=env, capture_output=True, text=True)
    if r.returncode:
        print("MIX AUDIT FAILED:", r.stderr[-500:])
        return 1
    print(f"[mix audit ok] {batch}")

    # 2. generate merge-ntuple workspace
    r = subprocess.run(["python3", "dag_generator.py", "generate", "--output-mode", "merge-ntuple",
                        "--split-manifest", split_manifest, "--output-dir", str(run_dir),
                        "--cmssw15-runtime-tarball", "common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz",
                        "--hepjob-group", "cms", "--hepjob-merge-walltime", "short",
                        "--hepjob-ntuple-walltime", "mid", "--proxy", PROXY],
                       cwd=REPO, env=env, capture_output=True, text=True)
    if r.returncode:
        print("WORKSPACE GEN FAILED:", r.stderr[-500:])
        return 1
    print(f"[workspace ok] {run_dir}")

    # 3. pre-submit merge audit
    r = subprocess.run(["python3", "dag_generator.py", "audit-split", "--stage", "merge",
                        "--workspace", str(run_dir)],
                       cwd=REPO, env=env, capture_output=True, text=True)
    try:
        d = json.loads(r.stdout)
        print(f"[pre-submit merge audit] complete={d.get('complete')} "
              f"passed={d['campaigns']['JJP_TPS_MC_v4_1']['passed']} "
              f"failed={d['campaigns']['JJP_TPS_MC_v4_1']['failed']}")
    except (ValueError, KeyError):
        print("[pre-submit merge audit] parse failed:", r.stdout[-300:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
