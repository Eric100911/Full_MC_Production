#!/usr/bin/env python3
"""Inventory merge groups of a coordinator manifest against EOS sidecars.

Reads the frozen coordinator manifest (task authority) and audits the live EOS
state of every component block, merge group, and ntuple. Classifies each group:
  1 complete through NTUPLE
  2 valid MERGE, missing NTUPLE
  3 five valid components, missing MERGE
  4 waiting for one or more MIX components

Campaign, job index, and the EOS output layout are derived from the coordinator
manifest. Re-runnable: manifests are cached locally under the workspace cache.

Env overrides: INV_EOS, INV_COORD, INV_WORK, INV_WORKERS, INV_TIMEOUT, INV_TMP.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

EOS = os.environ.get("INV_EOS", "root://cceos.ihep.ac.cn:1094")
COORD = Path(os.environ.get("INV_COORD", "/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/"
             "JpsiJpsiPhi_MC_Production_v4_2_full/plan_subdags/"
             "JJP_TPS_MC_v4_1/job_6/coord_manifest_JJP_TPS_MC_v4_1_6.json"))
WORK = Path(os.environ.get("INV_WORK", "/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/"
            "tps_ihep_cutover_20260806T074300Z"))
CACHE = WORK / "inventory" / "manifest_cache"
CACHE.mkdir(parents=True, exist_ok=True)

MAX_WORKERS = int(os.environ.get("INV_WORKERS", "8"))
TIMEOUT = int(os.environ.get("INV_TIMEOUT", "60"))
TMPDIR = Path(os.environ.get("INV_TMP", "/tmp/chiw"))
TMPDIR.mkdir(parents=True, exist_ok=True)

# Derived from the coordinator manifest in main(); read by worker functions.
CAMPAIGN = ""
JOB_PFX = ""


def run_checked(cmd, **kwargs):
    """Run a subprocess with retries on transient fork/resource errors."""
    import time as _time
    last = None
    for attempt in range(4):
        try:
            return subprocess.run(cmd, **kwargs)
        except OSError as exc:  # EAGAIN and friends
            last = exc
            _time.sleep(0.5 * (attempt + 1))
    raise last


def lfn_of(url):
    """Return /store/... path for xrdfs, or None for non-root URLs.

    Tolerates the triple-slash form (root://host///store/...) by collapsing
    leading slashes down to one.
    """
    m = re.match(r"^root://[^/]+/(.*)$", url)
    return "/" + m.group(1).lstrip("/") if m else None


def dir_lfn_of(url):
    lfn = lfn_of(url)
    return lfn.rsplit("/", 1)[0] if lfn else None


def fetch_manifest(url, use_cache=True):
    """Return parsed manifest dict (or None) by xrdcp from EOS with local cache."""
    if not url:
        return None
    lfn = lfn_of(url)
    if not lfn:
        return None
    if use_cache:
        cache_path = CACHE / lfn.replace("/", "_")
        if cache_path.is_file():
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
    fd, tmp = tempfile.mkstemp(prefix="inv_", suffix=".json", dir=str(TMPDIR))
    os.close(fd)
    try:
        r = run_checked(["xrdcp", "--nopbar", "-f", url, tmp], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       timeout=TIMEOUT)
        if r.returncode:
            return None
        payload = json.loads(Path(tmp).read_text(encoding="utf-8"))
        if use_cache and isinstance(payload, dict):
            cache_path = CACHE / lfn.replace("/", "_")
            cache_path.write_text(json.dumps(payload), encoding="utf-8")
        return payload
    except (OSError, json.JSONDecodeError, subprocess.TimeoutExpired):
        return None
    finally:
        Path(tmp).unlink(missing_ok=True)


def stat_size(url):
    """Return (exists: bool, size_bytes: int|None) for a root:// URL."""
    lfn = lfn_of(url)
    if not lfn:
        return False, None
    r = run_checked(["xrdfs", EOS + "/", "stat", lfn], check=False,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   universal_newlines=True, timeout=TIMEOUT)
    if r.returncode:
        return False, None
    m = re.search(r"Size:\s*(\d+)", r.stdout)
    return True, int(m.group(1)) if m else None


def processing_ok(p):
    if not isinstance(p, dict) or p.get("failure_reason"):
        return False
    return ((p.get("status") == "ok" and p.get("complete") is True)
            or (p.get("status") == "partial" and p.get("merge_eligible") is True))


def merge_ok(p, planned_url):
    if not isinstance(p, dict) or p.get("failure_reason"):
        return False
    if p.get("output_url") != planned_url:
        return False
    if p.get("status") == "ok":
        return p.get("merge_eligible") is not False
    return p.get("status") == "partial" and p.get("merge_eligible") is True


def ntuple_ok(p, planned_url):
    return (isinstance(p, dict) and not p.get("failure_reason")
            and p.get("status") == "ok" and p.get("ntuple_url") == planned_url)


def main():
    global CAMPAIGN, JOB_PFX
    coord = json.loads(COORD.read_text(encoding="utf-8"))
    campaign = coord["campaign"]
    job_index = int(coord["job_index"])
    CAMPAIGN = campaign
    JOB_PFX = f"JOB{job_index:06d}"
    blocks = {str(b["index"]): b for b in coord["mixed_blocks"]}
    groups = coord["merge_groups"]
    ntuples = {str(t["job_id"]): t for t in coord["ntuples"]}
    n_blocks = len(blocks)
    print(f"campaign={campaign} job={job_index} blocks={n_blocks} groups={len(groups)} "
          f"ntuples={len(ntuples)}", file=sys.stderr)

    # ---- 0. pre-pass: which block dirs exist (one xrdfs ls) ----
    existing = set()
    # miniaod_url layout: <base>/output/<campaign>/<job_id>/output_MINIAOD.root
    miniaod_lfn = lfn_of(coord["mixed_blocks"][0]["miniaod_url"])
    root_lfn = miniaod_lfn.rsplit("/", 2)[0]   # <base>/output/<campaign>
    rl = run_checked(["xrdfs", EOS + "/", "ls", root_lfn], check=False,
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                     universal_newlines=True, timeout=120)
    if rl.returncode == 0:
        for line in rl.stdout.splitlines():
            m = re.search(rf"{JOB_PFX}_BLOCK(\d+)$", line.strip())
            if m:
                existing.add(int(m.group(1)))
    print(f"existing {JOB_PFX} block dirs: {len(existing)}", file=sys.stderr)
    if len(existing) < n_blocks:
        # listing is short of the planned block count; audit everything to be safe
        print("WARNING: existing-dirs listing looks truncated; auditing all blocks", file=sys.stderr)
        existing = set()

    # ---- 1. audit component blocks ----
    block_results = {}
    tasks = []
    for idx, blk in blocks.items():
        if int(idx) not in existing:
            block_results[idx] = {"block_index": int(idx),
                                  "job_id": f"{JOB_PFX}_BLOCK{int(idx):06d}",
                                  "valid": False, "manifest_present": False,
                                  "actual_miniaod_events": None,
                                  "miniaod_exists": False, "miniaod_size": None,
                                  "skipped_no_dir": True}
            continue
        tasks.append(("block", idx, blk))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {}
        for kind, idx, blk in tasks:
            futs[ex.submit(audit_block, blk)] = idx
        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                block_results[idx] = fut.result()
            except Exception as exc:  # noqa: BLE001
                block_results[idx] = {"error": str(exc)}

    # ---- 2. audit merge groups ----
    group_results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {}
        for g in groups:
            futs[ex.submit(audit_group, g, ntuples, block_results)] = g["merge_index"]
        for fut in as_completed(futs):
            gi = futs[fut]
            try:
                group_results[gi] = fut.result()
            except Exception as exc:  # noqa: BLE001
                group_results[gi] = {"error": str(exc)}

    errs = [gr for gr in group_results.values() if "error" in gr]
    for gr in errs[:3]:
        print("GROUP_ERROR", json.dumps(gr, indent=1)[:900], file=sys.stderr)

    # ---- 3. classify ----
    report = {"schema": "tps-merge-inventory-v1",
              "created_at": datetime.now(timezone.utc).isoformat(),
              "campaign": campaign, "coordinator": str(COORD),
              "n_blocks": n_blocks,
              "summary": {"cat1_complete": 0, "cat2_merge_missing_ntuple": 0,
                          "cat3_components_missing_merge": 0,
                          "cat4_waiting_mix": 0, "groups_with_valid_merge": 0,
                          "blocks_valid": 0, "blocks_missing": 0},
              "groups": {}}
    for gi, gr in group_results.items():
        if "error" in gr:
            report["groups"][f"group_{gi:06d}"] = gr
            report.setdefault("summary", {})["group_errors"] = report["summary"].get("group_errors", 0) + 1
            continue
        g = gr
        n_valid_comp = gr["n_valid_components"]
        merge_valid = gr["merge_valid"]
        ntuple_ok_grp = gr.get("ntuple_complete", gr.get("ntuple_ok", False))
        if ntuple_ok_grp:
            cat = 1
        elif merge_valid:
            cat = 2
        elif n_valid_comp >= 5:
            cat = 3
        else:
            cat = 4
        g["category"] = cat
        report["groups"][f"group_{gi:06d}"] = g
        report["summary"][f"cat{cat}_{['complete','merge_missing_ntuple','components_missing_merge','waiting_mix'][cat-1]}"] += 1
        if merge_valid:
            report["summary"]["groups_with_valid_merge"] += 1

    for idx, br in block_results.items():
        if br.get("valid"):
            report["summary"]["blocks_valid"] += 1
        else:
            report["summary"]["blocks_missing"] += 1

    out = WORK / "inventory" / f"{campaign}_job{job_index}_inventory.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"wrote {out}", file=sys.stderr)
    print("SUMMARY:", json.dumps(report["summary"], indent=2))


def audit_block(blk):
    manifest = fetch_manifest(blk["processing_manifest_url"])
    valid = processing_ok(manifest)
    miniaod_exists, miniaod_size = stat_size(blk["miniaod_url"])
    return {"block_index": blk["index"],
            "job_id": blk.get("job_id") or f"{JOB_PFX}_BLOCK{blk['index']:06d}",
            "miniaod_url": blk["miniaod_url"],
            "manifest_url": blk["processing_manifest_url"],
            "manifest_present": manifest is not None,
            "valid": valid,
            "status": manifest.get("status") if manifest else None,
            "complete": manifest.get("complete") if manifest else None,
            "merge_eligible": manifest.get("merge_eligible") if manifest else None,
            "failure_reason": manifest.get("failure_reason") if manifest else None,
            "actual_miniaod_events": manifest.get("actual_miniaod_events") if manifest else None,
            "miniaod_exists": miniaod_exists,
            "miniaod_size": miniaod_size}


def audit_group(g, ntuples, block_results):
    import traceback as _tb
    try:
        return _audit_group(g, ntuples, block_results)
    except Exception as exc:  # noqa: BLE001
        return {"merge_index": g.get("merge_index"), "job_id": g.get("job_id"),
                "error": str(exc), "traceback": _tb.format_exc()}


def _audit_group(g, ntuples, block_results):
    job_id = g["job_id"]
    merge_url = g["merged_miniaod_url"]
    merge_dir = merge_url.rsplit("/", 1)[0]
    merge_manifest_url = merge_dir + f"/merge_manifest_{CAMPAIGN}_{job_id}.json"
    n_valid = 0
    comp_records = []
    for comp in g["components"]:
        bm = block_results.get(str(comp["block_index"]), {})
        comp_records.append({"block_index": comp["block_index"], "job_id": comp["job_id"],
                             "valid": bm.get("valid", False),
                             "actual_miniaod_events": bm.get("actual_miniaod_events"),
                             "manifest_present": bm.get("manifest_present", False)})
        if bm.get("valid"):
            n_valid += 1
    mm = fetch_manifest(merge_manifest_url)
    merge_ok_v = merge_ok(mm, merge_url) if mm else False
    merged_exists, merged_size = stat_size(merge_url)
    nt = ntuples.get(job_id)
    ntuple_url = nt["ntuple_url"] if nt else None
    ntuple_exists, ntuple_size = stat_size(ntuple_url) if ntuple_url else (False, None)
    split_manifest_url = (ntuple_url.rsplit("/", 1)[0]
                          + f"/split_ntuple_manifest_{CAMPAIGN}_{job_id}.json"
                          if ntuple_url else None)
    sm = fetch_manifest(split_manifest_url) if split_manifest_url else None
    ntuple_ok_v = ntuple_ok(sm, ntuple_url) if sm else False
    expected = sum(c["actual_miniaod_events"] or 0 for c in comp_records)
    return {"merge_index": g["merge_index"], "job_id": job_id,
            "components": comp_records,
            "n_valid_components": n_valid,
            "expected_events": expected,
            "merge_valid": merge_ok_v,
            "merge_manifest_status": mm.get("status") if mm else None,
            "merge_manifest_present": mm is not None,
            "merge_actual_events": mm.get("actual_events") if mm else None,
            "merge_url": merge_url,
            "merged_miniaod_exists": merged_exists,
            "merged_miniaod_size": merged_size,
            "ntuple_url": ntuple_url,
            "ntuple_exists": ntuple_exists,
            "ntuple_size": ntuple_size,
            "split_ntuple_manifest_present": sm is not None,
            "ntuple_ok": ntuple_ok_v,
            "ntuple_complete": ntuple_ok_v or (ntuple_exists and (ntuple_size or 0) > 0)}


if __name__ == "__main__":
    main()
