#!/usr/bin/env python3
"""Streaming, columnar kinematic summaries and duplicate checks for LHE files."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import random
import sys
import tempfile
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

BIN_EDGES = {
    "jpsi_pt": (0, 1, 2, 3, 5, 8, 12, 20, 40, 100),
    "jpsi_y": (-6, -4, -3, -2, -1, 0, 1, 2, 3, 4, 6),
    "pair_pt": (0, 1, 2, 3, 5, 8, 12, 20, 40, 100),
    "pair_mass": (0, 3, 4, 5, 6, 8, 10, 15, 25, 50, 100),
    "delta_y": (0, .25, .5, 1, 1.5, 2, 3, 4, 6, 10),
}


def imports():
    try:
        import awkward as ak
        import numpy as np
        import pylhe
    except ImportError as exc:
        raise RuntimeError("Install/use pylhe, numpy, and awkward in the configured LCG environment") from exc
    return pylhe, np, ak


def file_rows(inventory, pool, filters):
    rows = inventory["pools"][pool]["files"]
    out = []
    for row in rows:
        if row.get("status") != "ok":
            continue
        if filters.get("namespace") and row.get("source_namespace") not in filters["namespace"]:
            continue
        if filters.get("seed_min") is not None and row.get("seed", -1) < filters["seed_min"]:
            continue
        if filters.get("seed_max") is not None and row.get("seed", -1) > filters["seed_max"]:
            continue
        out.append(row)
    return out


def choose(rows, sample, seed):
    if sample is None:
        return rows
    rng = random.Random(seed)
    if isinstance(sample, float):
        n = max(1, math.ceil(len(rows) * sample)) if rows else 0
    else:
        n = min(int(sample), len(rows))
    # Shuffle within source namespace strata; retain deterministic ordering afterward.
    strata = {}
    for row in rows:
        strata.setdefault(row.get("source_namespace", ""), []).append(row)
    if not rows or n == 0:
        return []
    # Hamilton (largest remainder) apportionment guarantees exact total n.
    quotas = {key: n * len(group) / len(rows) for key, group in strata.items()}
    allocation = {key: int(math.floor(quota)) for key, quota in quotas.items()}
    remaining = n - sum(allocation.values())
    ranked = sorted(strata, key=lambda key: (-(quotas[key] - allocation[key]), key))
    for key in ranked[:remaining]:
        allocation[key] += 1
    selected = []
    for key, group in strata.items():
        selected.extend(rng.sample(group, allocation[key]))
    return sorted(selected, key=lambda r: (r.get("source_namespace", ""), r.get("seed", -1), r["path"]))


def open_input(path):
    if path.startswith("root://"):
        # Keep endpoint and triple slash path intact. Stage one file at a time.
        suffix = ".lhe.gz" if urlparse(path).path.endswith(".lhe.gz") else ".lhe"
        with tempfile.NamedTemporaryFile(prefix="lhe-audit-", suffix=suffix, dir="/tmp/chiw", delete=False) as tmp:
            staged = tmp.name
        import subprocess
        xrdcp_env = os.environ.copy()
        xrdcp_env.pop("LD_LIBRARY_PATH", None)
        xrdcp_env.pop("PYTHONPATH", None)
        try:
            subprocess.run(["/usr/bin/xrdcp", "--nopbar", "--force", path, staged],
                           check=True, env=xrdcp_env)
            return staged, True
        except Exception:
            Path(staged).unlink(missing_ok=True)
            raise
    return path, False


def resolve_input_path(path, base, lfn_endpoint=None):
    if lfn_endpoint and path.startswith("/store/"):
        endpoint = urlparse(lfn_endpoint)
        if endpoint.scheme != "root" or not endpoint.netloc or endpoint.path not in ("", "/"):
            raise ValueError("lfn_endpoint must be a root://host:port endpoint without an LFN path")
        return lfn_endpoint.rstrip("/") + "//" + path
    if path.startswith("root://") or Path(path).is_absolute():
        return path
    return str(Path(base, path)) if base else path


def particle_digest(event):
    h = hashlib.sha256()
    for p in sorted((p for p in event.particles if p.status == 1), key=lambda x: (x.id, x.status, x.px, x.py, x.pz, x.e)):
        h.update(("%d,%d," % (p.id, p.status)).encode())
        h.update((",".join(float(getattr(p, k)).hex() for k in ("px", "py", "pz", "e", "m"))).encode())
        h.update(b";")
    return h.hexdigest()


def event_weight(event):
    for attr in ("weight", "weights"):
        val = getattr(event, attr, None)
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, (list, tuple)) and val:
            return float(val[0])
    return None


def analyze_file(row, base, chunk_size=10000, lfn_endpoint=None):
    pylhe, np, ak = imports()
    source = resolve_input_path(row["path"], base, lfn_endpoint)
    local, cleanup = open_input(source)
    counts = Counter(); hist = {k: np.zeros(len(v)-1, dtype=np.int64) for k,v in BIN_EDGES.items()}
    overflow = Counter(); sign = Counter(); weights = []; dup_hashes = Counter()
    stream_hash = hashlib.sha256()
    sums = {k: [] for k in BIN_EDGES}; event_buf = []
    def consume(events):
        if not events: return
        # awkward jagged records keep event boundaries while numpy performs the calculations.
        ids = ak.Array([[p.id for p in e.particles if p.status == 1 and p.id == 443] for e in events])
        px = ak.Array([[p.px for p in e.particles if p.status == 1 and p.id == 443] for e in events])
        py = ak.Array([[p.py for p in e.particles if p.status == 1 and p.id == 443] for e in events])
        pz = ak.Array([[p.pz for p in e.particles if p.status == 1 and p.id == 443] for e in events])
        en = ak.Array([[p.e for p in e.particles if p.status == 1 and p.id == 443] for e in events])
        counts["events"] += len(events)
        counts["jpsi"] += int(ak.sum(ak.num(ids)))
        per_pt = np.asarray(ak.flatten(np.hypot(px, py)), dtype=float)
        per_y = np.asarray(ak.flatten(0.5 * np.log((en + pz) / (en - pz))), dtype=float)
        values = {"jpsi_pt": per_pt, "jpsi_y": per_y}
        sizes = np.asarray(ak.num(ids))
        pair_idx = np.flatnonzero(sizes == 2)
        if len(pair_idx):
            a,b = px[pair_idx,0],px[pair_idx,1]; c,d = py[pair_idx,0],py[pair_idx,1]
            z1,z2 = pz[pair_idx,0],pz[pair_idx,1]; e1,e2 = en[pair_idx,0],en[pair_idx,1]
            pairpt = np.hypot(a+b,c+d)
            mass = np.sqrt(np.maximum((e1+e2)**2-(a+b)**2-(c+d)**2-(z1+z2)**2,0))
            yy1=.5*np.log((e1+z1)/(e1-z1)); yy2=.5*np.log((e2+z2)/(e2-z2))
            values.update(pair_pt=np.asarray(pairpt), pair_mass=np.asarray(mass), delta_y=np.abs(yy1-yy2))
            counts["events_exactly_two_jpsi"] += len(pair_idx)
        else:
            for k in ("pair_pt","pair_mass","delta_y"): values[k]=np.array([], dtype=float)
        counts["events_without_jpsi"] += int(np.sum(sizes == 0)); counts["events_not_exactly_two_jpsi"] += int(np.sum(sizes != 2))
        for k, vals in values.items():
            finite=vals[np.isfinite(vals)]; sums[k].extend(finite.tolist())
            hist[k] += np.asarray(np.histogram(finite, bins=BIN_EDGES[k])[0], dtype=np.int64)
            overflow[k] += int(np.sum((finite < BIN_EDGES[k][0]) | (finite >= BIN_EDGES[k][-1])))
        counts["jpsi_y_positive"] += int(np.sum(per_y > 0)); counts["jpsi_y_negative"] += int(np.sum(per_y < 0)); counts["jpsi_y_zero"] += int(np.sum(per_y == 0))
        for e in events:
            w=event_weight(e)
            if w is not None:
                weights.append(w); sign["positive"] += w>0; sign["negative"] += w<0; sign["zero"] += w==0
            digest = particle_digest(e)
            dup_hashes[digest] += 1
            digest_bytes = bytes.fromhex(digest)
            stream_hash.update(len(digest_bytes).to_bytes(2, "big"))
            stream_hash.update(digest_bytes)
    try:
        try:
            events = iter(pylhe.read_lhe(local))
        except SystemExit as exc:
            raise RuntimeError(f"pylhe exited while reading {source}: exit code {exc.code!r}") from exc
        while True:
            try:
                event = next(events)
            except StopIteration:
                break
            except SystemExit as exc:
                raise RuntimeError(f"pylhe exited while reading {source}: exit code {exc.code!r}") from exc
            event_buf.append(event)
            if len(event_buf) >= chunk_size:
                consume(event_buf); event_buf=[]
        consume(event_buf)
    finally:
        if cleanup: Path(local).unlink(missing_ok=True)
    stream_hash.update(counts["events"].to_bytes(8, "big"))
    duplicate_count=sum(n-1 for n in dup_hashes.values() if n>1)
    moments={}
    for key, vals in sums.items():
        arr=np.asarray(vals,dtype=float)
        moments[key]={"n":int(len(arr)),"mean":float(np.mean(arr)) if len(arr) else None,"std":float(np.std(arr)) if len(arr) else None,"quantiles":{str(q):float(np.quantile(arr,q)) for q in (.1,.5,.9)} if len(arr) else None}
    return {"path":row["path"],"source_id":row.get("source_id"),"seed":row.get("seed"),"inventory_event_sha256":row.get("event_sha256"),"kinematic_stream_sha256":stream_hash.hexdigest(),"counts":dict(counts),"weight": {"available_events":len(weights),"sum":float(sum(weights)) if weights else None,"sign_counts":dict(sign)},"duplicate": {"repeated_event_fingerprints":duplicate_count,"unique_fingerprints":len(dup_hashes)},"histograms":{k:{"edges":list(BIN_EDGES[k]),"counts":v.tolist(),"under_overflow":overflow[k]} for k,v in hist.items()},"moments":moments,"_arrays":{k:sums[k] for k in sums}}


def matching_kinematic_streams(files_by_cohort, across_cohorts=False):
    groups = {}
    for cohort, files in files_by_cohort.items():
        for file in files:
            events = file["counts"].get("events", 0)
            if events == 0:
                continue
            key = (file["kinematic_stream_sha256"], events)
            groups.setdefault(key, []).append({"cohort": cohort, "source_id": file["source_id"],
                                                "path": file["path"]})
    return [{"kinematic_stream_sha256": digest, "events": events, "sources": sources}
            for (digest, events), sources in groups.items()
            if len(sources) > 1 and (not across_cohorts or len({s["cohort"] for s in sources}) > 1)]


def summarize_cohort(name, files):
    result={"name":name,"files_selected":len(files),"events":sum(x["counts"].get("events",0) for x in files),"jpsi":sum(x["counts"].get("jpsi",0) for x in files),"files":files,
            "matching_kinematic_streams":matching_kinematic_streams({name:files})}
    for k in BIN_EDGES:
        allv=[v for f in files for v in f["_arrays"][k]]
        if allv:
            import numpy as np
            a=np.asarray(allv); result.setdefault("moments",{})[k]={"n":len(a),"mean":float(a.mean()),"std":float(a.std()),"quantiles":{str(q):float(np.quantile(a,q)) for q in (.1,.5,.9)}}
        result.setdefault("histograms",{})[k]={"edges":list(BIN_EDGES[k]),"counts":[sum(f["histograms"][k]["counts"][i] for f in files) for i in range(len(BIN_EDGES[k])-1)],"under_overflow":sum(f["histograms"][k]["under_overflow"] for f in files)}
    result["files"]=[{k:v for k,v in f.items() if k!="_arrays"} for f in files]
    return result


def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config",required=True,help="JSON cohort/comparison configuration")
    ap.add_argument("--output",default="lhe-kinematics-audit.json")
    args=ap.parse_args(argv)
    cfg=json.load(open(args.config)); inv=json.load(open(cfg["inventory"]))
    seed=int(cfg.get("sample_seed",1)); cohort_results={}; raw={}
    for name, spec in cfg["cohorts"].items():
        pool=spec["pool"]
        rows=file_rows(inv,pool,spec)
        sample=spec.get("sample_fraction",spec.get("sample_count"))
        if sample is not None and not isinstance(sample,float) and "sample_fraction" in spec: sample=float(sample)
        rows=choose(rows,sample,seed+len(cohort_results))
        files=[analyze_file(row,spec.get("base",cfg.get("base","")),
                            lfn_endpoint=spec.get("lfn_endpoint",cfg.get("lfn_endpoint"))) for row in rows]
        raw[name]=files; cohort_results[name]=summarize_cohort(name,files)
    known={}
    for pool, pdata in inv["pools"].items():
        for row in pdata["files"]:
            if row.get("event_sha256"): known.setdefault((pool,row["event_sha256"]),[]).append(row["path"])
    comparisons=[]
    for comp in cfg.get("comparisons",[]):
        left,right=comp["left"],comp["right"]; detail={"left":left,"right":right,"observables":{},"label":"exploratory; histogram-based descriptive comparison"}
        for k in BIN_EDGES:
            a=cohort_results[left]["histograms"][k]["counts"]; b=cohort_results[right]["histograms"][k]["counts"]
            na=sum(a); nb=sum(b)
            if na and nb:
                import numpy as np
                d=float(np.max(np.abs(np.cumsum(a)/na-np.cumsum(b)/nb)))
                detail["observables"][k]={"histogram_ks_d":d,"n_left":na,"n_right":nb}
            else: detail["observables"][k]={"available":False,"reason":"one cohort has no in-range observations"}
        comparisons.append(detail)
    for name,spec in cfg["cohorts"].items():
        for f in cohort_results[name]["files"]:
            f["same_pool_inventory_alias_paths"]=known.get((spec["pool"],f.get("inventory_event_sha256")),[])
    output={"schema":"lhe-kinematics-audit-v1","sample_seed":seed,"bin_semantics":"unit weighted; lower-inclusive upper-exclusive; under_overflow counts include values outside configured edges","cohorts":cohort_results,"comparisons":comparisons,
            "cross_cohort_kinematic_stream_matches":matching_kinematic_streams({name: result["files"] for name, result in cohort_results.items()}, across_cohorts=True)}
    Path(args.output).write_text(json.dumps(output,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":args.output,"cohorts":{k:{"files":v["files_selected"],"events":v["events"]} for k,v in cohort_results.items()}},indent=2))
    return 0


if __name__ == "__main__":
    try: raise SystemExit(main())
    except (RuntimeError, OSError, ValueError, KeyError) as exc:
        print(f"error: {exc}",file=sys.stderr); raise SystemExit(2)
