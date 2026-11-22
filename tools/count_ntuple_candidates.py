#!/usr/bin/env python3
"""
count_ntuple_candidates.py — Count events with >=1 reconstructed JpsiJpsiPhi
candidate combination across TPS-Onia2MuMu ntuple ROOT files.

The candidate definition is pinned by the analyzer source
(external/TPS-Onia2MuMu/src/MultiLepPAT.cc):

  evaluatePriCandidateDiagnostics():
      fitPass    = priFitValid && (PriVtxProbCut_ < 0.0 || priVtxProb >= PriVtxProbCut_)
      assocPVPass = all 6 daughters share a common associated PV (PriRequireCommonAssocPV)
      passAny    = fitPass || assocPVPass

  A primary candidate is stored into the Pri_* vectors ONLY when passAny is true
  (the builder does `if (!diagnostics.passAny) continue;`).  Therefore "events
  with at least one reconstructed candidate combination" == entries of X_data
  whose `Pri_passAny` vector is non-empty.

Usage:
  python3 tools/count_ntuple_candidates.py \
      --ntuple-tasks <ntuple_tasks.json> \
      [--proxy <x509_user_proxy>] [--jobs 16] [--out summary.json] [--max-files N]

  python3 tools/count_ntuple_candidates.py \
      --url-list urls.txt [--job-prefix MERGE] ...

Input URL sources (mutually exclusive):
  --ntuple-tasks  JSON array of {campaign, job_id, ntuple_url, ...} — the
                  authoritative split-workflow task list (workspace
                  runs/<campaign>/tasks/<campaign>/ntuple_tasks.json).
  --url-list      plain text file, one ntuple URL per line.

Output: a JSON summary with per-file records and totals; also printed to stdout.
"""

import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor


def _count_urls(urls):
    """Process a list of URLs in the current process; returns per-file stats."""
    import ROOT
    ROOT.gROOT.SetBatch(True)
    stats = []
    for url in urls:
        rec = _count_one(ROOT, url)
        if rec:
            stats.append(rec)
    return stats


def _count_one(ROOT, url):
    """Return {url, events, cand_events, n_cand} for one ntuple file, or None."""
    f = None
    try:
        f = ROOT.TFile.Open(url)
        if not f or f.IsZombie():
            return None
        d = f.Get("mkcands")
        if not d:
            return None
        t = d.Get("X_data")
        if not t:
            return None
        events = t.GetEntries()
        t.SetBranchStatus("*", 0)
        t.SetBranchStatus("Pri_passAny", 1)
        cand_events = 0
        n_cand = 0
        for i in range(events):
            t.GetEntry(i)
            v = t.Pri_passAny
            s = v.size()
            if s > 0:
                cand_events += 1
                for j in range(s):
                    if v[j] != 0:
                        n_cand += 1
        return {"url": url, "events": events, "cand_events": cand_events, "n_cand": n_cand}
    except Exception as exc:  # noqa: BLE001 — one bad file must not sink the batch
        return {"url": url, "error": str(exc)}
    finally:
        if f is not None:
            try:
                f.Close()
            except Exception:  # noqa: BLE001
                pass


def load_urls(args):
    if args.ntuple_tasks:
        with open(args.ntuple_tasks) as fh:
            tasks = json.load(fh)
        return [(t.get("job_id") or t.get("task_id"), t["ntuple_url"]) for t in tasks]
    urls = []
    with open(args.url_list) as fh:
        for line in fh:
            line = line.strip()
            if line:
                urls.append(("", line))
    return urls


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--ntuple-tasks", default=None,
                     help="Split-workflow ntuple_tasks.json (authoritative URL list)")
    src.add_argument("--url-list", default=None,
                     help="Plain text file, one ntuple ROOT URL per line")
    p.add_argument("--proxy", default=None,
                   help="X509_USER_PROXY credential to authenticate EOS reads")
    p.add_argument("--jobs", type=int, default=16, help="Parallel worker processes")
    p.add_argument("--max-files", type=int, default=0,
                   help="Limit to first N files (testing)")
    p.add_argument("--out", default=None, help="Write summary JSON to this path")
    p.add_argument("--job-prefix", default="",
                   help="Optional label prefix for per-file job_id (e.g. MERGE)")
    return p.parse_args()


def main():
    args = parse_args()
    if args.proxy:
        os.environ["X509_USER_PROXY"] = args.proxy
    labeled = load_urls(args)
    if args.max_files:
        labeled = labeled[: args.max_files]
    if not labeled:
        print("No ntuple URLs to process.", file=sys.stderr)
        return 1

    # Chunk URLs per worker so each process imports ROOT once.
    n_workers = min(args.jobs, len(labeled))
    chunks = [[] for _ in range(n_workers)]
    for idx, (_, url) in enumerate(labeled):
        chunks[idx % n_workers].append(url)

    per_file = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        for chunk_stats in ex.map(_count_urls, chunks):
            per_file.extend(chunk_stats)

    ok = [r for r in per_file if "error" not in r]
    bad = [r for r in per_file if "error" in r]
    total_events = sum(r["events"] for r in ok)
    total_cand = sum(r["cand_events"] for r in ok)
    total_n_cand = sum(r["n_cand"] for r in ok)

    summary = {
        "candidate_definition": "Pri_passAny (fitPass || assocPVPass); stored only when passAny — "
                                "see external/TPS-Onia2MuMu/src/MultiLepPAT.cc",
        "files_total": len(per_file),
        "files_ok": len(ok),
        "files_failed": len(bad),
        "events_total": total_events,
        "candidate_events_total": total_cand,
        "n_candidates_total": total_n_cand,
        "candidate_events_fraction": (total_cand / total_events) if total_events else None,
        "per_file": per_file,
    }
    print(json.dumps({k: v for k, v in summary.items() if k != "per_file"}, indent=2))
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(summary, fh, indent=2)
        print(f"wrote {args.out}")
    if bad:
        print(f"WARNING: {len(bad)} files failed to read:", file=sys.stderr)
        for r in bad[:10]:
            print(f"  {r['url']}: {r['error']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
