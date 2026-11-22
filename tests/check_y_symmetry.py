#!/usr/bin/env python3
"""
Check y-rapidity symmetry of J/psi and phi across multiple ntuple files.

For each file, computes:
  A = (N_pos - N_neg) / (N_pos + N_neg)
with binomial uncertainty:
  sigma_A = sqrt((1 - A^2) / N_total)

Flags files where |A| exceeds the expected statistical scatter.

Usage:
  python3 tests/check_y_symmetry.py <url_or_path> [<url_or_path> ...]
  python3 tests/check_y_symmetry.py --set1 url1 url2 ... --set2 urlA urlB ...
"""

import ROOT
import sys
import os
import math
import subprocess
import tempfile
import argparse

ROOT.gROOT.SetBatch(True)

def download(url, workdir):
    fname = os.path.basename(url.replace("root://", "").rstrip("/").split("/")[-1])
    # Include parent dir to avoid collisions
    parts = url.replace("root://", "").rstrip("/").split("/")
    if len(parts) >= 2:
        fname = parts[-2] + "_" + parts[-1]
    local = os.path.join(workdir, fname)
    subprocess.run(["xrdcp", "--nopbar", url, local], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return local


def check_branch(tree, branch, cut, nbins=40):
    """
    Returns (n_total, n_pos, n_neg, mean, asym, sigma_asym).
    Uses explicit per-event loop to avoid TTree::Draw quirks with vector branches.
    """
    n_pos = 0
    n_neg = 0
    sum_y = 0.0
    n_total = 0
    n_entries = tree.GetEntries()

    # Figure out if branch is a vector or scalar by checking a sample entry
    tree.GetEntry(0)
    val = getattr(tree, branch)
    is_vector = hasattr(val, '__len__') and not isinstance(val, str) and not isinstance(val, bytes)

    if is_vector:
        for i in range(n_entries):
            tree.GetEntry(i)
            v = getattr(tree, branch)
            for y in v:
                if y == 0.0:
                    continue
                n_total += 1
                sum_y += y
                if y > 0:
                    n_pos += 1
                else:
                    n_neg += 1
    else:
        for i in range(n_entries):
            tree.GetEntry(i)
            y = getattr(tree, branch)
            if y == 0.0:
                continue
            n_total += 1
            sum_y += y
            if y > 0:
                n_pos += 1
            else:
                n_neg += 1

    mean = sum_y / n_total if n_total > 0 else 0.0
    asym = (n_pos - n_neg) / n_total if n_total > 0 else 0.0
    sigma_asym = math.sqrt((1 - asym * asym) / n_total) if n_total > 0 else 0.0
    return n_total, n_pos, n_neg, mean, asym, sigma_asym


def check_file(path, workdir):
    """Analyze one ntuple file. Returns dict of results."""
    print(f"  {os.path.basename(path)}  ", end="", flush=True)
    local = download(path, workdir) if path.startswith("root://") else path

    f = ROOT.TFile(local)
    t = f.Get("mkcands").Get("X_data")
    n_evt = t.GetEntries()

    results = {"file": os.path.basename(path), "n_events": n_evt, "branches": {}}

    for branch, name in [
        ("SingleJpsi_y",  "SingleJ/#psi"),
        ("SinglePhi_y",   "Single#phi"),
        ("Jpsi_1_y",      "Pri J/#psi_{1}"),
        ("Jpsi_2_y",      "Pri J/#psi_{2}"),
        ("Phi_y",         "Pri #phi"),
    ]:
        n_tot, n_pos, n_neg, mean, asym, sig = check_branch(t, branch, "")
        results["branches"][name] = {
            "n": n_tot, "n_pos": n_pos, "n_neg": n_neg,
            "mean": mean, "asym": asym, "sigma": sig,
        }

    f.Close()
    if path.startswith("root://") and os.path.exists(local):
        os.unlink(local)

    # Print one-line summary
    sj = results["branches"]["SingleJ/#psi"]
    sp = results["branches"]["Single#phi"]
    n_sig = abs(sj["asym"]) / sj["sigma"] if sj["sigma"] > 0 else 0
    print(f"n_evt={n_evt:5d}  SJ: N={sj['n']:5d}  asym={sj['asym']:+6.3f}+-{sj['sigma']:.3f}  ({n_sig:.1f}#sigma)  "
          f"SPhi: N={sp['n']:5d}  asym={sp['asym']:+6.3f}+-{sp['sigma']:.3f}")
    return results


def print_summary(all_results):
    """Print a summary table across all files."""
    print(f"\n{'='*110}")
    print("PER-FILE SUMMARY")
    header = f"{'File':45s} {'N_evt':>6s} | {'SJ_N':>6s} {'SJ_asym':>9s} {'SJ_sig':>7s} | {'SPhi_N':>6s} {'SPhi_asym':>9s} {'SPhi_sig':>7s} | {'PriJ1_N':>7s} {'PriJ1_asym':>9s} | {'PriJ2_N':>7s} {'PriJ2_asym':>9s} | {'PriPhi_N':>7s} {'PriPhi_asym':>9s}"
    print(header)
    print("-" * 110)
    for r in all_results:
        b = r["branches"]
        sj, sp = b["SingleJ/#psi"], b["Single#phi"]
        j1, j2, ph = b["Pri J/#psi_{1}"], b["Pri J/#psi_{2}"], b["Pri #phi"]
        def fmt_asym(d):
            if d["n"] == 0 or d["sigma"] == 0:
                return f"{'--':>8s}", "  --s"
            return f"{d['asym']:+8.4f}", f"{abs(d['asym'])/d['sigma']:5.1f}s"
        sj_a, sj_s = fmt_asym(sj)
        sp_a, sp_s = fmt_asym(sp)
        j1_a = f"{j1['asym']:+8.4f}" if j1['n'] > 0 else f"{'--':>8s}"
        j2_a = f"{j2['asym']:+8.4f}" if j2['n'] > 0 else f"{'--':>8s}"
        ph_a = f"{ph['asym']:+8.4f}" if ph['n'] > 0 else f"{'--':>8s}"
        print(f"{r['file']:45s} {r['n_events']:6d} | {sj['n']:6d} {sj_a} {sj_s} | "
              f"{sp['n']:6d} {sp_a} {sp_s} | "
              f"{j1['n']:7d} {j1_a} | {j2['n']:7d} {j2_a} | "
              f"{ph['n']:7d} {ph_a}")

    # Aggregated by branch type
    print(f"\n{'='*110}")
    print("AGGREGATE (summed across all files)")
    for name in ["SingleJ/#psi", "Single#phi", "Pri J/#psi_{1}", "Pri J/#psi_{2}", "Pri #phi"]:
        n_tot = sum(r["branches"][name]["n"] for r in all_results)
        n_pos = sum(r["branches"][name]["n_pos"] for r in all_results)
        n_neg = sum(r["branches"][name]["n_neg"] for r in all_results)
        mean = sum(r["branches"][name]["mean"] * r["branches"][name]["n"] for r in all_results)
        mean = mean / n_tot if n_tot > 0 else 0
        asym = (n_pos - n_neg) / n_tot if n_tot > 0 else 0
        sig = math.sqrt((1 - asym * asym) / n_tot) if n_tot > 0 else 0
        n_sig = abs(asym) / sig if sig > 0 else 0
        flag = " <<< ASYMMETRIC" if n_sig > 5 else ""
        print(f"  {name:20s}  N={n_tot:7d}  asym={asym:+8.4f}+-{sig:.4f}  ({n_sig:.1f}#sigma){flag}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="*", help="Files or URLs to check")
    parser.add_argument("--set1", nargs="+", default=[], help="First set of files")
    parser.add_argument("--set2", nargs="+", default=[], help="Second set of files")
    args = parser.parse_args()

    workdir = tempfile.mkdtemp(prefix="ycheck_", dir="/tmp/chiw")
    all_results = []

    for set_label, urls in [("SET 1", args.set1), ("SET 2", args.set2)]:
        if not urls:
            continue
        print(f"\n{'='*60}")
        print(f"{set_label}")
        print(f"{'='*60}")
        for url in urls:
            results = check_file(url, workdir)
            all_results.append(results)

    # Any positional args treated as a default set
    if args.files:
        print(f"\n{'='*60}")
        print("FILES")
        print(f"{'='*60}")
        for url in args.files:
            results = check_file(url, workdir)
            all_results.append(results)

    if all_results:
        print_summary(all_results)

    # Cleanup workdir
    import shutil
    shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
