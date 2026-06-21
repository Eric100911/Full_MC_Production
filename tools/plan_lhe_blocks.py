#!/usr/bin/env python3
"""
plan_lhe_blocks.py — Per-pool LHE block planner.

Runs as a Condor job after HELAC LHE generation. Locates the LHE output,
decompresses if needed, runs lhe_shuffle_split to produce fixed-size blocks,
compresses each block to .lhe.gz, stages them to the target directory,
and writes a plan manifest.

Usage:
  python3 plan_lhe_blocks.py \\
      --pool-name pool_2jpsi_cs --helac-seed 100 \\
      --lhe-path root://eos.example//path/to/sample.lhe.gz \\
      --output-dir /path/to/manifest/output \\
      --events-per-block 1000 --shuffle-seed 100037 \\
      --block-output-dir /path/to/blocks
"""

import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Per-pool LHE block planner")
    p.add_argument("--pool-name", required=True, help="LHE pool name")
    p.add_argument("--helac-seed", type=int, required=True, help="HELAC generation seed")
    p.add_argument("--lhe-path", required=True, help="Path to LHE file (local or root:// URL)")
    p.add_argument("--output-dir", required=True, help="Directory for manifest output")
    p.add_argument("--events-per-block", type=int, default=1000)
    p.add_argument("--shuffle-seed", type=int, required=True)
    p.add_argument("--shuffle-mode", default="stratified")
    p.add_argument("--n-strata", default="auto")
    p.add_argument("--drop-incomplete-last-block", action="store_true")
    p.add_argument("--block-output-dir", required=True, help="Where to store block .lhe.gz files")
    p.add_argument("--local-output-base", default="", help="Local storage base path")
    p.add_argument("--reuse-existing-blocks", action="store_true")
    p.add_argument("--manifest-output-path", default="", help="Full path for the output manifest JSON")
    p.add_argument("--lhe-shuffle-split-bin", default="lhe_shuffle_split",
                   help="Path to lhe_shuffle_split binary")
    return p.parse_args()


def count_lhe_events(path: str) -> int:
    """Count <event> lines in an LHE file, handling .gz transparently."""
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
            return sum(1 for line in fh if line.strip().startswith("<event>"))
    else:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return sum(1 for line in fh if line.strip().startswith("<event>"))


def stage_file(src: str, dst: str, is_remote: bool):
    """Copy or xrdcp a file to its destination."""
    if is_remote:
        subprocess.run(["xrdcp", "--nopbar", "--force", src, dst], check=True)
    else:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)


def check_remote_file(url: str) -> bool:
    """Check if a remote file exists via xrdfs stat."""
    try:
        subprocess.run(["xrdfs", "cceos.ihep.ac.cn", "stat", _extract_eos_path(url)],
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       timeout=30)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


def _extract_eos_path(url: str) -> str:
    """Extract /eos/... path from root://host//eos/... URL."""
    if url.startswith("root://"):
        parts = url.split("//", 2)
        if len(parts) >= 3:
            return "/" + parts[2]
    return url


def main() -> int:
    args = parse_args()

    is_remote = args.lhe_path.startswith("root://")
    workdir = tempfile.mkdtemp(prefix="plan_lhe_blocks_")

    try:
        # --- 1. Resolve LHE file ---
        local_lhe: str = ""
        if is_remote:
            basename = os.path.basename(_extract_eos_path(args.lhe_path))
            local_lhe = os.path.join(workdir, basename)
            print(f"[INFO] Downloading {args.lhe_path} -> {local_lhe}")
            subprocess.run(["xrdcp", "--nopbar", "--force", args.lhe_path, local_lhe], check=True)
        elif args.local_output_base:
            local_lhe = args.lhe_path
        else:
            local_lhe = args.lhe_path

        if not os.path.exists(local_lhe):
            print(f"[ERROR] LHE file not found: {local_lhe}", file=sys.stderr)
            return 1

        # --- 2. Decompress if needed ---
        if local_lhe.endswith(".gz"):
            decompressed = os.path.join(workdir, "input_decompressed.lhe")
            print(f"[INFO] Decompressing {local_lhe} -> {decompressed}")
            with gzip.open(local_lhe, "rb") as f_in:
                with open(decompressed, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            local_lhe = decompressed

        # --- 3. Count events ---
        n_events = count_lhe_events(local_lhe)
        if n_events == 0:
            print("[ERROR] LHE file contains zero events", file=sys.stderr)
            return 1
        print(f"[INFO] Input LHE has {n_events} events")

        # --- 4. Check for existing blocks (reuse) ---
        manifest_path = args.manifest_output_path or os.path.join(
            args.output_dir, f"plan_manifest_{args.pool_name}_{args.helac_seed}.json"
        )
        prefix = f"block_{args.helac_seed}_"

        if args.reuse_existing_blocks:
            existing = _list_existing_blocks(args.block_output_dir, prefix, is_remote)
            if existing and _blocks_match_manifest(existing, manifest_path):
                print(f"[INFO] Reusing {len(existing)} existing blocks (--reuse-existing-blocks)")
                return 0
            print("[INFO] Existing blocks incomplete or mismatched, regenerating...")

        # --- 5. Run lhe_shuffle_split ---
        split_out = os.path.join(workdir, "split_out")
        os.makedirs(split_out, exist_ok=True)
        shuffle_cmd = [
            args.lhe_shuffle_split_bin,
            "--input", local_lhe,
            "--output-dir", split_out,
            "--seed", str(args.shuffle_seed),
            "--events-per-block", str(args.events_per_block),
            "--mode", args.shuffle_mode,
            "--n-strata", args.n_strata,
            "--filename-prefix", f"{args.helac_seed}_",
            "--write-provenance",
        ]
        if args.drop_incomplete_last_block:
            shuffle_cmd.append("--drop-incomplete-last-block")

        print(f"[INFO] Running: {' '.join(shuffle_cmd)}")
        result = subprocess.run(shuffle_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)
        if result.returncode != 0:
            print(f"[ERROR] lhe_shuffle_split failed (exit {result.returncode})", file=sys.stderr)
            print(result.stderr, file=sys.stderr)
            return 1
        print(result.stdout)

        # Read the shuffle manifest for block metadata
        shuffle_manifest_path = os.path.join(split_out, "shuffle_split_manifest.json")
        with open(shuffle_manifest_path, "r") as f:
            shuffle_manifest = json.load(f)

        n_blocks = shuffle_manifest.get("n_blocks", 0)
        if n_blocks == 0:
            print("[ERROR] lhe_shuffle_split produced zero blocks", file=sys.stderr)
            return 1

        # --- 6. Compress each block and stage out ---
        os.makedirs(args.output_dir, exist_ok=True)
        if not is_remote:
            os.makedirs(args.block_output_dir, exist_ok=True)

        plan_blocks = []
        for bi in range(n_blocks):
            src_name = f"{args.helac_seed}_block_{bi:06d}.lhe"
            dst_name = f"block_{args.helac_seed}_{bi:06d}.lhe.gz"
            src_path = os.path.join(split_out, src_name)
            gz_tmp = os.path.join(workdir, dst_name)

            # Compress
            print(f"[INFO] Compressing {src_name} -> {dst_name}")
            with open(src_path, "rb") as f_in:
                with gzip.open(gz_tmp, "wb", compresslevel=1) as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Stage
            if is_remote:
                dst_url = args.block_output_dir.rstrip("/") + "/" + dst_name
                stage_file(gz_tmp, dst_url, is_remote=True)
                # Verify
                if not check_remote_file(dst_url):
                    print(f"[ERROR] Verification failed for {dst_url}", file=sys.stderr)
                    return 1
                plan_blocks.append({"index": bi, "filename": dst_name,
                                    "n_events": shuffle_manifest["blocks"][bi]["n_events"],
                                    "path": dst_url})
            else:
                dst_path = os.path.join(args.block_output_dir, dst_name)
                stage_file(gz_tmp, dst_path, is_remote=False)
                plan_blocks.append({"index": bi, "filename": dst_name,
                                    "n_events": shuffle_manifest["blocks"][bi]["n_events"],
                                    "path": dst_path})

        # --- 7. Write plan manifest ---
        manifest = {
            "tool": "plan_lhe_blocks",
            "version": "1.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pool": args.pool_name,
            "helac_seed": args.helac_seed,
            "shuffle_seed": args.shuffle_seed,
            "shuffle_mode": args.shuffle_mode,
            "n_strata_arg": args.n_strata,
            "n_blocks": n_blocks,
            "events_per_block": args.events_per_block,
            "total_input_events": n_events,
            "dropped_incomplete": shuffle_manifest.get("event_conservation", {}).get(
                "dropped_from_incomplete_block", 0),
            "blocks": plan_blocks,
            "unused_blocks": [],
        }

        manifest_tmp = manifest_path + ".tmp"
        with open(manifest_tmp, "w") as f:
            json.dump(manifest, f, indent=2)
        os.rename(manifest_tmp, manifest_path)
        print(f"[INFO] Manifest written: {manifest_path}")

        # Cleanup uncompressed block files
        for bi in range(n_blocks):
            src_name = f"{args.helac_seed}_block_{bi:06d}.lhe"
            src_path = os.path.join(split_out, src_name)
            if os.path.exists(src_path):
                os.remove(src_path)

        print(f"[OK] Plan complete: {n_blocks} blocks, {n_events} total input events")
        return 0

    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def _list_existing_blocks(block_output_dir: str, prefix: str, is_remote: bool) -> list:
    """List existing block files matching the prefix."""
    if is_remote:
        try:
            eos_path = _extract_eos_path(block_output_dir)
            result = subprocess.run(
                ["xrdfs", "cceos.ihep.ac.cn", "ls", eos_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, timeout=30)
            files = [os.path.basename(line.strip()) for line in result.stdout.splitlines()
                     if line.strip().endswith(".lhe.gz") and prefix in line]
            return sorted(files)
        except Exception:
            return []
    else:
        if not os.path.isdir(block_output_dir):
            return []
        return sorted(f for f in os.listdir(block_output_dir)
                      if f.startswith(prefix) and f.endswith(".lhe.gz"))


def _blocks_match_manifest(existing: list, manifest_path: str) -> bool:
    """Check if existing blocks match the recorded manifest."""
    if not os.path.exists(manifest_path):
        return False
    try:
        with open(manifest_path, "r") as f:
            m = json.load(f)
        recorded = [b["filename"] for b in m.get("blocks", [])]
        return recorded == existing
    except Exception:
        return False


if __name__ == "__main__":
    sys.exit(main())
