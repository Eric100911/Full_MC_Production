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
from urllib.parse import urlsplit


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Per-pool LHE block planner")
    p.add_argument("--pool-name", required=True, help="LHE pool name")
    p.add_argument("--helac-seed", type=int, default=None, help="Legacy HELAC generation seed")
    p.add_argument("--primary-seed", type=int, default=None, help="Primary seed for grouped LHE input")
    p.add_argument("--group-id", default="", help="Group namespace for output block files")
    p.add_argument("--helac-seeds", default="", help="Comma-separated seeds corresponding to --lhe-path inputs")
    p.add_argument("--lhe-path", action="append", required=True,
                   help="Path to LHE file (local or root:// URL); may be repeated")
    p.add_argument("--lhe-event-counts", default="",
                   help="Comma-separated expected input event counts from discovery inventory")
    p.add_argument("--output-dir", required=True, help="Directory for manifest output")
    p.add_argument("--events-per-block", type=int, default=1000)
    p.add_argument("--max-events-per-plan", type=int, default=0,
                   help="Maximum events emitted by this planner after shuffle ordering; 0 means uncapped")
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
        endpoint, remote_path = _parse_xrootd_url(url)
        subprocess.run(
            ["xrdfs", endpoint, "stat", remote_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=120,
        )
        return True
    except ValueError as exc:
        print(f"[ERROR] Cannot verify remote file: {exc}", file=sys.stderr)
        return False
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        print(
            f"[ERROR] xrdfs stat failed for {url}"
            f" (exit code {exc.returncode}): {detail or 'no diagnostic output'}",
            file=sys.stderr,
        )
        return False
    except subprocess.TimeoutExpired as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        print(
            f"[ERROR] xrdfs stat timed out after 120 seconds for {url}: "
            f"{detail or 'no diagnostic output'}",
            file=sys.stderr,
        )
        return False


def _parse_xrootd_url(url: str) -> tuple:
    """Return the exact xrdfs endpoint and normalized remote path."""
    parsed = urlsplit(url)
    if parsed.scheme != "root" or not parsed.netloc:
        raise ValueError(f"Invalid XRootD URL: {url}")
    endpoint = f"root://{parsed.netloc}/"
    remote_path = "/" + parsed.path.lstrip("/")
    return endpoint, remote_path


def _extract_eos_path(url: str) -> str:
    """Extract /eos/... path from root://host//eos/... URL."""
    if url.startswith("root://"):
        _, remote_path = _parse_xrootd_url(url)
        return remote_path
    return url


def _ensure_remote_dir(url: str) -> None:
    """Best-effort mkdir for XRootD destinations."""
    if not url.startswith("root://"):
        return
    endpoint, remote_path = _parse_xrootd_url(url)
    subprocess.run(["xrdfs", endpoint, "mkdir", "-p", remote_path],
                   check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _normalize_inputs(args: argparse.Namespace) -> tuple:
    primary_seed = args.primary_seed or args.helac_seed
    if primary_seed is None:
        raise ValueError("--primary-seed or --helac-seed is required")

    group_id = args.group_id or str(primary_seed)
    seeds = [int(s) for s in args.helac_seeds.split(",") if s]
    if not seeds:
        seeds = [primary_seed]
    if len(seeds) != len(args.lhe_path):
        raise ValueError(
            f"--helac-seeds has {len(seeds)} entries but --lhe-path has {len(args.lhe_path)}"
        )
    expected_counts = []
    if args.lhe_event_counts:
        expected_counts = [int(s) for s in args.lhe_event_counts.split(",") if s]
        if len(expected_counts) != len(args.lhe_path):
            raise ValueError(
                f"--lhe-event-counts has {len(expected_counts)} entries but "
                f"--lhe-path has {len(args.lhe_path)}"
            )
    return primary_seed, group_id, seeds, expected_counts


def main() -> int:
    args = parse_args()
    try:
        primary_seed, group_id, seeds, expected_input_event_counts = _normalize_inputs(args)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    is_remote_output = args.block_output_dir.startswith("root://")
    grouped_output = group_id != str(primary_seed)
    workdir = tempfile.mkdtemp(prefix="plan_lhe_blocks_")

    try:
        # --- 1. Resolve and decompress LHE files ---
        local_inputs = []
        for i, lhe_path in enumerate(args.lhe_path):
            local_lhe = lhe_path
            if lhe_path.startswith("root://"):
                basename = os.path.basename(_extract_eos_path(lhe_path))
                local_lhe = os.path.join(workdir, f"input_{i}_{basename}")
                print(f"[INFO] Downloading {lhe_path} -> {local_lhe}")
                subprocess.run(["xrdcp", "--nopbar", "--force", lhe_path, local_lhe], check=True)

            if not os.path.exists(local_lhe):
                print(f"[ERROR] LHE file not found: {local_lhe}", file=sys.stderr)
                return 1

            if local_lhe.endswith(".gz"):
                decompressed = os.path.join(workdir, f"input_{i}.lhe")
                print(f"[INFO] Decompressing {local_lhe} -> {decompressed}")
                with gzip.open(local_lhe, "rb") as f_in:
                    with open(decompressed, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                local_lhe = decompressed

            local_inputs.append(local_lhe)

        print(f"[INFO] Input LHE group {group_id} has {len(local_inputs)} file(s)")

        # --- 2. Check for existing blocks (reuse) ---
        manifest_path = args.manifest_output_path or os.path.join(
            args.output_dir, f"plan_manifest_{args.pool_name}_{group_id}.json"
        )
        prefix = f"block_{group_id}_"
        block_stage_dir = args.block_output_dir.rstrip("/")
        if grouped_output:
            block_stage_dir = block_stage_dir + "/" + group_id

        if args.reuse_existing_blocks:
            existing = _list_existing_blocks(block_stage_dir, prefix, is_remote_output)
            if existing and _blocks_match_manifest(existing, manifest_path):
                print(f"[INFO] Reusing {len(existing)} existing blocks (--reuse-existing-blocks)")
                return 0
            print("[INFO] Existing blocks incomplete or mismatched, regenerating...")

        # --- 3. Run lhe_shuffle_split ---
        split_out = os.path.join(workdir, "split_out")
        os.makedirs(split_out, exist_ok=True)
        shuffle_cmd = [
            args.lhe_shuffle_split_bin,
            "--output-dir", split_out,
            "--seed", str(args.shuffle_seed),
            "--events-per-block", str(args.events_per_block),
            "--mode", args.shuffle_mode,
            "--n-strata", args.n_strata,
            "--filename-prefix", f"{group_id}_",
            "--write-provenance",
        ]
        if args.max_events_per_plan > 0:
            shuffle_cmd.extend(["--max-output-events", str(args.max_events_per_plan)])
        for local_lhe in local_inputs:
            shuffle_cmd.extend(["--input", local_lhe])
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

        splitter_events = shuffle_manifest.get("total_input_events")
        if splitter_events is None:
            print("[ERROR] lhe_shuffle_split manifest is missing total_input_events", file=sys.stderr)
            return 1
        n_events = int(splitter_events)
        if n_events <= 0:
            print("[ERROR] lhe_shuffle_split counted zero input events", file=sys.stderr)
            return 1

        n_blocks = shuffle_manifest.get("n_blocks", 0)
        if n_blocks == 0:
            print("[ERROR] lhe_shuffle_split produced zero blocks", file=sys.stderr)
            return 1

        # --- 4. Compress each block and stage out ---
        os.makedirs(args.output_dir, exist_ok=True)
        if is_remote_output:
            _ensure_remote_dir(block_stage_dir)
        else:
            os.makedirs(block_stage_dir, exist_ok=True)

        plan_blocks = []
        for bi in range(n_blocks):
            src_name = f"{group_id}_block_{bi:06d}.lhe"
            dst_name = f"block_{group_id}_{bi:06d}.lhe.gz"
            src_path = os.path.join(split_out, src_name)
            gz_tmp = os.path.join(workdir, dst_name)

            # Compress
            print(f"[INFO] Compressing {src_name} -> {dst_name}")
            with open(src_path, "rb") as f_in:
                with gzip.open(gz_tmp, "wb", compresslevel=1) as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Stage
            if is_remote_output:
                dst_url = block_stage_dir.rstrip("/") + "/" + dst_name
                stage_file(gz_tmp, dst_url, is_remote=True)
                # Verify
                if not check_remote_file(dst_url):
                    print(f"[ERROR] Verification failed for {dst_url}", file=sys.stderr)
                    return 1
                plan_blocks.append({"index": bi, "filename": dst_name,
                                    "n_events": shuffle_manifest["blocks"][bi]["n_events"],
                                    "path": dst_url})
            else:
                dst_path = os.path.join(block_stage_dir, dst_name)
                stage_file(gz_tmp, dst_path, is_remote=False)
                plan_blocks.append({"index": bi, "filename": dst_name,
                                    "n_events": shuffle_manifest["blocks"][bi]["n_events"],
                                    "path": dst_path})

        # --- 5. Write plan manifest ---
        manifest = {
            "tool": "plan_lhe_blocks",
            "version": "2.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pool": args.pool_name,
            "group_id": group_id,
            "primary_seed": primary_seed,
            "helac_seed": primary_seed,
            "seeds": seeds,
            "input_files": args.lhe_path,
            "input_event_counts": expected_input_event_counts,
            "input_event_count_source": (
                "discovery_inventory" if expected_input_event_counts else "not_provided"
            ),
            "shuffle_seed": args.shuffle_seed,
            "shuffle_mode": args.shuffle_mode,
            "n_strata_arg": args.n_strata,
            "n_blocks": n_blocks,
            "events_per_block": args.events_per_block,
            "max_events_per_plan": args.max_events_per_plan,
            "total_input_events": n_events,
            "planned_events": shuffle_manifest.get("event_conservation", {}).get("output_total"),
            "event_selection": shuffle_manifest.get("event_selection", {}),
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
            src_name = f"{group_id}_block_{bi:06d}.lhe"
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
            endpoint, eos_path = _parse_xrootd_url(block_output_dir)
            result = subprocess.run(
                ["xrdfs", endpoint, "ls", eos_path],
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
