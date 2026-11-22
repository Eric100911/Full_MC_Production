#!/usr/bin/env python3
"""
Compress existing uncompressed LHE files in a pool directory.

Writes a JSON manifest with compression statistics.  Supports dry-run,
--keep (preserve originals), --force (recompress even if .lhe.gz exists),
and configurable gzip level.

Usage:
  python3 tools/compress_existing_lhe.py --pool-dir /path/to/pool
  python3 tools/compress_existing_lhe.py --pool-dir /path/to/pool --level 3 --keep
  python3 tools/compress_existing_lhe.py --pool-dir /path/to/pool --dry-run
  python3 tools/compress_existing_lhe.py --input-list files.txt --output-manifest manifest.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.compression_util import (  # noqa: E402
    accepts_lhe_ext,
    gzip_file_atomic,
    strip_gz_suffix,
)


def collect_lhe_files(pool_dir: str) -> List[str]:
    """Return sorted list of uncompressed .lhe paths under *pool_dir*."""
    result: List[str] = []
    for root, _dirs, files in os.walk(pool_dir):
        for name in files:
            if name.endswith(".lhe") and not name.endswith(".lhe.gz"):
                result.append(os.path.join(root, name))
    result.sort()
    return result


def compress_one(
    src: str,
    level: int,
    keep: bool,
    force: bool,
    dry_run: bool,
) -> Dict:
    """Compress a single .lhe file.  Returns a manifest record."""
    dst = src + ".gz"
    record = {
        "original_path": src,
        "compressed_path": dst,
        "original_size": os.path.getsize(src),
        "compressed_size": None,
        "level": level,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "skipped",
    }

    if os.path.exists(dst) and not force:
        record["compressed_size"] = os.path.getsize(dst)
        record["status"] = "skipped_exists"
        return record

    if dry_run:
        record["status"] = "dry_run"
        return record

    gzip_file_atomic(src, dst, level=level, force=force)
    if not os.path.exists(dst):
        record["status"] = "failed"
        return record

    record["compressed_size"] = os.path.getsize(dst)
    record["status"] = "compressed"

    if not keep:
        os.remove(src)
        record["status"] = "compressed_removed_original"

    return record


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--pool-dir",
        help="Local directory containing uncompressed .lhe files.",
    )
    group.add_argument(
        "--input-list",
        help="Text file with one .lhe path per line.",
    )
    parser.add_argument(
        "--level",
        type=int,
        default=1,
        choices=range(1, 10),
        help="gzip compression level (default: 1).",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep original uncompressed files after compression.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompress even if .lhe.gz already exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be compressed without modifying anything.",
    )
    parser.add_argument(
        "--output-manifest",
        default=None,
        help="Path to write JSON compression manifest.",
    )

    args = parser.parse_args(argv)

    if args.pool_dir:
        files = collect_lhe_files(args.pool_dir)
    else:
        with open(args.input_list, "r", encoding="utf-8") as fh:
            files = [line.strip() for line in fh if line.strip()]

    if not files:
        print("No uncompressed .lhe files found.", file=sys.stderr)
        return 0

    results: List[Dict] = []
    for src in files:
        record = compress_one(src, args.level, args.keep, args.force, args.dry_run)
        results.append(record)
        if args.dry_run:
            print(f"[DRY-RUN] {src} -> {src}.gz")
        elif record["status"] in ("skipped_exists",):
            print(f"[SKIP] {src}  (.gz already exists)")
        elif record["status"] == "failed":
            print(f"[FAIL] {src}", file=sys.stderr)
        else:
            ratio = (
                record["compressed_size"] / record["original_size"] * 100
                if record["original_size"] > 0
                else 0
            )
            print(
                f"[OK] {src} -> {src}.gz "
                f"({record['original_size']} -> {record['compressed_size']} bytes, "
                f"{ratio:.1f}%)"
            )

    succeeded = sum(1 for r in results if r["status"].startswith("compressed"))
    skipped = sum(1 for r in results if r["status"] in ("skipped_exists", "dry_run"))
    failed = sum(1 for r in results if r["status"] == "failed")

    print(
        f"\nSummary: {succeeded} compressed, {skipped} skipped, {failed} failed "
        f"(out of {len(results)} files)"
    )

    if args.output_manifest:
        manifest_path = args.output_manifest
    else:
        manifest_path = "compression_manifest.json"

    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print(f"Manifest written to: {manifest_path}")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
