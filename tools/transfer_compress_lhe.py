#!/usr/bin/env python3
"""
Generate a DAG to compress and transfer LHE files between XRootD directories.

Each condor job handles one chunk of files: download from source, compress,
upload to target, verify, and optionally clean the source.

Usage:
  python3 tools/transfer_compress_lhe.py \\
    --source-pool pool_jpsi_CSCO_g --target-subdir SPS-Jpsi \\
    --chunk-size 20 --output-dir generated/transfer/SPS-Jpsi \\
    [--dry-run] [--level 1] [--clean-source]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import Dict, List, Optional, Sequence

# Add repo root to path for imports from dag_generator
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dag_generator import (  # noqa: E402
    EOS_REDIRECTOR,
    EOS_LFN_BASE,
    EOS_HOST,
    EOS_XRDFS_TARGET,
    EXISTING_LHE_SUBDIR_BY_POOL,
    build_bundle,
    build_proxy_bundle,
    BUNDLE_NAMES,
    COMPRESS_WRAPPER_PATH,
    COMPRESS_WRAPPER_NAME,
    TRANSFER_COMPRESS_WRAPPER_PATH,
    TRANSFER_COMPRESS_WRAPPER_NAME,
    build_compression_bundle,
    bool_string,
    dag_escape,
    ensure_dir,
    pool_storage_name,
)

SOURCE_LHE_POOLS_BASE = f"{EOS_LFN_BASE}/lhe_pools"
TARGET_LHE_POOL_BASE = f"{EOS_LFN_BASE}/LHE_pool"
SOURCE_URL_PREFIX = f"root://{EOS_REDIRECTOR}//{EOS_LFN_BASE}/lhe_pools"


def _run_xrdfs(*args: str) -> str:
    """Run xrdfs against EOS_XRDFS_TARGET, return stdout."""
    cmd = ["xrdfs", EOS_XRDFS_TARGET] + list(args)
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        print(f"xrdfs error: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def list_source_files(pool_name: str) -> List[str]:
    """List uncompressed .lhe files in a source pool via xrdfs ls."""
    remote_dir = f"{SOURCE_LHE_POOLS_BASE}/{pool_name}"
    print(f"Listing: {remote_dir}")
    output = _run_xrdfs("ls", remote_dir)
    files = []
    for line in output.splitlines():
        line = line.strip()
        if line.endswith(".lhe") and not line.endswith(".lhe.gz"):
            files.append(os.path.basename(line))
    files.sort()
    print(f"  Found {len(files)} .lhe files")
    return files


def chunk_files(files: List[str], chunk_size: int) -> List[List[str]]:
    """Split a sorted list of files into fixed-size chunks."""
    return [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]


def write_input_lists(
    chunks: List[List[str]], output_dir: str, pool_name: str
) -> List[str]:
    """Write each chunk to a text file. Returns list of absolute paths."""
    lists_dir = os.path.join(output_dir, "input_lists")
    ensure_dir(lists_dir)
    paths: List[str] = []
    for i, chunk in enumerate(chunks):
        path = os.path.join(lists_dir, f"chunk_{pool_name}_{i:04d}.txt")
        with open(path, "w", encoding="utf-8") as fh:
            for name in chunk:
                fh.write(f"{name}\n")
        paths.append(path)
    return paths


def generate_dag(
    output_dir: str,
    dag_name: str,
    pool_name: str,
    target_subdir: str,
    input_list_paths: List[str],
    compression_bundle_path: str,
    compression_bundle_name: str,
    proxy_bundle_path: str,
    proxy_bundle_name: str,
    level: int,
    clean_source: bool,
    log_root: str,
) -> str:
    """Write a DAG file with one job per chunk. Returns DAG path."""
    dag_path = os.path.join(output_dir, dag_name)
    source_prefix = f"{SOURCE_URL_PREFIX}/{pool_name}"

    with open(dag_path, "w", encoding="utf-8") as fh:
        fh.write(f"# Auto-generated transfer-compress DAG for {pool_name}\n")
        fh.write(f"# {len(input_list_paths)} jobs, chunk size = {len(input_list_paths)}-way split\n")
        fh.write("\n")

        for i, input_list in enumerate(input_list_paths):
            job_name = f"transfer_{pool_name}_{i:04d}"
            fh.write(f"JOB {job_name} processing/templates/transfer_compress.sub\n")
            vars_fmt = (
                'VARS {job} compression_bundle_path="{compression_bundle_path}" '
                'compression_bundle_name="{compression_bundle_name}" '
                'proxy_bundle_path="{proxy_bundle_path}" '
                'proxy_bundle_name="{proxy_bundle_name}" '
                'transfer_compress_wrapper_path="{wrapper_path}" '
                'source_prefix="{source_prefix}" '
                'target_subdir="{target_subdir}" '
                'input_list_path="{input_list_path}" '
                'input_list_name="{input_list_name}" '
                'level="{level}" '
                'clean_source="{clean_source}" '
                'log_root="{log_root}"'
            ).format(
                job=job_name,
                compression_bundle_path=dag_escape(compression_bundle_path),
                compression_bundle_name=dag_escape(compression_bundle_name),
                proxy_bundle_path=dag_escape(proxy_bundle_path),
                proxy_bundle_name=dag_escape(proxy_bundle_name),
                wrapper_path=dag_escape(TRANSFER_COMPRESS_WRAPPER_PATH),
                source_prefix=dag_escape(source_prefix),
                target_subdir=dag_escape(target_subdir),
                input_list_path=dag_escape(input_list),
                input_list_name=dag_escape(os.path.basename(input_list)),
                level=str(level),
                clean_source=dag_escape(bool_string(clean_source)),
                log_root=dag_escape(log_root),
            )
            fh.write(vars_fmt + "\n")
            fh.write("\n")

    return dag_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source-pool",
        required=True,
        help="Source pool name (e.g. pool_jpsi_CSCO_g).",
    )
    parser.add_argument(
        "--target-subdir",
        required=True,
        help="Target subdirectory under LHE_pool/ (e.g. SPS-Jpsi).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=20,
        help="Files per condor job (default: 20).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory for DAG, bundles, and input lists.",
    )
    parser.add_argument(
        "--proxy",
        default=None,
        help="Path to x509 proxy file.",
    )
    parser.add_argument(
        "--level",
        type=int,
        default=1,
        choices=range(1, 10),
        help="gzip compression level (default: 1).",
    )
    parser.add_argument(
        "--clean-source",
        action="store_true",
        help="Delete source .lhe files after verified upload.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files and show chunking plan without generating DAG.",
    )
    parser.add_argument(
        "--dag-name",
        default=None,
        help="DAG filename (default: transfer_<source_pool>.dag).",
    )

    args = parser.parse_args(argv)

    # Resolve proxy
    proxy_path = args.proxy
    if proxy_path is None:
        proxy_path = os.environ.get("X509_USER_PROXY", "")
    if not proxy_path or not os.path.exists(proxy_path):
        print(
            "ERROR: No valid proxy found. Use --proxy or set X509_USER_PROXY.",
            file=sys.stderr,
        )
        return 1

    # 1. List source files
    files = list_source_files(args.source_pool)
    if not files:
        print("No uncompressed .lhe files found in source pool.", file=sys.stderr)
        return 1

    # 2. Chunk
    chunks = chunk_files(files, args.chunk_size)
    print(f"Split into {len(chunks)} chunks of up to {args.chunk_size} files each")

    if args.dry_run:
        for i, chunk in enumerate(chunks):
            print(f"  Chunk {i:04d}: {len(chunk)} files, from {chunk[0]} to {chunk[-1]}")
        print(f"\nWould generate {len(chunks)} jobs in DAG.")
        print("Dry-run complete. No files written.")
        return 0

    # 3. Prepare output directories
    ensure_dir(args.output_dir)
    log_root = os.path.join(args.output_dir, "log")
    ensure_dir(log_root)

    # 4. Write input list files
    print("Writing input lists...")
    input_list_paths = write_input_lists(chunks, args.output_dir, args.source_pool)
    print(f"  Wrote {len(input_list_paths)} chunk files")

    # 5. Build bundles
    print("Building compression bundle...")
    compression_bundle_path, compression_bundle_name = build_compression_bundle(
        args.output_dir
    )
    print(f"  {compression_bundle_name}")

    print("Building proxy bundle...")
    proxy_bundle_path, proxy_bundle_name = build_proxy_bundle(
        args.output_dir, proxy_path
    )
    print(f"  {proxy_bundle_name}")

    # 6. Generate DAG
    dag_name = args.dag_name or f"transfer_{args.source_pool}.dag"
    print("Generating DAG...")
    dag_path = generate_dag(
        output_dir=args.output_dir,
        dag_name=dag_name,
        pool_name=args.source_pool,
        target_subdir=args.target_subdir,
        input_list_paths=input_list_paths,
        compression_bundle_path=compression_bundle_path,
        compression_bundle_name=compression_bundle_name,
        proxy_bundle_path=proxy_bundle_path,
        proxy_bundle_name=proxy_bundle_name,
        level=args.level,
        clean_source=args.clean_source,
        log_root=log_root,
    )
    print(f"  DAG written to: {dag_path}")

    # 7. Summary
    print(f"\nDone. Generated DAG with {len(chunks)} jobs.")
    print(f"  Source pool:  {args.source_pool} ({len(files)} files)")
    print(f"  Target dir:   {TARGET_LHE_POOL_BASE}/{args.target_subdir}")
    print(f"  Chunk size:   {args.chunk_size}")
    print(f"  Clean source: {args.clean_source}")
    print(f"\nSubmit with:")
    print(f"  condor_submit_dag {dag_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
