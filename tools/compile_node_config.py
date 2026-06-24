#!/usr/bin/env python3
"""Compile and validate exact node storage configuration."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from copy import deepcopy
from typing import Any, Dict, Tuple

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import dag_generator  # noqa: E402


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def split_remote_path(path: str, default_redirector: str) -> Tuple[str, str]:
    if path.startswith("root://"):
        rest = path[len("root://"):]
        host, _, remote_path = rest.partition("/")
        if not host or not remote_path:
            raise ValueError(f"Invalid XRootD path: {path}")
        return host, "/" + remote_path.lstrip("/")
    if path.startswith("/store/"):
        return default_redirector, path
    raise ValueError(f"Path must be root://... or /store/...: {path}")


def normalize_remote_path(path: str, default_redirector: str) -> str:
    host, remote_path = split_remote_path(path, default_redirector)
    return f"root://{host}/{remote_path}"


def validate_pool_path(path: str) -> None:
    if path.startswith("root://"):
        host, remote_path = split_remote_path(path, "")
        result = subprocess.run(
            ["xrdfs", host, "ls", remote_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"xrdfs ls failed for {path}: {result.stderr.strip() or result.stdout.strip()}"
            )
        entries = [line for line in result.stdout.splitlines() if line.strip()]
    else:
        if not os.path.isdir(path):
            raise RuntimeError(f"Local pool path does not exist: {path}")
        entries = [os.path.join(path, name) for name in os.listdir(path)]

    lhe_entries = [
        entry for entry in entries
        if entry.endswith(".lhe") or entry.endswith(".lhe.gz")
    ]
    if not lhe_entries:
        raise RuntimeError(f"Configured pool path contains no .lhe/.lhe.gz files: {path}")


def compile_from_subprocess_map(
    root_path: str,
    subprocess_map_path: str,
    default_redirector: str,
) -> Dict[str, Dict[str, str]]:
    root_url = normalize_remote_path(root_path.rstrip("/"), default_redirector)
    subprocess_map = load_json(subprocess_map_path)
    if not isinstance(subprocess_map, dict):
        raise ValueError("--subprocess-map must contain a JSON object")

    pool_dirs: Dict[str, Dict[str, str]] = {}
    for subprocess_name, info in subprocess_map.items():
        if not isinstance(info, dict):
            raise ValueError(f"Subprocess mapping {subprocess_name} must be an object")
        pool = str(info.get("pool") or "")
        if not pool:
            raise ValueError(f"Subprocess mapping {subprocess_name} is missing pool")
        if pool not in dag_generator.LHE_POOLS:
            raise ValueError(f"Unknown LHE pool in subprocess map: {pool}")
        storage_name = str(info.get("storage_name") or dag_generator.LHE_POOLS[pool].storage_name)
        pool_dirs[pool] = {
            "storage_name": storage_name,
            "path": f"{root_url}/{subprocess_name}",
        }
    return pool_dirs


def compile_from_pool_paths(pool_paths_path: str, default_redirector: str) -> Dict[str, Dict[str, str]]:
    raw = load_json(pool_paths_path)
    if not isinstance(raw, dict):
        raise ValueError("--pool-paths must contain a JSON object")
    if isinstance(raw.get("lhe_pool_directories"), dict):
        raw = raw["lhe_pool_directories"]

    pool_dirs: Dict[str, Dict[str, str]] = {}
    for pool, info in raw.items():
        if pool not in dag_generator.LHE_POOLS:
            raise ValueError(f"Unknown LHE pool in pool paths: {pool}")
        if isinstance(info, str):
            storage_name = dag_generator.LHE_POOLS[pool].storage_name
            path = info
        elif isinstance(info, dict):
            storage_name = str(info.get("storage_name") or dag_generator.LHE_POOLS[pool].storage_name)
            path = str(info.get("path") or "")
        else:
            raise ValueError(f"Pool path mapping {pool} must be a string or object")
        if not path:
            raise ValueError(f"Pool path mapping {pool} is missing path")
        if path.startswith("root://") or path.startswith("/store/"):
            path = normalize_remote_path(path, default_redirector)
        else:
            path = os.path.abspath(path)
        pool_dirs[pool] = {"storage_name": storage_name, "path": path.rstrip("/")}
    return pool_dirs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--subprocess-map", help="JSON mapping subprocess name to pool/storage_name")
    source.add_argument("--pool-paths", help="JSON mapping pool to exact path or object")
    parser.add_argument("--root-path", help="Root path used with --subprocess-map")
    parser.add_argument("--redirector", default="cceos.ihep.ac.cn:1094")
    parser.add_argument(
        "--pool",
        action="append",
        default=[],
        help="Validate and emit only this pool; repeat for multiple pools",
    )
    parser.add_argument("--output", default="-", help="Output JSON path, or - for stdout")
    parser.add_argument("--no-validate", action="store_true", help="Skip path existence/content validation")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    defaults = deepcopy(dag_generator.NODE_CONFIG_DEFAULTS)
    storage = defaults.setdefault("storage", {})
    if not isinstance(storage, dict):
        raise ValueError("default storage config must be an object")

    if args.subprocess_map:
        if not args.root_path:
            raise ValueError("--root-path is required with --subprocess-map")
        pool_dirs = compile_from_subprocess_map(args.root_path, args.subprocess_map, args.redirector)
        host, remote_path = split_remote_path(args.root_path, args.redirector)
        storage["eos_redirector"] = host
        storage["eos_lfn_base"] = remote_path.rstrip("/")
    else:
        pool_dirs = compile_from_pool_paths(args.pool_paths, args.redirector)

    if args.pool:
        requested_pools = set(args.pool)
        unknown_pools = sorted(requested_pools - set(pool_dirs))
        if unknown_pools:
            raise ValueError(
                "Requested pools are not present in the input mapping: "
                + ", ".join(unknown_pools)
            )
        pool_dirs = {
            pool: info for pool, info in pool_dirs.items()
            if pool in requested_pools
        }

    if not args.no_validate:
        for pool, info in pool_dirs.items():
            try:
                validate_pool_path(info["path"])
            except Exception as exc:
                raise RuntimeError(f"{pool}: {exc}") from exc

    storage.pop("lhe_pool_subdir", None)
    storage.pop("legacy_lhe_pool_subdir", None)
    defaults["lhe_pool_directories"] = pool_dirs

    output_text = json.dumps(defaults, indent=2, ensure_ascii=False) + "\n"
    if args.output == "-":
        sys.stdout.write(output_text)
    else:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(output_text)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
