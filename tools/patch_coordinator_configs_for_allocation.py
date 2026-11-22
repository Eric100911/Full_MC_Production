#!/usr/bin/env python3
"""Patch generated coordinator configs to use shared manifests and shard cursors."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", required=True)
    parser.add_argument("--allocation-manifest", required=True)
    parser.add_argument(
        "--campaign-source",
        action="append",
        default=[],
        metavar="CAMPAIGN=PATH",
        help="Shared source-manifest file for one campaign",
    )
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def load_sources(values: list[str]) -> dict[str, Path]:
    result = {}
    for value in values:
        campaign, separator, path = value.partition("=")
        if not separator or not campaign or not path:
            raise ValueError(f"invalid --campaign-source value: {value}")
        result[campaign] = Path(path).resolve()
    if not result:
        raise ValueError("at least one --campaign-source is required")
    return result


def atomic_write(path: Path, payload: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> int:
    args = parse_args()
    config_dir = Path(args.config_dir)
    allocation_path = Path(args.allocation_manifest).resolve()
    source_paths = load_sources(args.campaign_source)
    allocation = json.loads(allocation_path.read_text(encoding="utf-8"))

    source_counts = {}
    for campaign, path in source_paths.items():
        entries = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(entries, list) or not entries:
            raise ValueError(f"{campaign}: source manifest is empty: {path}")
        source_counts[campaign] = len(entries)

    updates: list[tuple[Path, dict[str, object]]] = []
    seen = set()
    for path in sorted(config_dir.glob("COORD_*.json")):
        config = json.loads(path.read_text(encoding="utf-8"))
        campaign = str(config["campaign"])
        if campaign not in source_paths:
            continue
        shard_index = int(config["job_index"])
        shards = allocation["campaigns"][campaign]["shards"]
        try:
            shard = next(
                record
                for record in shards
                if int(record["shard_index"]) == shard_index
            )
        except StopIteration as error:
            raise ValueError(
                f"{path.name}: allocation has no shard {shard_index}"
            ) from error

        config.pop("source_manifests", None)
        config["source_manifests_path"] = str(source_paths[campaign])
        config["source_manifests_count"] = source_counts[campaign]
        config["allocation_manifest_path"] = str(allocation_path)
        config["allocation_shard_index"] = shard_index
        config["pool_start_blocks"] = {}
        config["processing_start_index"] = 0
        config["max_processing_nodes"] = int(shard["node_count"])
        updates.append((path, config))
        seen.add((campaign, shard_index))

    expected = {
        (campaign, int(shard["shard_index"]))
        for campaign in source_paths
        for shard in allocation["campaigns"][campaign]["shards"]
    }
    if seen != expected:
        missing = sorted(expected - seen)
        extra = sorted(seen - expected)
        raise ValueError(
            f"coordinator config/shard mismatch: missing={missing}, extra={extra}"
        )

    mode = "APPLY" if args.apply else "DRY-RUN"
    for path, config in updates:
        if args.apply:
            atomic_write(path, config)
        print(
            f"[{mode}] {path.name}: campaign={config['campaign']} "
            f"shard={config['allocation_shard_index']} "
            f"nodes={config['max_processing_nodes']} "
            f"sources={config['source_manifests_count']}"
        )
    print(f"[{mode}] validated {len(updates)} coordinator configs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
