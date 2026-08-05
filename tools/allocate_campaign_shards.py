#!/usr/bin/env python3
"""Allocate campaign shards from completed, authoritative LHE plan manifests."""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    return parser.parse_args()


def write_json_file(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def load_pool_streams(source_manifest_path: Path) -> dict[str, list[int]]:
    source_infos = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if not isinstance(source_infos, list) or not source_infos:
        raise ValueError("source_manifests_path must contain a non-empty list")
    streams: dict[str, list[int]] = defaultdict(list)
    for source in source_infos:
        pool = str(source["pool"])
        manifest_path = Path(str(source["path"]))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        blocks = manifest.get("blocks")
        if not isinstance(blocks, list) or not blocks:
            raise ValueError(f"planner manifest has no blocks: {manifest_path}")
        for block in blocks:
            events = int(block.get("n_events", 0) or 0)
            if events <= 0:
                raise ValueError(
                    f"planner manifest has an empty block: {manifest_path}"
                )
            streams[pool].append(events)
    return dict(streams)


def allocate_campaign(
    streams: dict[str, list[int]],
    campaign_inputs: list[str],
    budgets: list[int],
    start_cursors: dict[str, int],
    node_limit: int,
) -> tuple[int, dict[str, int], list[int], list[int]]:
    cursors = dict(start_cursors)
    nodes = 0
    events_by_slot = [0] * len(campaign_inputs)
    blocks_by_slot = [0] * len(campaign_inputs)
    while node_limit <= 0 or nodes < node_limit:
        next_cursors = dict(cursors)
        node_events = [0] * len(campaign_inputs)
        node_blocks = [0] * len(campaign_inputs)
        can_build = True
        for slot, (pool, budget) in enumerate(zip(campaign_inputs, budgets)):
            blocks = streams[pool]
            cursor = next_cursors.get(pool, 0)
            accumulated = 0
            chosen = 0
            while cursor < len(blocks) and (
                chosen == 0 or accumulated < budget
            ):
                accumulated += blocks[cursor]
                cursor += 1
                chosen += 1
            if chosen == 0 or accumulated < budget:
                can_build = False
                break
            next_cursors[pool] = cursor
            node_events[slot] = accumulated
            node_blocks[slot] = chosen
        if not can_build:
            break
        cursors = next_cursors
        nodes += 1
        events_by_slot = [
            total + value for total, value in zip(events_by_slot, node_events)
        ]
        blocks_by_slot = [
            total + value for total, value in zip(blocks_by_slot, node_blocks)
        ]
    return nodes, cursors, events_by_slot, blocks_by_slot


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    source_manifest_path = Path(str(config["source_manifests_path"]))
    output_path = Path(str(config["output_path"]))
    shard_nodes = int(config["shard_processing_nodes"])
    if shard_nodes <= 0:
        raise ValueError("shard_processing_nodes must be positive")

    campaigns = config["campaigns"]
    allocation_order = config["campaign_allocation_order"]
    if (
        not isinstance(campaigns, dict)
        or not isinstance(allocation_order, list)
        or len(allocation_order) != len(campaigns)
        or len(set(allocation_order)) != len(allocation_order)
        or set(allocation_order) != set(campaigns)
    ):
        raise ValueError("campaign allocation config is inconsistent")

    streams = load_pool_streams(source_manifest_path)
    pool_cursors = {pool: 0 for pool in streams}
    campaign_output: dict[str, object] = {}
    for campaign_name in allocation_order:
        campaign = campaigns[campaign_name]
        inputs = [str(value) for value in campaign["campaign_inputs"]]
        budgets = [int(value) for value in campaign["source_lhe_budgets"]]
        if len(inputs) != len(budgets) or not inputs:
            raise ValueError(f"{campaign_name}: invalid inputs or budgets")
        missing = sorted(set(inputs) - set(streams))
        if missing:
            raise ValueError(
                f"{campaign_name}: source stream misses {', '.join(missing)}"
            )
        cap = int(campaign.get("max_processing_nodes", 0) or 0)
        campaign_pools = tuple(dict.fromkeys(inputs))
        campaign_start = dict(pool_cursors)
        (
            processing_nodes,
            campaign_end,
            events_by_slot,
            blocks_by_slot,
        ) = allocate_campaign(
            streams,
            inputs,
            budgets,
            campaign_start,
            cap,
        )
        if processing_nodes <= 0:
            raise ValueError(f"{campaign_name}: no processing nodes allocated")

        shards = []
        shard_cursor = dict(campaign_start)
        for start_index in range(0, processing_nodes, shard_nodes):
            shard_index = start_index // shard_nodes
            node_count = min(shard_nodes, processing_nodes - start_index)
            shard_start = dict(shard_cursor)
            selected, shard_cursor, _, _ = allocate_campaign(
                streams,
                inputs,
                budgets,
                shard_start,
                node_count,
            )
            if selected != node_count:
                raise ValueError(
                    f"{campaign_name}: shard {shard_index} allocated "
                    f"{selected} of {node_count} nodes"
                )
            shards.append({
                "shard_index": shard_index,
                "node_count": node_count,
                "pool_start_blocks": {
                    pool: shard_start[pool] for pool in campaign_pools
                },
                "pool_end_blocks": {
                    pool: shard_cursor[pool] for pool in campaign_pools
                },
            })
        if shard_cursor != campaign_end:
            raise ValueError(f"{campaign_name}: shard allocation end mismatch")
        expected_shards = int(campaign.get("expected_shards", 0) or 0)
        if expected_shards > 0 and len(shards) != expected_shards:
            raise ValueError(
                f"{campaign_name}: actual planner blocks require "
                f"{len(shards)} shards, but the DAG contains "
                f"{expected_shards} coordinator nodes"
            )
        pool_cursors = campaign_end
        campaign_output[campaign_name] = {
            "processing_nodes": processing_nodes,
            "pool_start_blocks": {
                pool: campaign_start[pool] for pool in campaign_pools
            },
            "pool_end_blocks": {
                pool: campaign_end[pool] for pool in campaign_pools
            },
            "planned_lhe_events_by_slot": events_by_slot,
            "planned_blocks_by_slot": blocks_by_slot,
            "shards": shards,
        }

    payload = {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_manifests_path": str(source_manifest_path),
        "blocks_by_pool": {
            pool: len(blocks) for pool, blocks in streams.items()
        },
        "events_by_pool": {
            pool: sum(blocks) for pool, blocks in streams.items()
        },
        "campaign_allocation_order": allocation_order,
        "campaigns": campaign_output,
        "final_pool_cursors": pool_cursors,
    }
    write_json_file(output_path, payload)
    print(f"[OK] Campaign allocation manifest written: {output_path}")
    for campaign_name in allocation_order:
        campaign = campaign_output[campaign_name]
        print(
            f"[OK] {campaign_name}: {campaign['processing_nodes']} nodes, "
            f"{len(campaign['shards'])} shards"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
