"""Deterministic, campaign-local views over shared source-block inventories."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .identity import deterministic_seed, edm_event_id
from .inventory import SourceBlock
from .models import Campaign, CampaignSource


class FinalInputShortfall(ValueError):
    """A proven exhausted pool cannot fill even one event in a requested slot."""


def stream_key(campaign: str, pool: str) -> str:
    return f"{campaign}::{pool}"


def _strict_int(value: Any, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        qualifier = "positive" if minimum == 1 else "non-negative"
        raise ValueError(f"{name} must be a {qualifier} integer")
    return value


def _campaign_entries(spec: Any) -> list[tuple[str, list[str]]]:
    raw_campaigns = spec.get("campaigns", []) if isinstance(spec, Mapping) else getattr(spec, "campaigns", [])
    result = []
    for campaign in raw_campaigns:
        if isinstance(campaign, Campaign):
            name = campaign.name
            pools = [slot.pool for slot in campaign.sources]
        else:
            name = str(campaign.get("name", ""))
            pools = [str(item.get("pool", "")) for item in campaign.get("sources", [])]
        if not name:
            raise ValueError("campaign name is required to build consumption streams")
        result.append((name, pools))
    return result


def _as_campaign(value: Campaign | Mapping[str, Any]) -> Campaign:
    if isinstance(value, Campaign):
        return value
    sources = [CampaignSource(item["pool"], item["mode"], item["lhe_budget"],
                              item.get("efficiency", 1.0)) for item in value["sources"]]
    variants = list(value.get("resolved_detector_variants", []))
    names = [item["profile_id"] for item in variants]
    return Campaign(value["name"], value["analysis"], sources,
                    value["target_mixed_events"], value["max_processing_nodes"],
                    value["merge_events"], names, variants,
                    int(value.get("global_job_index_start", 0)),
                    int(value.get("merge_group_index_start", 0)))


def build_streams(spec: Any) -> dict[str, dict[str, Any]]:
    """Build stream state for every campaign/pool membership in a run spec.

    Optional ``consumption_streams`` entries use ``campaign``, ``pool``,
    ``stride`` and ``offset`` fields. Unspecified streams consume every block.
    """
    memberships: dict[str, set[str]] = {}
    for campaign, pools in _campaign_entries(spec):
        memberships[campaign] = set(pools)
    raw_overrides = spec.get("consumption_streams", []) if isinstance(spec, Mapping) else getattr(spec, "consumption_streams", [])
    if not isinstance(raw_overrides, list):
        raise ValueError("consumption_streams must be a list of objects")
    overrides: dict[str, tuple[int, int]] = {}
    for item in raw_overrides:
        if not isinstance(item, Mapping):
            raise ValueError("each consumption_streams entry must be an object")
        campaign = str(item.get("campaign", ""))
        pool = str(item.get("pool", ""))
        if not campaign or not pool or pool not in memberships.get(campaign, set()):
            raise ValueError(f"consumption stream {campaign!r}/{pool!r} is not a campaign pool membership")
        key = stream_key(campaign, pool)
        if key in overrides:
            raise ValueError(f"duplicate consumption stream {key}")
        stride = _strict_int(item.get("stride", 1), f"{key}.stride", minimum=1)
        offset = _strict_int(item.get("offset", 0), f"{key}.offset")
        if offset >= stride:
            raise ValueError(f"{key}.offset must be smaller than stride")
        overrides[key] = (stride, offset)
    streams: dict[str, dict[str, Any]] = {}
    for campaign, pools in _campaign_entries(spec):
        for pool in pools:
            key = stream_key(campaign, pool)
            stride, offset = overrides.get(key, (1, 0))
            streams[key] = {"stride": stride, "offset": offset, "allocation_cursor": offset,
                            "safe_cursor": offset, "closed": False}
    return streams


def next_matching(index: int, stream: Mapping[str, Any]) -> int:
    """Return the first inventory index at or after ``index`` in this stream."""
    stride = _strict_int(stream.get("stride"), "stream.stride", minimum=1)
    offset = _strict_int(stream.get("offset"), "stream.offset")
    if offset >= stride:
        raise ValueError("stream.offset must be smaller than stride")
    index = _strict_int(index, "index")
    return index + ((offset - index) % stride)


def may_still_need(index: int, stream: Mapping[str, Any]) -> bool:
    """Whether an inventory index remains in the unconsumed stream suffix."""
    if stream.get("closed", False):
        return False
    index = _strict_int(index, "index")
    stride = _strict_int(stream.get("stride"), "stream.stride", minimum=1)
    offset = _strict_int(stream.get("offset"), "stream.offset")
    cursor = _strict_int(stream.get("safe_cursor"), "stream.safe_cursor")
    return index >= cursor and (index - offset) % stride == 0


def _stream_block(blocks: Sequence[SourceBlock], stream: Mapping[str, Any], cursor: int) -> SourceBlock | None:
    index = next_matching(cursor, stream)
    return blocks[index] if index < len(blocks) else None


def allocate_shard(campaign: Campaign | Mapping[str, Any], blocks_by_pool: Mapping[str, Sequence[SourceBlock]],
                   streams: Mapping[str, Mapping[str, Any]], start_job_index: int,
                   node_limit: int, exhausted_pools: set[str],
                   global_job_index_start: int | None = None) -> dict[str, Any] | None:
    """Allocate a deterministic shard without changing caller-owned state.

    A temporary shortage in any requested slot rejects the whole shard. A
    proven exhausted pool may contribute a nonempty tail, which is reported in
    ``shortfalls`` so downstream mixing can truncate to the common event prefix.
    """
    campaign = _as_campaign(campaign)
    start_job_index = _strict_int(start_job_index, "start_job_index")
    if global_job_index_start is None:
        global_job_index_start = campaign.global_job_index_start
    global_job_index_start = _strict_int(global_job_index_start, "global_job_index_start")
    node_limit = _strict_int(node_limit, "node_limit", minimum=1)
    if not isinstance(exhausted_pools, set):
        raise ValueError("exhausted_pools must be a set")
    keys = {slot.pool: stream_key(campaign.name, slot.pool) for slot in campaign.sources}
    for pool, key in keys.items():
        if key not in streams:
            raise ValueError(f"missing consumption stream {key}")
        stream = streams[key]
        _strict_int(stream.get("stride"), f"{key}.stride", minimum=1)
        _strict_int(stream.get("offset"), f"{key}.offset")
        if stream["offset"] >= stream["stride"]:
            raise ValueError(f"{key}.offset must be smaller than stride")
        _strict_int(stream.get("allocation_cursor", 0), f"{key}.allocation_cursor")
        _strict_int(stream.get("safe_cursor", 0), f"{key}.safe_cursor")
        if stream.get("closed", False):
            raise ValueError(f"consumption stream {key} is closed")
    max_nodes = min(node_limit, max(0, campaign.max_processing_nodes - start_job_index))
    starting = {keys[slot.pool]: streams[keys[slot.pool]]["allocation_cursor"] for slot in campaign.sources}
    cursors = dict(starting)
    nodes: list[dict[str, Any]] = []
    shortfalls: list[dict[str, Any]] = []
    for node_offset in range(max_nodes):
        job_index = start_job_index + node_offset
        global_job_index = global_job_index_start + job_index
        job_id = f"JOB000000_BLOCK{job_index:06d}"
        trial = dict(cursors)
        slot_records = []
        node_shortfalls = []
        for slot_index, slot in enumerate(campaign.sources):
            key = keys[slot.pool]
            stream = streams[key]
            blocks = blocks_by_pool.get(slot.pool, ())
            next_index = next_matching(trial[key], stream)
            chosen: list[SourceBlock] = []
            total = 0
            while total < slot.lhe_budget:
                block = _stream_block(blocks, stream, next_index)
                if block is None:
                    if slot.pool not in exhausted_pools:
                        return None
                    if not chosen:
                        raise FinalInputShortfall(
                            f"campaign {campaign.name} slot {slot_index} pool {slot.pool} has no remaining blocks")
                    node_shortfalls.append({"slot": slot_index, "pool": slot.pool,
                                            "requested_events": slot.lhe_budget,
                                            "available_events": total,
                                            "missing_events": slot.lhe_budget - total})
                    break
                chosen.append(block)
                total += block.events
                next_index += stream["stride"]
            trial[key] = next_index
            refs = [block.artifact_id for block in chosen]
            slot_records.append({"slot": slot_index, "pool": slot.pool, "mode": slot.mode,
                                 "lhe_budget": slot.lhe_budget, "block_refs": refs,
                                 "lhe_events": total,
                                 "source_rng_seed": deterministic_seed(
                                     f"{campaign.name}|{global_job_index}|source={slot_index}|"
                                     f"{';'.join(refs)}|{slot.mode}")})
        inputs = [ref for record in slot_records for ref in record["block_refs"]]
        modes = [slot.mode for slot in campaign.sources]
        nodes.append({"job_index": job_index, "job_id": job_id, "sources": slot_records,
                      "target_mixed_events": campaign.target_mixed_events,
                      "global_job_index": global_job_index,
                      "edm_event_id": edm_event_id(0, global_job_index, campaign.target_mixed_events),
                      "mixing_rng_seed": deterministic_seed(
                          f"{campaign.name}|{global_job_index}|shuffle|"
                          f"{','.join(inputs)}|{','.join(modes)}")})
        for entry in node_shortfalls:
            shortfalls.append({"job_index": job_index, **entry})
        cursors = trial
    return {"campaign": campaign.name, "start_job_index": start_job_index,
            "start_cursors": starting, "end_cursors": cursors, "nodes": nodes,
            "shortfalls": shortfalls, "complete": len(nodes) == max_nodes}
