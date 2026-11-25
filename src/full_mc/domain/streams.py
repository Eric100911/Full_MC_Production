"""Deterministic static allocation over campaign-local source streams."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .identity import deterministic_seed, edm_event_id
from .inventory import SourceBlock
from .models import Campaign


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


def build_streams(spec: Any) -> dict[str, dict[str, Any]]:
    """Build the selection rules for each campaign/pool membership.

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
            streams[key] = {"stride": stride, "offset": offset}
    return streams


def next_matching(index: int, stream: Mapping[str, Any]) -> int:
    """Return the first inventory index at or after ``index`` in this stream."""
    stride = _strict_int(stream.get("stride"), "stream.stride", minimum=1)
    offset = _strict_int(stream.get("offset"), "stream.offset")
    if offset >= stride:
        raise ValueError("stream.offset must be smaller than stride")
    index = _strict_int(index, "index")
    return index + ((offset - index) % stride)



def allocate_campaign(campaign: Mapping[str, Any],
                      blocks_by_pool: Mapping[str, Sequence[SourceBlock]],
                      streams: Mapping[str, Mapping[str, Any]], block_events: int) -> dict[str, Any]:
    """Bind complete sets of source slots up to the campaign node limit.

    Full blocks can fill multi-block slot budgets. A source tail gets a slot of
    its own. If any slot has run out, leave every block considered for that
    incomplete node in the unused list.
    """
    name = campaign["name"]
    global_start = campaign.get("global_job_index_start", 0)
    target = campaign["target_mixed_events"]
    slots = campaign["sources"]
    pools = {slot["pool"]: streams[stream_key(name, slot["pool"])] for slot in slots}
    selected = {pool: blocks_by_pool.get(pool, ())[rule["offset"]::rule["stride"]]
                for pool, rule in pools.items()}
    positions = dict.fromkeys(selected, 0)
    nodes = []
    for job_index in range(campaign["max_processing_nodes"]):
        global_job_index = global_start + job_index
        next_positions = positions.copy()
        bound = []
        for slot_index, slot in enumerate(slots):
            pool, mode = slot["pool"], slot["mode"]
            blocks, start = selected[pool], next_positions[pool]
            end, total = start, 0
            while end < len(blocks) and total < slot["lhe_budget"]:
                block = blocks[end]
                if total and block.events < block_events:
                    break
                total += block.events
                end += 1
                if block.events < block_events:
                    break
            if end == start:
                break
            next_positions[pool] = end
            refs = [block.artifact_id for block in blocks[start:end]]
            bound.append({"slot": slot_index, "pool": pool, "mode": mode,
                          "block_refs": refs, "lhe_events": total,
                          "source_rng_seed": deterministic_seed(
                              f"{name}|{global_job_index}|source={slot_index}|"
                              f"{';'.join(refs)}|{mode}")})
        if len(bound) != len(slots):
            break
        positions = next_positions
        inputs = [ref for record in bound for ref in record["block_refs"]]
        modes = [slot["mode"] for slot in slots]
        events = min(target, *(slot["lhe_events"] for slot in bound))
        nodes.append({"job_index": job_index, "sources": bound,
                      "target_mixed_events": events, "global_job_index": global_job_index,
                      "edm_event_id": edm_event_id(0, global_job_index, target),
                      "mixing_rng_seed": deterministic_seed(
                          f"{name}|{global_job_index}|shuffle|"
                          f"{','.join(inputs)}|{','.join(modes)}")})
    unused = {pool: [block.artifact_id for block in selected[pool][position:]]
              for pool, position in sorted(positions.items())}
    return {"nodes": nodes, "unused_blocks": unused}
