"""Exact immutable allocation snapshots for one bounded wave."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence

from .identity import deterministic_seed, edm_event_id
from .inventory import SourceBlock
from .models import Campaign


def _take_budget(blocks: Sequence[SourceBlock], cursor: int, budget: int) -> tuple[int, List[SourceBlock]] | None:
    selected: List[SourceBlock] = []
    total = 0
    while cursor < len(blocks) and (not selected or total < budget):
        selected.append(blocks[cursor])
        total += blocks[cursor].events
        cursor += 1
    # A nonempty final tail can still produce a valid common mixed prefix.
    return (cursor, selected) if selected else None


def allocate_campaign_wave(campaign: Campaign, blocks_by_pool: Mapping[str, Sequence[SourceBlock]],
                           starting_cursors: Mapping[str, int], node_limit: int,
                           global_job_offset: int = 0) -> Dict[str, Any]:
    """Atomically calculate an allocation before any child starts.

    The returned snapshot contains exact compact block IDs.  It can be persisted
    and replayed after a coordinator retry without consulting mutable planners.
    """
    if node_limit <= 0:
        raise ValueError("node_limit must be positive")
    cursors = dict(starting_cursors)
    nodes = []
    max_nodes = min(node_limit, max(0, campaign.max_processing_nodes - global_job_offset))
    for node_index in range(max_nodes):
        index = global_job_offset + node_index
        global_index = campaign.global_job_index_start + index
        job_id = f"JOB000000_BLOCK{index:06d}"
        trial = dict(cursors)
        slot_records = []
        for slot_index, slot in enumerate(campaign.sources):
            selected = _take_budget(blocks_by_pool.get(slot.pool, ()), trial.get(slot.pool, 0), slot.lhe_budget)
            if selected is None:
                return {"campaign": campaign.name, "start_cursors": dict(starting_cursors),
                        "end_cursors": cursors, "nodes": nodes, "complete": False}
            end, chosen = selected
            trial[slot.pool] = end
            refs = [item.artifact_id for item in chosen]
            slot_records.append({"slot": slot_index, "pool": slot.pool, "mode": slot.mode,
                                 "lhe_budget": slot.lhe_budget, "block_refs": refs,
                                 "lhe_events": sum(item.events for item in chosen),
                                 "source_rng_seed": deterministic_seed(
                                     f"{campaign.name}|{global_index}|source={slot_index}|"
                                     f"{';'.join(refs)}|{slot.mode}")})
        inputs = [ref for slot in slot_records for ref in slot["block_refs"]]
        modes = [slot.mode for slot in campaign.sources]
        nodes.append({"job_index": global_job_offset + node_index,
                      "global_job_index": global_index,
                      "job_id": job_id,
                      "sources": slot_records, "target_mixed_events": campaign.target_mixed_events,
                      "edm_event_id": edm_event_id(0, global_index, campaign.target_mixed_events),
                      "mixing_rng_seed": deterministic_seed(
                          f"{campaign.name}|{global_index}|shuffle|"
                          f"{','.join(inputs)}|{','.join(modes)}")})
        cursors = trial
    return {"campaign": campaign.name, "start_cursors": dict(starting_cursors),
            "end_cursors": cursors, "nodes": nodes, "complete": len(nodes) == max_nodes}


def allocate_wave(campaigns: Sequence[Campaign], blocks_by_pool: Mapping[str, Sequence[SourceBlock]],
                  pool_cursors: Mapping[str, int], wave_nodes: int,
                  job_offsets: Mapping[str, int] | None = None) -> Dict[str, Any]:
    """Allocate campaigns serially from shared pools and return a frozen snapshot."""
    cursors = dict(pool_cursors)
    offsets = dict(job_offsets or {})
    allocations = []
    remaining = wave_nodes
    for campaign in campaigns:
        if remaining <= 0:
            break
        allocation = allocate_campaign_wave(campaign, blocks_by_pool, cursors, remaining,
                                            offsets.get(campaign.name, 0))
        allocations.append(allocation)
        cursors = allocation["end_cursors"]
        offsets[campaign.name] = offsets.get(campaign.name, 0) + len(allocation["nodes"])
        remaining -= len(allocation["nodes"])
    return {"version": 1, "pool_cursors_before": dict(pool_cursors), "pool_cursors_after": cursors,
            "job_offsets_after": offsets, "campaigns": allocations}
