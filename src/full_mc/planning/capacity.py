"""Pure capacity estimates and staging-reservation checks."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, Mapping

from full_mc.domain.inventory import SourceBlock
from full_mc.domain.models import Campaign, Source


def complete_node_capacity(blocks_by_pool, campaign, cursors=None):
    """Use the production allocator for capacity, including repeated pools/tails."""
    from full_mc.domain.allocation import allocate_campaign_wave
    return len(allocate_campaign_wave(
        campaign, blocks_by_pool, cursors or {}, campaign.max_processing_nodes)["nodes"])


def campaign_capacity_report(blocks_by_pool: Mapping[str, list[SourceBlock]], campaign: Campaign,
                             cursors: Mapping[str, int] | None = None) -> Dict[str, Any]:
    capacity = complete_node_capacity(blocks_by_pool, campaign, cursors)
    slot_rows = []
    counts = Counter(slot.pool for slot in campaign.sources)
    for index, slot in enumerate(campaign.sources):
        available = sum(block.events for block in blocks_by_pool.get(slot.pool, [])[
            int((cursors or {}).get(slot.pool, 0)):])
        slot_rows.append({"slot": index, "pool": slot.pool, "mode": slot.mode,
                          "lhe_budget": slot.lhe_budget, "efficiency": slot.efficiency,
                          "expected_accepted_hepmc": slot.lhe_budget * slot.efficiency,
                          "pool_multiplicity": counts[slot.pool], "available_lhe_events": available})
    return {"campaign": campaign.name, "available_processing_nodes": capacity,
            "selected_processing_nodes": capacity,
            "expected_final_events": capacity * campaign.target_mixed_events,
            "slots": slot_rows}


def estimate_retained_bytes(campaign_reports: Iterable[Mapping[str, Any]], profiles: Mapping[str, Any],
                            existing_retained_bytes: int = 0) -> Dict[str, Any]:
    """Use configured/measured per-final-event bytes; no file-size inference."""
    bytes_per_event = int(profiles.get("bytes_per_final_event", 0) or 0)
    expected_events = sum(int(row.get("expected_final_events", 0)) for row in campaign_reports)
    new_bytes = expected_events * bytes_per_event
    return {"expected_final_events": expected_events, "bytes_per_final_event": bytes_per_event,
            "predicted_new_bytes": new_bytes, "existing_retained_bytes": int(existing_retained_bytes),
            "predicted_total_bytes": int(existing_retained_bytes) + new_bytes}


def staging_reservation(storage: Mapping[str, Any], requested_bytes: int, currently_reserved_bytes: int = 0) -> Dict[str, int | bool]:
    """Reserve actual declared output bytes while preserving the configured reserve."""
    requested = int(requested_bytes)
    reserved = int(currently_reserved_bytes)
    limit = int(storage["staging_bytes"]) - int(storage["reserve_bytes"])
    if requested < 0 or reserved < 0:
        raise ValueError("staging reservations cannot be negative")
    return {"requested_bytes": requested, "currently_reserved_bytes": reserved,
            "usable_bytes": limit, "accepted": reserved + requested <= limit,
            "remaining_bytes": max(0, limit - reserved - requested)}
