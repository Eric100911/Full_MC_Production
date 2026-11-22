"""Authoritative finite source inventory handling."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List

from .models import Source


@dataclass(frozen=True)
class SourceBlock:
    artifact_id: str
    pool: str
    source_id: str
    index: int
    events: int
    source_url: str

    @property
    def compact_ref(self) -> str:
        return self.artifact_id


def source_blocks(sources: Iterable[Source], block_events: int) -> Dict[str, List[SourceBlock]]:
    """Split known finite inventories while retaining the final partial tail.

    The order is the selected inventory order, then source-local block.
    This is the only ordering consumed by the allocator, preventing inferred
    ranges, source overlap, and silently skipped tails.
    """
    if block_events <= 0:
        raise ValueError("block_events must be positive")
    result: Dict[str, List[SourceBlock]] = {}
    for source in sources:
        stream = result.setdefault(source.pool, [])
        remaining = source.available_events
        local_index = 0
        while remaining:
            events = min(block_events, remaining)
            index = len(stream)
            artifact_id = f"BLOCK:{source.pool}:{source.source_id}:{local_index:06d}"
            stream.append(SourceBlock(artifact_id, source.pool, source.source_id, index, events, source.url))
            remaining -= events
            local_index += 1
    return result


def compact_inventory(blocks_by_pool: Dict[str, List[SourceBlock]]) -> Dict[str, object]:
    """A single compact index; task JSON references artifact IDs, not source lists."""
    return {"version": 1, "pools": {pool: [
        {"artifact_id": block.artifact_id, "source_id": block.source_id,
         "events": block.events, "original_source_url": block.source_url}
        for block in blocks] for pool, blocks in sorted(blocks_by_pool.items())}}
