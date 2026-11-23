"""Disposable negative admission hints; the locked ledger always grants work.

Every publication derives the entire view from one successfully read state.
Its age starts at that read, not at the later cache write. Slow or out-of-order
publishers therefore cannot keep an old observation alive indefinitely.
"""
import math
import os
from pathlib import Path
import time

from full_mc.io import atomic_json, read_json


HINT_SCHEMA = "mcprod-admission-hints-v1"
MAX_AGE_SECONDS = 120
_SOURCE_REASON = "waiting for earlier source admission in this pool"
_SHARD_REASON = "waiting for earlier assignments in this campaign"


def enabled():
    return os.environ.get("MCPROD_ADMISSION_HINTS") == "1"


def _path(root):
    return Path(root) / "ledger/admission-hints.json"


def _nonnegative_integer(value):
    return type(value) is int and value >= 0


def _fresh_timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        if not math.isfinite(value):
            return False
    except (OverflowError, ValueError):
        return False
    # A backwards wall-clock adjustment also forces an authoritative read.
    age = time.time() - value
    return 0 <= age < MAX_AGE_SECONDS


def _view(root, spec):
    if not enabled():
        return None
    try:
        view = read_json(_path(root))
    except (OSError, ValueError, UnicodeError):
        return None
    if (not isinstance(view, dict)
            or view.get("schema") != HINT_SCHEMA
            or not spec.get("run_id")
            or view.get("run_id") != spec["run_id"]
            or not _fresh_timestamp(view.get("observed_at"))):
        return None
    return view


def reject_source(root, spec, index):
    """Return a known ordering wait, or None to use the authoritative PRE."""
    view = _view(root, spec)
    if view is None:
        return None
    sources = spec.get("sources", [])
    if not _nonnegative_integer(index) or index >= len(sources):
        return None
    progress = view.get("source_next")
    if not isinstance(progress, dict):
        return None
    pool = sources[index].get("pool")
    # SOURCE's terminal skip precedes its ordering guard. Once all campaigns
    # using this pool are allocated, every remaining source may independently
    # record PRE_SKIP; a hint must not turn those skips into a serial chain.
    campaigns = spec.get("campaigns")
    assigned = view.get("shard_next")
    if not isinstance(campaigns, list) or not isinstance(assigned, dict):
        return None
    relevant = [campaign for campaign in campaigns
                if any(slot.get("pool") == pool
                       for slot in campaign.get("sources", []))]
    if not relevant:
        return None
    if all(_nonnegative_integer(assigned.get(campaign.get("name")))
           and type(campaign.get("max_processing_nodes")) is int
           and assigned[campaign["name"]] >= campaign["max_processing_nodes"]
           for campaign in relevant):
        return None
    first = progress.get(pool)
    if (not _nonnegative_integer(first) or first >= len(sources)
            or sources[first].get("pool") != pool):
        return None
    return _SOURCE_REASON if index > first else None


def reject_shard(root, spec, campaign, start):
    """Only reject future allocations; already allocated work must re-render."""
    view = _view(root, spec)
    if view is None or not _nonnegative_integer(start):
        return None
    progress = view.get("shard_next")
    if not isinstance(progress, dict):
        return None
    assigned = progress.get(campaign)
    if not _nonnegative_integer(assigned):
        return None
    return _SHARD_REASON if start > assigned else None


def publish(root, state, observed_at):
    """Best-effort publication after unlock, never part of a ledger commit.

    Call only for committed state or an audited unmodified Deferred snapshot.
    Missing/broken caches are harmless: callers use the original transaction.
    """
    if not enabled() or not _fresh_timestamp(observed_at):
        return
    try:
        spec = read_json(Path(root) / "run.json")
        run_id = spec.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            return
        planner = state["planner"]
        statuses = planner["sources"]
        source_next = {}
        for index, source in enumerate(spec["sources"]):
            pool = source["pool"]
            source_next.setdefault(pool, None)
            if source_next[pool] is None and str(index) not in statuses:
                source_next[pool] = index
        assigned = planner["assigned_progress"]
        if (not isinstance(assigned, dict)
                or any(not _nonnegative_integer(value)
                       for value in assigned.values())):
            return
        atomic_json(_path(root), {
            "schema": HINT_SCHEMA,
            "run_id": run_id,
            "observed_at": observed_at,
            "source_next": source_next,
            "shard_next": dict(assigned),
        })
    except (OSError, ValueError, TypeError, KeyError, AttributeError, UnicodeError):
        # This is a disposable cache. In particular, a publication failure must
        # never make a successful ledger commit look like a failed POST.
        return
