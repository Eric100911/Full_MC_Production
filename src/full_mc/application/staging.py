"""Locked production-state transactions.

This module performs local control-record I/O only. Remote publication and
removal happen in workers, before their exact results are accepted here.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import fcntl
import json
import os
import tempfile
import time
from pathlib import Path

from full_mc.application import admission_hints, control_events

FUSED_STATE_SCHEMA = "mcprod-shard-state-v3"
INVENTORY_SEGMENT_SCHEMA = "mcprod-plan-inventory-v1"


def require_schema(record):
    if record.get("state_schema") != FUSED_STATE_SCHEMA:
        raise ValueError(f"requires {FUSED_STATE_SCHEMA}. Use the frozen runtime for historical runs.")


class LockBusy(BlockingIOError):
    """The nonblocking ledger flock alone was unavailable."""


class Deferred(Exception):
    """Abort a proven no-mutation transaction and return a temporary hold."""

    def __init__(self, reason, code=75):
        super().__init__(reason)
        self.code = code
        self.reason = reason


def _check_inventory_segments(state):
    """Validate the compact, acceptance-ordered PLAN segment index."""
    inventory = state.get("planner", {}).get("inventory_segments")
    if (not isinstance(inventory, dict)
            or set(inventory) != {"schema", "pools"}
            or inventory.get("schema") != INVENTORY_SEGMENT_SCHEMA
            or not isinstance(inventory.get("pools"), dict)):
        raise ValueError("invalid v2 PLAN inventory segment index")
    seen = set()
    for pool, identities in inventory["pools"].items():
        if not isinstance(pool, str) or not pool:
            raise ValueError("invalid PLAN inventory pool")
        if not isinstance(identities, list):
            raise ValueError("PLAN inventory segments must be an ordered list")
        for identity in identities:
            if (not isinstance(identity, str) or not identity.startswith("PLAN_")
                    or not identity[5:].isascii() or not identity[5:].isdigit()
                    or f"PLAN_{int(identity[5:]):06d}" != identity):
                raise ValueError("invalid PLAN inventory segment identity")
            marker = state.get("accepted", {}).get(identity)
            if not isinstance(marker, dict) or marker.get("stage") != "plan":
                raise ValueError("PLAN inventory segment lacks an accepted PLAN result")
            if identity in seen:
                raise ValueError("PLAN inventory segment is repeated")
            seen.add(identity)
    accepted_plans = {identity for identity, marker in state.get("accepted", {}).items()
                      if isinstance(marker, dict) and marker.get("stage") == "plan"}
    if seen != accepted_plans:
        raise ValueError("PLAN inventory segments disagree with accepted PLAN results")


def _serialize(state):
    """Return the exact on-disk form of a state record."""
    return json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n"


def _write_state(root, state, previous=None):
    """Replace state only after the complete temporary record reaches disk.

    ``previous`` is the record already on disk. When the serialized state is
    byte-identical to it the file is left untouched: a transaction that only
    reads state must not pay a full-record rewrite. Callers that require an
    unconditional write pass no ``previous``.

    Unsupported durability operations on the submit filesystem are hard
    failures. Local tests do not establish file/directory fsync or flock
    behavior on AFS.
    A directory fsync failure occurs after replace, so the new state may already
    be visible and must be reread on retry rather than assumed rolled back.
    """
    timed = control_events.active()
    started = time.perf_counter() if timed else None
    payload = _serialize(state)
    if timed:
        control_events.record_duration("serialize", time.perf_counter() - started)
        started = time.perf_counter()
    if previous is not None and payload == previous:
        if timed:
            control_events.record_duration("commit", time.perf_counter() - started)
            control_events.record_commit(False, "no_op")
        return False
    if timed:
        control_events.record_commit(False, "failed_before_replace")
    fd, name = tempfile.mkstemp(prefix=".state-", suffix=".json", dir=root)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            phase_started = time.perf_counter() if timed else None
            os.fsync(handle.fileno())
            if timed:
                control_events.record_duration("file_fsync", time.perf_counter() - phase_started)
        phase_started = time.perf_counter() if timed else None
        os.replace(name, Path(root) / "state.json")
        if timed:
            control_events.record_duration("replace", time.perf_counter() - phase_started)
            control_events.record_commit(False, "visible_unsynced")
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            phase_started = time.perf_counter() if timed else None
            os.fsync(directory)
            if timed:
                control_events.record_duration("directory_fsync", time.perf_counter() - phase_started)
                control_events.record_commit(True, "replacement_synced")
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)
        if timed:
            control_events.record_duration("commit", time.perf_counter() - started)
    return True


def initialize(root, planner, pools):
    """Create a durable run state with an acceptance-ordered PLAN inventory."""
    root = Path(root)
    (root / "ledger").mkdir()
    fd = os.open(root / "ledger/state.lock", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    planner = deepcopy(planner)
    planner["inventory_segments"] = {"schema": INVENTORY_SEGMENT_SCHEMA,
                                     "pools": {pool: [] for pool in sorted(pools)}}
    state = {"state_schema": FUSED_STATE_SCHEMA, "status": "planned",
             "planner": planner, "accepted": {}, "allocations": {}, "merge_group_counts": {}}
    require_schema(state)
    _check_inventory_segments(state)
    _write_state(root, state)
    return state


@contextmanager
def transaction(root):
    """Read, modify and durably replace state under a nonblocking whole-file lock."""
    root = Path(root)
    # No O_CREAT: recovery must never silently replace a missing lock inode.
    timed = control_events.active()
    started = time.perf_counter() if timed else None
    try:
        lock_file = (root / "ledger/state.lock").open("r+b")
    except BaseException:
        if timed:
            control_events.record_duration("lock_attempt", time.perf_counter() - started)
        raise
    with lock_file as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            if timed:
                control_events.record_duration("lock_attempt", time.perf_counter() - started)
            raise LockBusy(str(exc)) from exc
        except BaseException:
            if timed:
                control_events.record_duration("lock_attempt", time.perf_counter() - started)
            raise
        if timed:
            control_events.record_duration("lock_attempt", time.perf_counter() - started)
        publication_state = None
        observed_at = None
        publication_kind = None
        if timed:
            control_events.record_commit(False, "pending")
        try:
            read_started = time.perf_counter() if timed else None
            previous = (root / "state.json").read_text(encoding="utf-8")
            if timed:
                control_events.record_duration("state_read", time.perf_counter() - read_started)
            if admission_hints.enabled():
                observed_at = time.time()
            parse_started = time.perf_counter() if timed else None
            state = json.loads(previous)
            if timed:
                control_events.record_duration("state_parse", time.perf_counter() - parse_started)
            check_started = time.perf_counter() if timed else None
            require_schema(state)
            _check_inventory_segments(state)
            if timed:
                control_events.record_duration("entry_check", time.perf_counter() - check_started)
            body_started = time.perf_counter() if timed else None
            try:
                yield state
            except Deferred:
                if timed:
                    control_events.record_duration("transaction_body", time.perf_counter() - body_started)
                    control_events.record_commit(False, "deferred")
                publication_state = state
                publication_kind = "deferred"
                raise
            except BaseException:
                if timed:
                    control_events.record_duration("transaction_body", time.perf_counter() - body_started)
                raise
            if timed:
                control_events.record_duration("transaction_body", time.perf_counter() - body_started)
            check_started = time.perf_counter() if timed else None
            require_schema(state)
            _check_inventory_segments(state)
            if timed:
                control_events.record_duration("exit_check", time.perf_counter() - check_started)
            _write_state(root, state, previous)
            publication_state = state
            publication_kind = "committed"
        except BaseException:
            if timed and control_events.current_commit_outcome() not in {
                    "deferred", "visible_unsynced", "replacement_synced"}:
                control_events.record_commit(False, "failed_before_commit")
            raise
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            if publication_kind is not None and admission_hints.enabled():
                admission_hints.publish(root, publication_state, observed_at)
