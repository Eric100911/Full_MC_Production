"""Locked production-state transactions and legacy staging accounting.

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

STATE_SCHEMA = "mcprod-shard-state-v1"
INVENTORY_STATE_SCHEMA = "mcprod-shard-state-v2"
FUSED_STATE_SCHEMA = "mcprod-shard-state-v3"
INVENTORY_SEGMENT_SCHEMA = "mcprod-plan-inventory-v1"
_OBJECT_KEYS = {"artifact_id", "url", "size_bytes", "kind"}


class LockBusy(BlockingIOError):
    """The nonblocking ledger flock alone was unavailable."""


class Deferred(Exception):
    """Abort a proven no-mutation transaction and return a temporary hold."""

    def __init__(self, reason, code=75):
        super().__init__(reason)
        self.code = code
        self.reason = reason


def _bytes(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _objects(records):
    result = []
    identities = {}
    artifact_ids = {}
    for record in records:
        if not isinstance(record, dict) or set(record) != _OBJECT_KEYS:
            raise ValueError("staged object requires exact artifact_id/url/size_bytes/kind")
        item = dict(record)
        for key in ("artifact_id", "url", "kind"):
            if not isinstance(item[key], str) or not item[key]:
                raise ValueError(f"staged object {key} is required")
        _bytes(item["size_bytes"], "size_bytes")
        if item["artifact_id"] in artifact_ids and artifact_ids[item["artifact_id"]] != item:
            raise ValueError("conflicting staged object identity")
        artifact_ids[item["artifact_id"]] = item
        if item["url"] in identities:
            if identities[item["url"]] != item:
                raise ValueError("conflicting staged object identity")
            continue
        identities[item["url"]] = item
        result.append(item)
    return sorted(result, key=lambda item: item["url"])


def _check(state):
    schema = state.get("state_schema")
    if schema == FUSED_STATE_SCHEMA:
        if "staging" in state or "cleanup_claims" in state:
            raise ValueError("fused run state contains legacy staging accounting")
        _check_inventory_segments(state)
        return
    if schema not in {STATE_SCHEMA, INVENTORY_STATE_SCHEMA}:
        raise ValueError("unsupported staging state schema")
    if schema == INVENTORY_STATE_SCHEMA:
        _check_inventory_segments(state)
    staging = state["staging"]
    usable = _bytes(staging["usable_bytes"], "usable_bytes")
    resident = _bytes(staging["resident_bytes"], "resident_bytes")
    exact_objects = []
    for url, item in staging["objects"].items():
        if set(item) != _OBJECT_KEYS | {"resident"} or url != item["url"]:
            raise ValueError("invalid staging object record")
        if not isinstance(item["resident"], bool):
            raise ValueError("invalid staging residency flag")
        exact_objects.append({key: item[key] for key in _OBJECT_KEYS})
    _objects(exact_objects)
    actual = sum(item["size_bytes"] for item in staging["objects"].values()
                 if item["resident"])
    if resident != actual:
        raise ValueError("staging resident byte accounting disagrees with objects")
    for value in staging["ceilings"].values():
        _bytes(value, "producer ceiling")
    for producer, value in staging["claims"].items():
        if staging["ceilings"].get(producer) != value:
            raise ValueError("staging claim disagrees with producer ceiling")
        if producer in staging["settled_publications"]:
            raise ValueError("settled producer retains a claim")
    claimed = sum(_bytes(value, "claim") for value in staging["claims"].values())
    if resident + claimed > usable:
        raise ValueError("staging capacity exceeded")


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

    G3 deployment validation must verify file/directory fsync and flock on the
    actual submit filesystem (including AFS). Unsupported durability operations
    are hard failures; local tests establish no claim about those site semantics.
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


def initialize(root, usable_bytes, planner):
    """Plan one fresh run, creating its permanent lock inode exactly once."""
    root = Path(root)
    _bytes(usable_bytes, "usable_bytes")
    root.mkdir(parents=True, exist_ok=True)
    # An existing staging directory is a conflict, including an incomplete plan.
    (root / "staging").mkdir()
    fd = os.open(root / "staging/ledger.lock", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    if (root / "state.json").exists():
        raise FileExistsError(root / "state.json")
    state = {
        "state_schema": STATE_SCHEMA, "status": "planned",
        "staging": {"usable_bytes": usable_bytes, "resident_bytes": 0,
                    "claims": {}, "ceilings": {}, "objects": {},
                    "settled_publications": {}, "settled_reclaims": {}},
        "planner": deepcopy(planner), "accepted": {}, "allocations": {},
        "cleanup_claims": {}, "merge_group_counts": {},
    }
    _write_state(root, state)
    return state


def initialize_fused(root, planner, pools):
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
    _check(state)
    _write_state(root, state)
    return state


@contextmanager
def transaction(root, *, expected_schema=None):
    """Read, modify and durably replace state under a nonblocking whole-file lock."""
    root = Path(root)
    # No O_CREAT: recovery must never silently replace a missing lock inode.
    timed = control_events.active()
    started = time.perf_counter() if timed else None
    try:
        fused_lock = root / "ledger/state.lock"
        path = fused_lock if fused_lock.is_file() else root / "staging/ledger.lock"
        lock_file = path.open("r+b")
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
            schema = state.get("state_schema")
            if ((path == fused_lock) != (schema == FUSED_STATE_SCHEMA)
                    or (expected_schema is not None and schema != expected_schema)):
                raise ValueError("run state schema does not match its frozen controller/lock")
            _check(state)
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
            _check(state)
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


def reserve_bundle(state, producers):
    """Reserve all new producer ceilings atomically, or return temporary hold 75."""
    _check(state)
    staging = state["staging"]
    additions = {}
    for producer, limit in producers.items():
        if not isinstance(producer, str) or not producer:
            raise ValueError("producer identity is required")
        _bytes(limit, "producer ceiling")
        if producer in staging["ceilings"]:
            if staging["ceilings"][producer] != limit:
                raise ValueError("producer ceiling changed")
        else:
            additions[producer] = limit
    if (staging["resident_bytes"] + sum(staging["claims"].values())
            + sum(additions.values()) > staging["usable_bytes"]):
        return 75
    staging["ceilings"].update(additions)
    staging["claims"].update(additions)
    _check(state)
    return 0


def settle_publication(state, task_id, staged_objects):
    """Convert a producer claim to exact physical resident bytes once."""
    _check(state)
    objects = _objects(staged_objects)
    staging = state["staging"]
    if task_id in staging["settled_publications"]:
        if staging["settled_publications"][task_id] != objects:
            raise ValueError("settled publication changed")
        return
    if task_id not in staging["claims"]:
        raise ValueError("publication lacks a staging claim")
    if sum(item["size_bytes"] for item in objects) > staging["claims"][task_id]:
        raise ValueError("publication exceeds producer ceiling")
    additions = []
    known_ids = {item["artifact_id"]: item["url"]
                 for item in staging["objects"].values()}
    for item in objects:
        if item["artifact_id"] in known_ids and known_ids[item["artifact_id"]] != item["url"]:
            raise ValueError("conflicting staged object identity")
        existing = staging["objects"].get(item["url"])
        if existing is not None:
            identity = {key: existing[key] for key in _OBJECT_KEYS}
            if identity != item:
                raise ValueError("conflicting staged object identity")
            if not existing["resident"]:
                raise ValueError("cannot republish a reclaimed physical object")
        else:
            additions.append(item)
    for item in additions:
        staging["objects"][item["url"]] = dict(item, resident=True)
        staging["resident_bytes"] += item["size_bytes"]
    del staging["claims"][task_id]
    staging["settled_publications"][task_id] = objects
    _check(state)


def _reclaim_results(results, objects):
    dispositions = {}
    by_url = {item["url"]: item for item in objects}
    for item in results:
        if "objects" in item:
            records = item["objects"]
        else:
            records = [item]
        for record in records:
            url = record["url"]
            expected = by_url.get(url)
            if expected is None:
                raise ValueError("reclaim results do not match exact references")
            if "objects" in item:
                identity = item["artifact_id"]
                if record["kind"] == "sidecar":
                    identity += ":sidecar"
                elif record["kind"] != "payload":
                    raise ValueError("invalid reclaim object kind")
            else:
                identity = record["artifact_id"]
            if identity != expected["artifact_id"]:
                raise ValueError("reclaim result identity disagrees with exact reference")
            status = record.get("disposition", record.get("status"))
            if status not in {"removed", "already-missing"}:
                raise ValueError("reclaim did not confirm removal")
            if url in dispositions:
                raise ValueError("duplicate reclaim result")
            dispositions[url] = status
    return dispositions


def settle_reclaim(state, task_id, refs, results):
    """Release exact physical refs confirmed removed/already-missing, once."""
    _check(state)
    objects = _objects(refs)
    dispositions = _reclaim_results(results, objects)
    if set(dispositions) != {item["url"] for item in objects}:
        raise ValueError("reclaim results do not match exact references")
    receipt = {"refs": objects, "results": dispositions}
    staging = state["staging"]
    if task_id in staging["settled_reclaims"]:
        if staging["settled_reclaims"][task_id] != receipt:
            raise ValueError("settled reclaim changed")
        return
    for item in objects:
        existing = staging["objects"].get(item["url"])
        if existing is None or {key: existing[key] for key in _OBJECT_KEYS} != item:
            raise ValueError("reclaim reference is not a known exact object")
    for item in objects:
        existing = staging["objects"][item["url"]]
        if existing["resident"]:
            existing["resident"] = False
            staging["resident_bytes"] -= item["size_bytes"]
    staging["settled_reclaims"][task_id] = receipt
    _check(state)
