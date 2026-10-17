"""Authoritative, locked byte accounting for run-owned staging objects.

This module performs local control-record I/O only. Remote publication and
removal happen in workers, before their exact results are accepted here.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import fcntl
import json
import os
from pathlib import Path
import tempfile

STATE_SCHEMA = "mcprod-shard-state-v1"
_OBJECT_KEYS = {"artifact_id", "url", "size_bytes", "kind"}


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
    if state.get("state_schema") != STATE_SCHEMA:
        raise ValueError("unsupported staging state schema")
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


def _write_state(root, state):
    """Replace state only after the complete temporary record reaches disk.

    G3 deployment validation must verify file/directory fsync and flock on the
    actual submit filesystem (including AFS). Unsupported durability operations
    are hard failures; local tests establish no claim about those site semantics.
    A directory fsync failure occurs after replace, so the new state may already
    be visible and must be reread on retry rather than assumed rolled back.
    """
    fd, name = tempfile.mkstemp(prefix=".state-", suffix=".json", dir=root)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, Path(root) / "state.json")
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


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


@contextmanager
def transaction(root):
    """Read, modify and durably replace state under a nonblocking whole-file lock."""
    root = Path(root)
    # No O_CREAT: recovery must never silently replace a missing lock inode.
    with (root / "staging/ledger.lock").open("r+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            state = json.loads((root / "state.json").read_text(encoding="utf-8"))
            _check(state)
            yield state
            _check(state)
            _write_state(root, state)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


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
