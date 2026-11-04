"""Opt-in, best-effort timing records for submit-host control operations."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import json
import os
import sys
import time


CONTROLLER_BUILD_ID = (
    os.environ.get("MCPROD_CONTROLLER_BUILD", "").strip() or "unversioned"
)
_CURRENT = ContextVar("mcprod_control_event", default=None)


def enabled():
    return os.environ.get("MCPROD_CONTROL_EVENTS", "") == "1"


def active():
    return _CURRENT.get() is not None


def record_duration(phase, seconds):
    event = _CURRENT.get()
    if event is not None:
        phases = event["phases_seconds"]
        phases[phase] = phases.get(phase, 0.0) + seconds


def record_io(name, seconds):
    event = _CURRENT.get()
    if event is None:
        return
    item = event["io"].setdefault(name, {"count": 0, "seconds": 0.0})
    item["count"] += 1
    item["seconds"] += seconds


def record_commit(committed, outcome):
    event = _CURRENT.get()
    if event is not None:
        event["state_committed"] = bool(committed)
        event["commit_outcome"] = outcome


def current_commit_outcome():
    event = _CURRENT.get()
    return event.get("commit_outcome") if event is not None else None


def update_metadata(**values):
    event = _CURRENT.get()
    if event is not None:
        event.update({key: value for key, value in values.items() if value is not None})


def _emit(event):
    try:
        event["duration_seconds"] = time.perf_counter() - event.pop("_started")
        event["timestamp_utc"] = datetime.now(timezone.utc).isoformat(
            timespec="milliseconds")
        payload = json.dumps(event, sort_keys=True, separators=(",", ":"))
        sys.stderr.write(payload + "\n")
        sys.stderr.flush()
    except Exception:
        # Diagnostics must never alter controller or DAGMan exit semantics.
        return


@contextmanager
def operation(name, metadata=None):
    """Collect one controller call and emit it after its transaction has exited."""
    if not enabled():
        yield None
        return
    event = {
        "event": "mcprod_control_operation",
        "schema_version": 1,
        "operation": name,
        "pid": os.getpid(),
        "controller_build": CONTROLLER_BUILD_ID,
        "controller_build_source": (
            "MCPROD_CONTROLLER_BUILD"
            if CONTROLLER_BUILD_ID != "unversioned" else "fallback"
        ),
        "phases_seconds": {},
        "io": {},
        "admission_hint": False,
        "state_committed": False,
        "commit_outcome": "not_attempted",
        "_started": time.perf_counter(),
    }
    if metadata:
        event.update(metadata)
    token = _CURRENT.set(event)
    try:
        yield event
    except BaseException as exc:
        event["outcome"] = "error"
        event["error_type"] = type(exc).__name__
        raise
    finally:
        _CURRENT.reset(token)
        _emit(event)
