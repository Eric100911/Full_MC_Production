"""Optional lookup index for accepted producer receipts.

The index is an auxiliary part of run state. Accepted receipts remain the source
of truth, and every indexed hit is checked against its accepted marker and
receipt before the reference is returned.
"""
import os


SCHEMA = "mcprod-producer-index-v1"
ENVIRONMENT_VARIABLE = "MCPROD_PRODUCER_INDEX"
PRODUCER_STAGES = frozenset(("lhe", "plan"))
_REFERENCE_FIELDS = ("artifact_id", "url", "kind", "size_bytes", "events",
                     "sidecar_url")


def enabled():
    """Return whether callers should use and maintain the optional index."""
    return os.environ.get(ENVIRONMENT_VARIABLE) == "1"


def _is_producer(state, task_id):
    marker = state["accepted"].get(task_id)
    return isinstance(marker, dict) and marker.get("stage") in PRODUCER_STAGES


def _references(outcome):
    if not isinstance(outcome, dict):
        raise ValueError("producer receipt is not a JSON object")
    return list(outcome.get("outputs", [])) + list(outcome.get("originals", []))


def _reference(outcome, artifact_id):
    for ref in _references(outcome):
        if ref.get("artifact_id") == artifact_id:
            return ref
    return None


def _same_reference(left, right):
    return all(left.get(field) == right.get(field) for field in _REFERENCE_FIELDS)


def _valid_index(value):
    return (isinstance(value, dict)
            and value.get("schema") == SCHEMA
            and type(value.get("accepted_count")) is int
            and value["accepted_count"] >= 0
            and isinstance(value.get("artifacts"), dict)
            and all(isinstance(artifact, str) and artifact
                    and isinstance(task_id, str) and task_id
                    for artifact, task_id in value["artifacts"].items()))


def _scan(root, state, receipt_reader):
    """Build canonical maps in task-id order, reading each producer once."""
    artifacts = {}
    references = {}
    consistent = True
    for task_id in sorted(state["accepted"]):
        if not _is_producer(state, task_id):
            continue
        outcome = receipt_reader(root, state, task_id)
        for ref in _references(outcome):
            artifact_id = ref["artifact_id"]
            if artifact_id in artifacts:
                if not _same_reference(references[artifact_id], ref):
                    consistent = False
                continue
            artifacts[artifact_id] = task_id
            references[artifact_id] = ref
    return ({"schema": SCHEMA, "accepted_count": len(state["accepted"]),
             "artifacts": artifacts}, references, consistent)


def _scan_lookup(root, state, artifact_id, receipt_reader):
    for task_id in sorted(state["accepted"]):
        if not _is_producer(state, task_id):
            continue
        outcome = receipt_reader(root, state, task_id)
        ref = _reference(outcome, artifact_id)
        if ref is not None:
            return ref
    raise RuntimeError("input has no accepted producer: " + artifact_id)


def _rebuild_index(root, state, receipt_reader):
    index, references, consistent = _scan(root, state, receipt_reader)
    if consistent:
        state["producer_index"] = index
    else:
        # Duplicate IDs such as generated LHE originals are valid when their
        # consumer-visible metadata agrees. On disagreement, keep the stable
        # sorted scan result but avoid caching an ambiguous mapping.
        state.pop("producer_index", None)
    return index, references, consistent


def rebuild(root, state, receipt_reader):
    """Synchronize the auxiliary index, independent of the environment toggle.

    Returns the index when duplicate references agree. Returns ``None`` and
    removes the cached index when duplicate metadata conflicts; callers can
    then use the deterministic sorted receipt scan.
    """
    index, _, consistent = _rebuild_index(root, state, receipt_reader)
    return index if consistent else None


def _rebuild(root, state, artifact_id, receipt_reader):
    _, references, _ = _rebuild_index(root, state, receipt_reader)
    if artifact_id not in references:
        raise RuntimeError("input has no accepted producer: " + artifact_id)
    return references[artifact_id]


def find(root, state, artifact_id, receipt_reader):
    """Find an accepted producer reference, rebuilding stale indexes safely.

    With the feature disabled this performs the sorted receipt scan and does
    not mutate state. A valid index hit reads and checks only its target receipt.
    """
    if not enabled():
        return _scan_lookup(root, state, artifact_id, receipt_reader)

    index = state.get("producer_index")
    if (_valid_index(index)
            and index["accepted_count"] == len(state["accepted"])):
        task_id = index["artifacts"].get(artifact_id)
        if task_id is not None and _is_producer(state, task_id):
            try:
                outcome = receipt_reader(root, state, task_id)
                ref = _reference(outcome, artifact_id)
            except (OSError, ValueError):
                ref = None
            if ref is not None:
                return ref

    return _rebuild(root, state, artifact_id, receipt_reader)


def _add_outcome(index, task_id, outcome, root, state, receipt_reader):
    artifacts = index["artifacts"]
    seen = {}
    for ref in _references(outcome):
        artifact_id = ref["artifact_id"]
        if artifact_id in seen:
            if not _same_reference(seen[artifact_id], ref):
                return False
            continue
        seen[artifact_id] = ref
        existing_task = artifacts.get(artifact_id)
        if existing_task is None:
            artifacts[artifact_id] = task_id
            continue
        if existing_task == task_id:
            existing_ref = ref
        else:
            existing_outcome = receipt_reader(root, state, existing_task)
            existing_ref = _reference(existing_outcome, artifact_id)
        if existing_ref is None or not _same_reference(existing_ref, ref):
            return False
        if task_id < existing_task:
            artifacts[artifact_id] = task_id
    return True


def record(root, state, task_id, outcome, receipt_reader):
    """Update the auxiliary index after its accepted marker has been added.

    A count mismatch indicates a legacy writer or missed update. Rebuild once
    from the accepted ledger; otherwise update only the new result and count all
    accepted markers, including non-producers. This function never writes a
    receipt or commits run state itself.
    """
    if not enabled():
        return
    if task_id not in state["accepted"]:
        raise ValueError("producer index update requires an accepted task")

    accepted_count = len(state["accepted"])
    index = state.get("producer_index")
    if (_valid_index(index)
            and index["accepted_count"] in (accepted_count - 1, accepted_count)):
        if _is_producer(state, task_id):
            if not _add_outcome(index, task_id, outcome, root, state, receipt_reader):
                state.pop("producer_index", None)
                return
        index["accepted_count"] = accepted_count
        return

    rebuild(root, state, receipt_reader)
