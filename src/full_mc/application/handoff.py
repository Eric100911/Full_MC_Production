"""Explicit ready-group handoffs, not recursive scans of arbitrary JSON files."""
from copy import deepcopy
from pathlib import Path

from full_mc.application.ledger import STATE_SCHEMA, load_groups
from full_mc.io import atomic_json, read_json
from full_mc.storage.transfer import TransferClient, artifact_ready

HANDOFF_SCHEMA = "full-mc-handoff-v1"
REF_KEYS = ("artifact_id", "url", "kind", "events", "size_bytes",
            "sidecar_url", "runtime_release", "count_source",
            "global_job_index", "edm_event_id",
            "generator_profile",
            "detector_profile_id", "detector_profile", "mc_global_tag", "era",
            "simulation_runtime", "merge_runtime", "ntuple_runtime")


def _compact_ref(ref):
    return {key: ref[key] for key in REF_KEYS if key in ref}


def _committed_groups(root, spec, state):
    """Completed merge groups the state has accepted, in sample-stream order.

    ``merge_groups.json`` is a derived operator snapshot, so it is not a source
    of truth: a group file at or above the committed counter is a group the run
    has not accepted yet and must never be exported.
    """
    return load_groups(root, spec, state.get("merge_group_counts", {}))


def export_handoff(run_dir, ready_only=True):
    """Export future IHEP jobs whose component MiniAOD inputs are ready.

    Handoff reads one immutable ledger file per committed merge group. Those
    files are combined task specifications, not successful merge outputs, and
    ``merge_groups.json`` is refreshed here as a derived operator snapshot.
    """
    root = Path(run_dir).resolve()
    spec = read_json(root / "run.json")
    state = read_json(root / "state.json")
    # Fail closed like the seam does.  A pre-refactor state has no
    # merge_group_counts, so exporting one would silently yield zero tasks and
    # overwrite the operator's merge_groups.json snapshot with an empty index.
    if state.get("state_schema") not in (STATE_SCHEMA, "mcprod-shard-state-v1"):
        raise RuntimeError("handoff export requires a " + STATE_SCHEMA + " run; "
                           "existing runs keep their frozen runtime and are not migrated")
    if (state.get("state_schema") == "mcprod-shard-state-v1"
            and state.get("status") != "complete"):
        raise RuntimeError("campaign-shard handoff requires a completed run")
    run_id, runtime_release = spec.get("run_id"), spec.get("runtime_release")
    if not run_id or not runtime_release:
        raise ValueError("merge group index must identify the run and runtime release")
    groups = _committed_groups(root, spec, state)
    snapshot = {"run_id": run_id, "runtime_release": runtime_release,
                "core_revision": spec.get("core_revision"), "groups": groups}
    if "analysis_output" in spec:
        snapshot["analysis_output"] = spec["analysis_output"]
    atomic_json(root / "merge_groups.json", snapshot)
    client = TransferClient()
    accepted, rejected, seen = [], [], set()
    for group in groups:
        identity = group["task_id"]
        if identity in seen:
            raise ValueError(f"duplicate merge group: {identity}")
        seen.add(identity)
        if group.get("stage") != "combined":
            raise ValueError("handoff groups must be combined task specifications")
        inputs = group["subtasks"]["merge"]["inputs"]
        if not inputs or len({ref["artifact_id"] for ref in inputs}) != len(inputs):
            raise ValueError(f"{identity}: missing or duplicate component MiniAODs")
        ready, resolved = True, []
        for ref in inputs:
            if ref.get("kind") != "miniaod" or not artifact_ready(ref, client):
                ready = False
                break
            metadata = client.read_json(ref.get("sidecar_url", ref["url"] + ".json"))
            if int(metadata.get("events", 0)) <= 0 or metadata.get("count_source") != "edmFileUtil":
                ready = False
                break
            resolved.append(_compact_ref(dict(metadata, sidecar_url=ref.get(
                "sidecar_url", ref["url"] + ".json"))))
        if not ready:
            rejected.append({"task_id": identity, "reason": "component MiniAODs not ready"})
            continue
        task = deepcopy(group)
        task["runtime_release"] = runtime_release
        task["subtasks"]["merge"]["inputs"] = resolved
        for subtask in task["subtasks"].values():
            p = subtask.get("parameters", {})
            # Site-local paths are resolved again by the receiving checkout.
            for key in ("core_root", "analyzer_path", "result_path"):
                p.pop(key, None)
            subtask["outputs"] = [_compact_ref(ref) for ref in subtask["outputs"]]
        accepted.append(task)
    if rejected and not ready_only:
        raise ValueError("some requested merge groups are not ready")
    payload = {"schema": HANDOFF_SCHEMA, "run_id": run_id,
               "runtime_release": runtime_release,
               "core_revision": spec.get("core_revision"), "tasks": accepted,
               "rejected": rejected}
    if "analysis_output" in spec:
        payload["analysis_output"] = spec["analysis_output"]
    atomic_json(root / "handoff.json", payload)
    return payload


def transfer_handoff(handoff_path, site_config):
    source = Path(handoff_path).resolve()
    payload = read_json(source)
    if payload.get("schema") != HANDOFF_SCHEMA:
        raise ValueError("unsupported handoff schema")
    destination = str(site_config.get("destination", "")).rstrip("/")
    if not destination:
        raise ValueError("site_config.destination is required")
    target = destination + "/" + source.name
    TransferClient().copy_out(source, target)
    return {"site": site_config.get("name", "ihep"), "handoff": target,
            "task_count": len(payload["tasks"]), "status": "transferred"}
