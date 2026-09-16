"""Explicit ready-group handoffs, not recursive scans of arbitrary JSON files."""
from copy import deepcopy
from pathlib import Path

from full_mc.io import atomic_json, read_json
from full_mc.storage.transfer import TransferClient, artifact_ready

HANDOFF_SCHEMA = "full-mc-handoff-v1"
REF_KEYS = ("artifact_id", "url", "kind", "events", "size_bytes",
            "sidecar_url", "runtime_release", "count_source")


def _compact_ref(ref):
    return {key: ref[key] for key in REF_KEYS if key in ref}


def export_handoff(run_dir, ready_only=True):
    """Export future IHEP jobs whose component MiniAOD inputs are ready.

    merge_groups.json is a planned index written by the CERN coordinator. Its
    groups are combined task specifications, not successful merge outputs.
    """
    root = Path(run_dir).resolve()
    index = read_json(root / "merge_groups.json")
    if not index.get("run_id") or not index.get("runtime_release"):
        raise ValueError("merge group index must identify the run and runtime release")
    client = TransferClient()
    accepted, rejected, seen = [], [], set()
    for group in index["groups"]:
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
        task["runtime_release"] = index["runtime_release"]
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
    payload = {"schema": HANDOFF_SCHEMA, "run_id": index["run_id"],
               "runtime_release": index["runtime_release"],
               "core_revision": index.get("core_revision"), "tasks": accepted,
               "rejected": rejected}
    if "analysis_output" in index:
        payload["analysis_output"] = index["analysis_output"]
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
