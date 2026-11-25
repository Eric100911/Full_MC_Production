"""Handoff payload shared by CERN RECLAIM and the IHEP batch."""
from copy import deepcopy
from pathlib import Path

from full_mc.io import atomic_json, read_json
from full_mc.storage.transfer import TransferClient, validated_record

HANDOFF_SCHEMA = "full-mc-handoff-v1"
REF_KEYS = ("artifact_id", "url", "kind", "events", "size_bytes",
            "sidecar_url", "runtime_release", "count_source",
            "global_job_index", "edm_event_id",
            "generator_profile",
            "detector_profile_id", "detector_profile", "mc_global_tag", "era",
            "simulation_runtime", "merge_runtime", "ntuple_runtime")


def _compact_ref(ref):
    return {key: ref[key] for key in REF_KEYS if key in ref}


def merge_snapshot(spec, groups):
    snapshot = {key: spec[key] for key in ("run_id", "runtime_release", "core_revision")}
    snapshot["groups"] = groups
    if "analysis_output" in spec:
        snapshot["analysis_output"] = spec["analysis_output"]
    return snapshot


def make_handoff(spec, groups, *, client=None):
    client = client or TransferClient()
    accepted, rejected, seen = [], [], set()
    for group in groups:
        identity = group["task_id"]
        if identity in seen or group["stage"] != "combined":
            raise ValueError(f"invalid or duplicate merge group: {identity}")
        seen.add(identity)
        inputs = group["subtasks"]["merge"]["inputs"]
        if not inputs or len({ref["artifact_id"] for ref in inputs}) != len(inputs):
            raise ValueError(f"{identity}: missing or duplicate component MiniAODs")
        resolved = []
        for ref in inputs:
            metadata = validated_record(ref, client)
            if (not metadata or metadata.get("kind") != "miniaod"
                    or int(metadata.get("events", 0)) <= 0
                    or metadata.get("count_source") != "edmFileUtil"
                    or any(key in ref and ref[key] != metadata.get(key) for key in
                           ("global_job_index", "edm_event_id", "detector_profile_id"))):
                break
            sidecar_url = ref.get("sidecar_url", ref["url"] + ".json")
            resolved.append(_compact_ref(dict(metadata, sidecar_url=sidecar_url)))
        if len(resolved) != len(inputs):
            rejected.append({"task_id": group["task_id"], "reason": "component MiniAODs not ready"})
            continue
        task = deepcopy(group)
        task["subtasks"]["merge"]["inputs"] = resolved
        for subtask in task["subtasks"].values():
            for key in ("core_root", "analyzer_path", "result_path"):
                subtask["parameters"].pop(key, None)
            subtask["outputs"] = [_compact_ref(ref) for ref in subtask["outputs"]]
        accepted.append(task)
    payload = {"schema": HANDOFF_SCHEMA, "run_id": spec["run_id"],
               "runtime_release": spec["runtime_release"], "core_revision": spec["core_revision"],
               "tasks": accepted, "rejected": rejected}
    if "analysis_output" in spec:
        payload["analysis_output"] = spec["analysis_output"]
    return payload


def export_handoff(run_dir, ready_only=True):
    root = Path(run_dir).resolve()
    spec = read_json(root / "run.json")
    completion = read_json(root / "results/RECLAIM.json")
    snapshot = read_json(root / "merge_groups.json")
    if (completion.get("status") != "ok"
            or any(snapshot.get(key) != spec[key] for key in ("run_id", "runtime_release", "core_revision"))):
        raise RuntimeError("handoff requires a successful RECLAIM")
    payload = make_handoff(spec, snapshot["groups"])
    if payload["rejected"] and not ready_only:
        raise ValueError("some requested merge groups are not ready")
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
