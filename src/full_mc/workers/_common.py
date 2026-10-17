"""Worker input, output, runtime and compact result conventions."""
from __future__ import annotations
import gzip
import json
import os
from pathlib import Path
from full_mc.io import atomic_json, read_json
from full_mc.runtime.profiles import STAGE_RUNTIME, resolve_profile, runtime_for_profile
from full_mc.runtime.launch import launch
from full_mc.storage.transfer import TransferClient, artifact_ref
from full_mc.settings import CORE_ROOT

def task_id(task):
    value = task.get("task_id")
    if not isinstance(value, str) or not value:
        raise ValueError("task_id is required")
    return value

def parameters(task):
    return task.get("parameters", {})


def expected_runtime(task):
    stage = task.get("stage")
    profile = parameters(task).get("detector_profile")
    if profile and stage in {"miniaod", "merge", "ntuple"}:
        role = {"miniaod": "simulation", "merge": "merge", "ntuple": "ntuple"}[stage]
        return runtime_for_profile(profile, role)
    return STAGE_RUNTIME.get(stage)

def core_path(task, relative):
    return Path(parameters(task).get("core_root", CORE_ROOT)) / relative

def batch_runtime_asset(task, profile):
    """Find one profile asset in the pinned IHEP release, without changing it."""
    release_dir = Path(task["runtime_dir"]).resolve()
    descriptor = read_json(release_dir / "runtime.json")
    if descriptor.get("runtime_release") != task.get("runtime_release"):
        raise ValueError("batch runtime release does not match the task")
    if core_path(task, "").resolve() != (release_dir / "controller/core").resolve():
        raise ValueError("batch core does not belong to the runtime release")
    if profile not in descriptor.get("profiles", []):
        raise ValueError(f"batch runtime lacks profile {profile}")
    name = descriptor.get("bundles", {}).get(profile)
    if not name or Path(name).name != name:
        raise ValueError(f"batch runtime lacks a valid bundle for {profile}")
    asset = release_dir / name
    recorded_size = descriptor.get("bundle_bytes", {}).get(name)
    if (not asset.is_file() or asset.stat().st_size <= 0 or
            (recorded_size is not None and asset.stat().st_size != int(recorded_size))):
        raise FileNotFoundError(f"batch runtime asset is missing or has the wrong size: {asset}")
    return asset

def prepare(task, work_dir, expected_runtime):
    task_id(task)
    if task.get("runtime") != expected_runtime:
        raise ValueError(f"{task['stage']} requires {expected_runtime}")
    Path(work_dir).mkdir(parents=True, exist_ok=True)
    return Path(work_dir)

def run(task, runtime, command, work_dir, extra_env=None):
    env = {"WORKFLOW_LOG_LEVEL":"info" if task.get("log_level")=="debug" else "slim",
           "PROCESSING_LOG_LEVEL":"info" if task.get("log_level")=="debug" else "slim",
           "FULL_MC_STAGE_RUNTIME":runtime}
    env.update(extra_env or {})
    launch(runtime, list(map(str,command)), Path(work_dir), env=env)

def result(task, outputs, runtime=None, **extra):
    if "staging_claim_bytes" in parameters(task) and "staged_objects" not in extra:
        objects = []
        for ref in outputs:
            objects.append({key: ref[key] for key in
                            ("artifact_id", "url", "size_bytes", "kind")})
            if "sidecar_url" in ref:
                objects.append({"artifact_id": ref["artifact_id"] + ":sidecar",
                                "url": ref["sidecar_url"], "kind": "sidecar",
                                "size_bytes": ref["sidecar_size_bytes"]})
        extra["staged_objects"] = objects
    return dict(task_id=task_id(task), stage=task["stage"], status="ok",
                runtime_release=task["runtime_release"], outputs=outputs, **extra)

def publish(path, task, artifact_id=None, kind=None, events=None, metadata=None):
    path = Path(path)
    declared = task.get("outputs", [])
    output = next((x for x in declared if artifact_id is None or x["artifact_id"] == artifact_id), None)
    if output is None:
        raise ValueError(f"task output not declared: {artifact_id}")
    limit = int(parameters(task).get("max_output_bytes", 0))
    if limit and path.stat().st_size > limit:
        raise RuntimeError(f"{task_id(task)} output exceeds reserved staging bytes")
    client = TransferClient()
    staged = "staging_claim_bytes" in parameters(task)
    if not staged:
        client.copy_out(path, output["url"])
    ref = artifact_ref(path, output["artifact_id"], kind or output["kind"], events, output["url"])
    ref["runtime_release"] = task["runtime_release"]
    for key, value in output.items():
        if key not in {"artifact_id", "url", "kind", "events", "size_bytes"}:
            ref[key] = value
    for key, value in (metadata or {}).items():
        if key in ref or key in ("status", "complete", "task_id", "runtime_release"):
            raise ValueError(f"reserved product metadata field: {key}")
        ref[key] = value
    record = dict(ref, status="ok", complete=True, task_id=task_id(task),
                  runtime_release=task["runtime_release"])
    sidecar = path.with_name(path.name+".json")
    atomic_json(sidecar, record)
    ref["sidecar_url"] = output["url"]+".json"
    if staged:
        ceiling = parameters(task)["staging_claim_bytes"]
        if isinstance(ceiling, bool) or not isinstance(ceiling, int) or ceiling < 0:
            raise ValueError("staging_claim_bytes must be a nonnegative integer")
        ref["sidecar_size_bytes"] = sidecar.stat().st_size
        if path.stat().st_size + ref["sidecar_size_bytes"] > ceiling:
            raise RuntimeError(f"{task_id(task)} output and sidecar exceed reserved staging bytes")
        client.copy_out(path, output["url"])
    client.copy_out(sidecar, ref["sidecar_url"])
    ref["local_path"] = str(path.resolve())
    return ref

def count_lhe_events(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return sum(line.strip().startswith("<event>") or line.strip().startswith("<event ") for line in handle)

def write_json(path, payload):
    atomic_json(path, payload)
    return Path(path)
