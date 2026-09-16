"""Durable per-stage recovery, with two branches from the same raw ntuple."""
from __future__ import annotations

from pathlib import Path
from full_mc.io import atomic_json, read_json
from full_mc.storage.transfer import artifact_ready
from full_mc.runtime.profiles import STAGE_RUNTIME

PIPELINE = ("merge", "ntuple", "selection", "efficiency")
PARENT = {"ntuple": "merge", "selection": "ntuple", "efficiency": "ntuple"}


def _identity(task):
    # Ignore worker-local cache paths, but not product identity/counts or settings.
    return {
        "task_id": task["task_id"], "stage": task["stage"],
        "runtime": task["runtime"], "runtime_release": task["runtime_release"],
        "parameters": task.get("parameters", {}),
        "inputs": [{k: value for k, value in ref.items() if k != "local_path"}
                   for ref in task.get("inputs", [])],
        "outputs": task["outputs"],
    }


def _checkpoint_ok(path, stage_task):
    try:
        saved = read_json(path)
    except (FileNotFoundError, ValueError):
        return None
    if saved.get("identity") != _identity(stage_task) or saved.get("status") != "ok":
        return None
    outputs = saved.get("outputs", [])
    expected = {(ref["artifact_id"], ref["url"]) for ref in stage_task["outputs"]}
    actual = {(ref["artifact_id"], ref["url"]) for ref in outputs}
    if not outputs or actual != expected or not all(artifact_ready(ref) for ref in outputs):
        return None
    return saved


def run_combined_task(taskfile, work_dir, log_level=None):
    from full_mc.workers import execute_task

    task = read_json(taskfile)
    if task.get("stage") != "combined" or not task.get("task_id"):
        raise ValueError("combined task with a task_id is required")
    subtasks = task.get("subtasks", {})
    analysis = subtasks.get("merge", {}).get("parameters", {}).get("analysis", "JJP")
    if analysis == "JUP":
        pipeline = ("merge", "ntuple")
    elif analysis == "JJP":
        pipeline = PIPELINE
    else:
        raise ValueError("combined task analysis must be JJP or JUP")
    if set(subtasks) != set(pipeline):
        raise ValueError(f"combined {analysis} task requires {', '.join(pipeline)}")
    if not task.get("runtime_release"):
        raise ValueError("combined task must pin a runtime release")
    root = Path(work_dir).resolve()
    checkpoints = Path(task.get("checkpoint_dir", root / "checkpoints"))
    completed = {}
    statuses = []
    for stage in pipeline:
        current = dict(subtasks[stage])
        current.setdefault("task_id", task["task_id"] + ":" + stage)
        current["stage"] = stage
        current.setdefault("runtime", STAGE_RUNTIME[stage])
        current.setdefault("runtime_release", task["runtime_release"])
        current.setdefault("log_level", task.get("log_level", "info"))
        if log_level:
            current["log_level"] = log_level
        if stage in PARENT:
            current["inputs"] = completed[PARENT[stage]]
        checkpoint = checkpoints / (stage + ".json")
        saved = _checkpoint_ok(checkpoint, current)
        if saved:
            outputs = saved["outputs"]
            state = "resumed"
        else:
            outcome = execute_task(current, root / stage)
            outputs = outcome.get("outputs", [])
            expected = {(ref["artifact_id"], ref["url"]) for ref in current["outputs"]}
            actual = {(ref["artifact_id"], ref["url"]) for ref in outputs}
            if (outcome.get("status") != "ok" or outcome.get("task_id") != current["task_id"]
                    or outcome.get("stage") != stage
                    or outcome.get("runtime_release") != current["runtime_release"]
                    or not outputs or actual != expected
                    or not all(artifact_ready(ref) for ref in outputs)):
                raise RuntimeError(f"{stage} returned unverified outputs")
            atomic_json(checkpoint, {"identity": _identity(current), "status": "ok",
                                     "outputs": outputs})
            state = "completed"
        completed[stage] = outputs
        statuses.append({"stage": stage, "status": state, "outputs": outputs})
    final_outputs = completed["ntuple"] if analysis == "JUP" else (
        completed["selection"] + completed["efficiency"])
    return {"task_id": task["task_id"], "status": "ok", "stages": statuses,
            "outputs": final_outputs}
