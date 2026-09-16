"""Dispatch only explicit physics stages to their pinned runtime adapters."""
from importlib import import_module
from pathlib import Path
from full_mc.runtime.profiles import STAGE_RUNTIME
from full_mc.settings import LOG_LEVELS
from full_mc.storage.transfer import TransferClient, validated_record

_HYDRATE_ALL = {"miniaod", "merge", "ntuple", "selection", "efficiency"}

def _hydrate_inputs(task):
    stage = task.get("stage")
    refs = task.get("inputs", [])
    if stage == "plan":
        selected = [ref for ref in refs if "events" not in ref]
    elif stage in _HYDRATE_ALL:
        selected = list(refs)
    else:
        return
    if not selected:
        return
    client = TransferClient()
    for ref in selected:
        record = validated_record(ref, client)
        if record is None:
            raise ValueError(f"input artifact is not ready: {ref.get('artifact_id', 'unknown')}")
        for key in ("events", "size_bytes", "runtime_release", "count_source", "local_path"):
            if key in record and (key != "local_path" or "local_path" not in ref):
                ref[key] = record[key]
        ref.setdefault("sidecar_url", str(ref["url"]) + ".json")



def execute_task(task, work_dir):
    stage = task.get("stage")
    if stage not in STAGE_RUNTIME:
        raise ValueError(f"unknown worker stage: {stage}")
    if task.get("runtime") != STAGE_RUNTIME[stage]:
        raise ValueError(f"{stage} requires runtime {STAGE_RUNTIME[stage]}")
    if task.get("log_level", "info") not in LOG_LEVELS:
        raise ValueError("invalid worker log level")
    if not task.get("runtime_release"):
        raise ValueError("worker task must pin a runtime release")
    _hydrate_inputs(task)
    module = "analysis" if stage in ("selection", "efficiency") else stage
    return import_module("." + module, __name__).execute(task, Path(work_dir).resolve())
