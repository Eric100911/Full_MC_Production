"""One CERN job for MIX and all of its detector-specific MiniAOD products."""
from copy import deepcopy
from functools import partial
import gzip
import hashlib
import json
import logging
import shutil
import subprocess

from full_mc.runtime.profiles import STAGE_RUNTIME
from full_mc.storage import checkpoint
from full_mc.storage.transfer import TransferClient, file_checksum
from . import miniaod, mix
from ._common import expected_runtime, parameters, prepare, result


def _provenance(task, stage, hepmc=None):
    # Only the frozen physics task participates. Bootstrap paths and operator
    # log levels are attempt-local and may change during recovery.
    frozen = {key: stage[key] for key in ("task_id", "stage", "runtime", "inputs", "outputs")}
    frozen["parameters"] = {key: value for key, value in parameters(stage).items()
                            if key != "core_root"}
    identity = hashlib.sha256(json.dumps(frozen, sort_keys=True,
                            separators=(",", ":")).encode()).hexdigest()
    provenance = dict(task_id=stage["task_id"], processing_task_id=task["task_id"],
                      runtime_release=task["runtime_release"], runtime=stage["runtime"],
                      core_revision=task["core_revision"], task_sha256=identity)
    if hepmc is not None:
        provenance["hepmc"] = {key: hepmc[key] for key in
                               ("artifact_id", "events", "size_bytes", "adler32")}
    return provenance


def _stages(task):
    mix_task = task["mix"]
    variants = task["variants"]
    stages = [mix_task, *variants]
    if (mix_task["stage"] != "mix" or not variants
            or any(stage["stage"] != "miniaod" for stage in variants)
            or len({stage["task_id"] for stage in stages}) != len(stages)
            or any(len(stage["outputs"]) != 1 for stage in stages)
            or [stage["outputs"][0] for stage in stages] != task["outputs"]
            or mix_task["inputs"] != task["inputs"]
            or any(stage["runtime_release"] != task["runtime_release"]
                   or stage["runtime"] != expected_runtime(stage) for stage in stages)
            or any(stage["inputs"] != mix_task["outputs"] for stage in variants)):
        raise ValueError("invalid frozen processing manifest")
    return mix_task, variants


def _check_output(task, stage, record, hepmc=None):
    p = parameters(stage)
    checkpoint.validate_metadata(record, stage["outputs"][0],
                                 _provenance(task, stage, hepmc), p["max_output_bytes"])
    expected = p["target_mixed_events"] if hepmc is None else hepmc["events"]
    events = record.get("events")
    if type(events) is not int or not 0 < events <= expected:
        raise ValueError("processing checkpoint event count disagrees with frozen inputs")
    if hepmc is None:
        counts = record.get("source_counts")
        if (not isinstance(counts, list) or len(counts) != len(p["sources"])
                or any(not isinstance(count, dict) or count.get("slot") != index
                       or type(count.get("actual_hepmc_events")) is not int
                       or count["actual_hepmc_events"] < events
                       for index, count in enumerate(counts))):
            raise ValueError("invalid MIX source event counts")
    else:
        if any(record.get(key) != p[key] for key in ("edm_event_id", "global_job_index")):
            raise ValueError("MiniAOD checkpoint differs from the frozen EDM identity")
        if (record.get("count_source") != "edmFileUtil"
                or record.get("expected_events") != expected
                or record.get("partial") is not (events < expected)
                or (events < expected and p["normal_shortfall_policy"] != "report-and-truncate")):
            raise ValueError("invalid MiniAOD event count or provenance")
    return record


def validate_outputs(task, outputs):
    mix_task, variants = _stages(task)
    if not isinstance(outputs, list) or len(outputs) != len(variants) + 1:
        raise ValueError("fused result must account for HepMC and every MiniAOD variant")
    hepmc = _check_output(task, mix_task, outputs[0])
    for stage, output in zip(variants, outputs[1:]):
        _check_output(task, stage, output, hepmc)


def _recover(task, stage, client, scratch, hepmc=None):
    validate = partial(_check_output, task, stage, hepmc=hepmc)
    return checkpoint.recover(client, stage["outputs"][0], validate, scratch)


def _publish(task, stage, client, path, events, metadata, hepmc=None):
    validate = partial(_check_output, task, stage, hepmc=hepmc)
    return checkpoint.publish(client, path, stage["outputs"][0], _provenance(task, stage, hepmc),
                              validate, events=events, metadata=metadata)


def recover_result(task, client, work_dir):
    """Reconstruct a lost result from durable products without running physics."""
    mix_task, variants = _stages(task)
    scratch = None
    if work_dir is not None:
        work_dir.mkdir(parents=True, exist_ok=True)
        scratch = work_dir / "checkpoint.json"
    hepmc = _recover(task, mix_task, client, scratch)
    if hepmc is None:
        raise FileNotFoundError("MIX checkpoint is absent: " + mix_task["task_id"])
    outputs = [hepmc]
    for stage in variants:
        record = _recover(task, stage, client, scratch, hepmc)
        if record is None:
            raise FileNotFoundError("MiniAOD checkpoint is absent: " + stage["task_id"])
        outputs.append(record)
    return result(task, outputs)


def _local_hepmc(client, record, work_dir):
    source = client.copy_in(record["url"], work_dir / "checkpoint.hepmc.gz")
    if source.stat().st_size != record["size_bytes"] or file_checksum(source) != record["adler32"]:
        raise RuntimeError("downloaded HepMC checkpoint differs from its commit")
    path = work_dir / "mixed.hepmc"
    with gzip.open(source, "rb") as incoming, path.open("wb") as outgoing:
        shutil.copyfileobj(incoming, outgoing)
    source.unlink()
    return path


def _clean_stage(path):
    # The bootstrap packages command logs after the worker ends.
    for child in path.iterdir():
        if child.suffix == ".log":
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, STAGE_RUNTIME["processing"])
    mix_task, variants = _stages(task)
    client = TransferClient()
    scratch = work_dir / "checkpoint.json"
    local = None
    hepmc = _recover(task, mix_task, client, scratch)
    if hepmc is None:
        mix_work = work_dir / "mix"
        native = deepcopy(mix_task)
        if "core_root" in parameters(task):
            native["parameters"]["core_root"] = parameters(task)["core_root"]
        path, events, counts = mix.produce(native, mix_work)
        hepmc, used_local = _publish(task, mix_task, client, path, events, {"source_counts": counts})
        if used_local:
            local = work_dir / "mixed.hepmc"
            (mix_work / "mixed.hepmc").replace(local)
        _clean_stage(mix_work)

    outputs = [hepmc]
    failed = []
    for stage in variants:
        stage_work = work_dir / stage["task_id"]
        try:
            record = _recover(task, stage, client, scratch, hepmc)
            if record is None:
                if local is None:
                    local = _local_hepmc(client, hepmc, work_dir)
                native = deepcopy(stage)
                # The native adapter accepts an absolute, checked local input.
                native["inputs"] = [dict(hepmc, local_path=str(local),
                                         size_bytes=local.stat().st_size)]
                native["parameters"]["shared_local_hepmc"] = True
                if "core_root" in parameters(task):
                    native["parameters"]["core_root"] = parameters(task)["core_root"]
                path, events, metadata = miniaod.produce(native, stage_work)
                record, _ = _publish(task, stage, client, path, events, metadata, hepmc)
            outputs.append(record)
        except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as exc:
            logging.error("%s failed: %s", stage["task_id"], exc)
            failed.append(stage["task_id"])
        finally:
            if stage_work.exists():
                _clean_stage(stage_work)
    if local is not None:
        local.unlink()
    if failed:
        failure = result(task, outputs, failed_variants=failed)
        failure["status"] = "failed"
        return failure
    return result(task, outputs)
