"""Process one bound block or block pair and publish the finished products."""
import shutil

from full_mc.runtime.profiles import STAGE_RUNTIME
from full_mc.storage.transfer import TransferClient
from . import miniaod, mix
from ._common import parameters, prepare, publish_atomic, result


def _discard_payloads(path):
    # The bootstrap collects the native command logs after this worker exits.
    for child in path.iterdir():
        if child.name == "command_logs" or child.suffix == ".log":
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, STAGE_RUNTIME["processing"])
    shared = {key: parameters(task)[key] for key in ("core_root", "publication_id")
              if key in parameters(task)}
    mix_task = task["mix"]
    mix_task["parameters"].update(shared)
    mix_work = work_dir / "mix"
    compressed, events, counts = mix.produce(mix_task, mix_work)
    local_hepmc = work_dir / "mixed.hepmc"
    (mix_work / "mixed.hepmc").replace(local_hepmc)
    local_compressed = work_dir / compressed.name
    compressed.replace(local_compressed)
    _discard_payloads(mix_work)

    outputs = []
    client = TransferClient()
    for variant in task["variants"]:
        ref = variant["outputs"][0]
        variant["parameters"].update(shared, shared_local_hepmc=True)
        variant["inputs"] = [dict(mix_task["outputs"][0], events=events,
                                  local_path=str(local_hepmc),
                                  size_bytes=local_hepmc.stat().st_size)]
        stage_work = work_dir / variant["task_id"]
        path, count, metadata = miniaod.produce(variant, stage_work)
        outputs.append(publish_atomic(client, path, ref, variant, events=count,
                                      metadata=metadata))
        _discard_payloads(stage_work)
    hepmc = publish_atomic(client, local_compressed, mix_task["outputs"][0], mix_task,
                           events=events, metadata={"source_counts": counts})
    return result(task, [hepmc, *outputs])
