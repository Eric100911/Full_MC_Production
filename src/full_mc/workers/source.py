"""Generate or read one LHE source, split it locally and publish its manifest."""
import gzip
from pathlib import Path
import shutil

from full_mc.io import atomic_json, read_json
from full_mc.storage.archive import create_archive
from full_mc.storage.transfer import TransferClient, materialize_input
from . import lhe
from ._common import core_path, parameters, prepare, publish_atomic, result, run, validate_lhe


def build_command(task, work_dir, source, events):
    p = parameters(task)
    command = ["python3", str(core_path(task, "tools/plan_lhe_blocks.py")),
               "--pool-name", str(p["pool_name"]),
               "--primary-seed", str(p["primary_seed"]),
               "--group-id", str(p["group_id"]),
               "--output-dir", str(work_dir),
               "--manifest-output-path", str(work_dir / "plan.json"),
               "--events-per-block", str(p["events_per_block"]),
               "--shuffle-seed", str(p["shuffle_seed"]),
               "--block-output-dir", str(work_dir / "blocks"),
               "--lhe-shuffle-split-bin", str(work_dir / "lhe_shuffle_split"),
               "--log-level", "info" if task.get("log_level") == "debug" else "slim"]
    command += ["--lhe-path", str(source), "--lhe-event-counts", str(events)]
    if int(p.get("max_events_per_plan", 0)) > 0:
        command += ["--max-events-per-plan", str(p["max_events_per_plan"])]
    return command



def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "utilities-el9")
    p = parameters(task)
    if "generation" in task:
        generation = task["generation"]
        generation["parameters"]["core_root"] = str(core_path(task, ""))
        source, events = lhe.generate(generation, work_dir / "helac")
    else:
        ref = task["inputs"][0]
        source = materialize_input(ref, work_dir)
        events = validate_lhe(source)
        if events != ref["events"]:
            raise RuntimeError(f"source LHE events actual={events} expected={ref['events']}: {source}")

    run(task, "utilities-el9", [
        "g++", "-std=c++14", "-O2", "-o", work_dir / "lhe_shuffle_split",
        core_path(task, "lhe_generation/lhe_shuffle_split.cc"),
    ], work_dir)
    run(task, "utilities-el9", build_command(task, work_dir, source, events), work_dir)
    split = read_json(work_dir / "plan.json")
    selected = min(events, p["max_events_per_plan"] or events)
    blocks = split["blocks"]
    if (split["total_input_events"] != events
            or sum(item["n_events"] for item in blocks) != selected):
        raise RuntimeError("source splitter disagrees with the exact LHE event count")

    client = TransferClient()
    outputs = []
    for item in blocks:
        index = item["index"]
        identity = f"BLOCK:{p['pool_name']}:{p['group_id']}:{index:06d}"
        url = p["block_output_dir"] + "/" + item["filename"]
        ref = dict(artifact_id=identity, url=url, kind="lhe-block")
        outputs.append(publish_atomic(client, item["path"], ref, task, events=item["n_events"]))

    # Keep the original archive on IHEP using this job's local file.
    member = work_dir / "000000_source.lhe.gz"
    if source.name.endswith(".lhe.gz"):
        shutil.copyfile(source, member)
    else:
        with source.open("rb") as incoming, gzip.open(member, "wb") as outgoing:
            shutil.copyfileobj(incoming, outgoing)
    archive = create_archive([member], work_dir / "upstream.tar")
    archived = publish_atomic(client, archive, p["archive"], task,
                              metadata={"source_id": p["group_id"],
                                        "source_events": events, "member": member.name})

    manifest = dict(pool=p["pool_name"], source_id=p["group_id"], runtime_release=task["runtime_release"],
                    actual_input_events=events, planned_events=selected, blocks=outputs)
    path = work_dir / "source.json"
    atomic_json(path, manifest)
    publication_id = p.get("publication_id", task["task_id"])
    client.copy_atomic(path, p["manifest_url"], publication_id)
    return result(task, [archived], manifest_url=p["manifest_url"],
                  actual_input_events=events, planned_events=selected, block_count=len(outputs))
