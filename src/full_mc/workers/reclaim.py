"""Assemble downstream merge inputs and delete the exact staged LHE objects."""
import logging
from pathlib import Path

from full_mc.application.handoff import make_handoff, merge_snapshot
from full_mc.application.merge_groups import merge_groups, sample_key
from full_mc.io import atomic_json, read_json
from full_mc.planning.graph import RESULTS_BUNDLE, node_stem
from full_mc.planning.tasks import staging_root
from full_mc.runtime.reclaim_bundle import unpack
from full_mc.storage.reclaim import remove_many
from full_mc.storage.layout import canonical_storage_location, storage_location_is_within
from ._common import parameters, prepare, result


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "utilities-el9")
    p = parameters(task)
    spec = p["spec"]
    plan = read_json(p["plan_path"])
    results = Path(p["results_dir"])
    bundle = results.with_name(RESULTS_BUNDLE)
    if bundle.is_file():
        results = unpack(bundle, work_dir / "results", plan)
        logging.info("Unpacked %d PROC results from %s", len(plan["processing"]), bundle.name)
    samples = {}
    for job in plan["processing"]:
        identity = job["task_id"]
        output = read_json(results / f"{node_stem(identity)}.json")
        if (output.get("task_id") != identity or output.get("status") != "ok"
                or output.get("runtime_release") != task["runtime_release"]
                or output.get("stage") != "processing"
                or len(output["outputs"]) != len(job["outputs"])):
            raise ValueError(f"invalid processing result: {identity}")
        for expected, ref in zip(job["outputs"], output["outputs"]):
            if any(ref.get(key) != value for key, value in expected.items()):
                raise ValueError(f"processing output disagrees with PLAN: {identity}")
            if ref["kind"] == "miniaod":
                if (ref.get("count_source") != "edmFileUtil"
                        or ref.get("events", 0) <= 0 or ref.get("size_bytes", 0) <= 0):
                    raise ValueError(f"invalid MiniAOD metadata: {identity}")
                key = sample_key(job["campaign"], ref["detector_profile_id"])
                samples.setdefault(key, []).append(ref)

    groups = merge_groups(spec, samples)
    handoff = make_handoff(spec, groups)
    if handoff["rejected"]:
        missing = ", ".join(group["task_id"] for group in handoff["rejected"])
        raise RuntimeError(f"cannot verify EOS MiniAODs for merge groups: {missing}")
    targets = plan["cleanup"]
    prefix = staging_root(spec) + "/sources/"
    location = canonical_storage_location(prefix)
    for target in targets:
        if (not target.startswith(prefix) or not target.endswith((".lhe.gz", "/source.json"))
                or not storage_location_is_within(canonical_storage_location(target), location)):
            raise ValueError(f"refusing to reclaim outside run source staging: {target}")
    reclaimed = remove_many(targets, work_dir=work_dir)
    output = Path(p.get("products_path", work_dir / "handoff.json"))
    atomic_json(output.with_name("merge_groups.json"), merge_snapshot(spec, groups))
    atomic_json(output, handoff)
    return result(task, [], reclaimed_count=len(reclaimed), merge_groups=len(handoff["tasks"]))
