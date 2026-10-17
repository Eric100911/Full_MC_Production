"""Deterministic local splitting; remote blocks have one compact task result."""
from __future__ import annotations

import gzip
import shutil
from pathlib import Path

from full_mc.io import read_json
from full_mc.storage.transfer import TransferClient, artifact_ref, materialize_input
from ._common import core_path, parameters, prepare, result, run


def build_command(task, work_dir, inputs):
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
    for path in inputs:
        command += ["--lhe-path", str(path)]
    counts = [int(item["events"]) for item in task["inputs"]]
    if not counts or min(counts) <= 0:
        raise ValueError("planner requires exact positive input counts")
    command += ["--lhe-event-counts", ",".join(map(str, counts))]
    if int(p.get("max_events_per_plan", 0)) > 0:
        command += ["--max-events-per-plan", str(p["max_events_per_plan"])]
    return command



def _original_output(task, inputs, work_dir, deferred_uploads=None):
    """Return the single retained original LHE reference, if requested.

    The splitter accepts multiple source inputs, but an inventory-original is a
    source-level product. Requiring one source makes its event count and
    provenance unambiguous.
    """
    p = parameters(task)
    output = p.get("original_output")
    if output is None:
        return None
    if not isinstance(output, dict):
        raise ValueError("original_output must be an artifact reference")
    if len(inputs) != 1 or len(task["inputs"]) != 1:
        raise ValueError("original_output requires exactly one planner input")
    for key in ("artifact_id", "kind", "url"):
        if not output.get(key):
            raise ValueError("original_output.%s is required" % key)
    source = Path(inputs[0])
    if source.name.endswith(".lhe.gz"):
        retained = source
    elif source.name.endswith(".lhe"):
        retained = Path(work_dir) / "originals" / (source.name + ".gz")
        retained.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as incoming, gzip.open(retained, "wb") as compressed:
            shutil.copyfileobj(incoming, compressed)
    else:
        raise ValueError("original LHE input must end in .lhe or .lhe.gz")
    cap = int(p.get("original_max_bytes", 0))
    if cap and retained.stat().st_size > cap:
        raise RuntimeError("original LHE exceeds reserved staging bytes")
    source_ref = task["inputs"][0]
    url = str(output["url"])
    if url != str(source_ref["url"]):
        if deferred_uploads is None:
            TransferClient().copy_out(retained, url)
        else:
            deferred_uploads.append((retained, url))
    original = artifact_ref(retained, output["artifact_id"], output["kind"],
                            source_ref["events"], url)
    for key in ("generator_profile", "detector_profile_id", "detector_profile"):
        if key in output:
            original[key] = output[key]
    # Fresh LHE generation already published this exact URL and its sidecar.
    # Preserve it for later archive/reclamation without making another sidecar.
    if url == str(source_ref["url"]) and source_ref.get("sidecar_url"):
        original["sidecar_url"] = source_ref["sidecar_url"]
    return original


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "utilities-el9")
    p = parameters(task)
    if int(p["events_per_block"]) <= 0:
        raise ValueError("events_per_block must be positive")
    inputs = [materialize_input(item, work_dir) for item in task["inputs"]]
    run(task, "utilities-el9", [
        "g++", "-std=c++14", "-O2", "-o", work_dir / "lhe_shuffle_split",
        core_path(task, "lhe_generation/lhe_shuffle_split.cc")], work_dir)
    run(task, "utilities-el9", build_command(task, work_dir, inputs), work_dir)
    manifest = read_json(work_dir / "plan.json")
    blocks = manifest["blocks"]
    if not blocks or manifest["total_input_events"] != sum(
            int(item["events"]) for item in task["inputs"]):
        raise RuntimeError("splitter input count disagrees with authoritative inventory")
    sizes = sum(Path(item["path"]).stat().st_size for item in blocks)
    limit = int(p.get("max_output_bytes", 0))
    if limit and sizes > limit:
        raise RuntimeError("split blocks exceed reserved staging bytes")
    staged = "staging_claim_bytes" in p
    uploads = [] if staged else None
    original = _original_output(task, inputs, work_dir, uploads)
    if staged:
        ceiling = p["staging_claim_bytes"]
        if isinstance(ceiling, bool) or not isinstance(ceiling, int) or ceiling < 0:
            raise ValueError("staging_claim_bytes must be a nonnegative integer")
        if sizes + sum(path.stat().st_size for path, _url in uploads) > ceiling:
            raise RuntimeError("planner outputs exceed reserved staging bytes")
    client = TransferClient()
    if staged:
        for path, url in uploads:
            client.copy_out(path, url)
    outputs = []
    for item in blocks:
        url = str(p["block_output_dir"]).rstrip("/") + "/" + item["filename"]
        client.copy_out(item["path"], url)
        identity = f"BLOCK:{p['pool_name']}:{p['group_id']}:{int(item['index']):06d}"
        outputs.append(artifact_ref(item["path"], identity, "lhe-block",
                                    item["n_events"], url))
    extra = {"actual_input_events": manifest["total_input_events"],
             "planned_events": sum(item["events"] for item in outputs)}
    if original is not None:
        extra["originals"] = [original]
    if staged:
        new_refs = outputs + ([original] if uploads else [])
        extra["staged_objects"] = [
            {key: ref[key] for key in ("artifact_id", "url", "size_bytes", "kind")}
            for ref in new_refs]
    return result(task, outputs, **extra)
