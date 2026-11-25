"""Build the one static processing DAG from SOURCE manifests only."""
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from full_mc.domain.inventory import SourceBlock
from full_mc.domain.streams import allocate_campaign, build_streams
from full_mc.io import atomic_json
from full_mc.planning.graph import (PLAN_RECORD, PROC_DAG, PROC_DIR,
                                    log_stem, node_stem, path_text, worker_job_lines)
from full_mc.planning.tasks import processing_tasks
from full_mc.storage.transfer import TransferClient
from ._common import parameters, prepare, result


def _block_inventory(spec, urls, client):
    pools, refs, indices = {}, {}, {}
    if len(spec["sources"]) != len(urls):
        raise ValueError("PLAN requires one manifest per source")
    for index, (source, url) in enumerate(zip(spec["sources"], urls)):
        manifest = client.read_json(url)
        pool, source_id = source["pool"], source["source_id"]
        actual = manifest["actual_input_events"]
        if (manifest["pool"] != pool or manifest["source_id"] != source_id
                or manifest["runtime_release"] != spec["runtime_release"]
                or type(actual) is not int or actual <= 0
                or (source.get("actual_events") is not None and actual != source["actual_events"])):
            raise ValueError(f"SOURCE manifest disagrees with the run specification: {url}")
        selected = min(actual, source.get("planned_events") or actual)
        if selected != manifest["planned_events"]:
            raise ValueError(f"SOURCE manifest selected event count disagrees: {url}")
        stream = pools.setdefault(pool, [])
        blocks = manifest["blocks"]
        width = spec["block_events"]
        if len(blocks) != (selected + width - 1) // width:
            raise ValueError(f"SOURCE manifest block count disagrees: {url}")
        for block_index, ref in enumerate(blocks):
            identity = f"BLOCK:{pool}:{source_id}:{block_index:06d}"
            block_url = f"{url.rsplit('/', 1)[0]}/blocks/block_{source_id}_{block_index:06d}.lhe.gz"
            expected = min(width, selected - block_index * width)
            if (ref["artifact_id"] != identity or ref["url"] != block_url
                    or ref["events"] != expected or ref["kind"] != "lhe-block"):
                raise ValueError(f"SOURCE manifest has an invalid block: {identity}")
            refs[identity] = ref
            indices[identity] = index
            stream.append(SourceBlock(identity, pool, source_id, block_index, expected, block_url))
    return pools, refs, indices


def _write_processing_bundle(bundle, p, pools, refs, indices):
    spec = p["spec"]
    submit_root = Path(p["submit_root"])
    streams = build_streams(spec)
    bundle.mkdir()
    (bundle / "processing.sub").write_text(p["processing_submit"])
    # Keep a submit-side results directory when there are no PROC jobs.
    (bundle / "results").mkdir()
    (bundle / "results/.keep").touch()
    lines = [f"CONFIG {path_text(submit_root / 'dagman.config')}"]
    processing_limit = spec["scheduler"].get("maxjobs", {}).get("processing")
    if processing_limit is not None:
        lines.append(f"MAXJOBS processing {processing_limit}")
    expected, unused = [], {}
    for campaign in spec["campaigns"]:
        name = campaign["name"]
        allocation = allocate_campaign(campaign, pools, streams, spec["block_events"])
        unused[name] = leftovers = allocation["unused_blocks"]
        for pool, identities in leftovers.items():
            if identities:
                logging.warning("%s: skipping %d unmatched or unallocated %s blocks",
                                name, len(identities), pool)
        for job in processing_tasks(spec, campaign, allocation["nodes"], refs, indices):
            lines.extend(worker_job_lines(submit_root, job, spec))
            identity = job["task_id"]
            (bundle / "results" / node_stem(identity)).parent.mkdir(parents=True, exist_ok=True)
            (bundle / "logs" / log_stem(identity)).parent.mkdir(parents=True, exist_ok=True)
            expected.append({"task_id": job["task_id"], "campaign": name,
                             "outputs": job["outputs"]})
    if not expected:
        lines.append(f"JOB EMPTY {path_text(submit_root / 'submit/noop.sub')} NOOP")
    (bundle / PROC_DAG).write_text("\n".join(lines) + "\n")
    atomic_json(bundle / PLAN_RECORD, {
        "processing": expected, "unused_blocks": unused,
        "cleanup": [ref["url"] for ref in refs.values()] + p["source_manifests"],
    })
    return len(expected)


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "utilities-el9")
    p = parameters(task)
    pools, refs, indices = _block_inventory(p["spec"], p["source_manifests"], TransferClient())
    destination = Path(p.get("products_path", work_dir / PROC_DIR))
    with TemporaryDirectory(prefix=".plan-", dir=destination.parent) as temporary:
        bundle = Path(temporary) / PROC_DIR
        job_count = _write_processing_bundle(bundle, p, pools, refs, indices)
        bundle.rename(destination)
    return result(task, [], processing_count=job_count)
