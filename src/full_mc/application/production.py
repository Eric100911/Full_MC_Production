"""Freeze one SOURCE → PLAN → PROC_SUBDAG → RECLAIM run."""
from copy import deepcopy
import logging
from pathlib import Path

from full_mc.application.result_validation import validate_source_inventory
from full_mc.credentials import ensure_valid_proxy, resolve_proxy_path
from full_mc.domain.identity import shuffle_seed
from full_mc.io import atomic_json
from full_mc.planning.graph import (CATEGORIES, log_stem, node_stem, path_text, root_dag,
                                    stage_requires_proxy, token, worker_submit, write_dagman_config)
from full_mc.planning.tasks import artifact_reference, staging_root, task_manifest
from full_mc.runtime.packages import validate_runtime_release
from full_mc.runtime.profiles import STAGE_RUNTIME, runtime_for_profile
from full_mc.settings import STATIC_WORKFLOW, load_run_spec

# Capacity estimates only, in bytes. These do not cap individual products.
ESTIMATED_HEPMC_BYTES = 64_000_000
ESTIMATED_MINIAOD_BYTES = 2_000_000_000


def _source_task(spec, index, source):
    identity = f"SOURCE_{index:06d}"
    pool, source_id = token(source["pool"]), token(source["source_id"])
    base = f"{staging_root(spec)}/sources/{pool}/{source_id}"
    archive_id = f"SOURCE_ARCHIVE_{index:06d}"
    archive_url = f"{spec['storage']['ihep_archive'].rstrip('/')}/{spec['run_id']}/{archive_id}.tar"
    archive = artifact_reference(archive_id, archive_url, "upstream-archive",
                                 sidecar_url=archive_url + ".json")
    limits = spec["limits"]
    scratch = max(4_000_000_000, 4 * (limits["source_lhe_bytes"] + limits["source_block_bytes"]))
    generation = source.get("resolved_generator_profile")
    inputs = [] if generation else [artifact_reference(
        f"INPUT_{index:06d}", source["url"], "lhe", events=source["actual_events"],
        **({"size_bytes": source["size_bytes"]} if source["size_bytes"] else {}))]
    task = task_manifest(spec, identity, "source", inputs, [archive], {
        "pool_name": pool, "group_id": source_id, "primary_seed": source["seed"],
        "events_per_block": spec["block_events"], "shuffle_seed": shuffle_seed(source["seed"]),
        "max_events_per_plan": source.get("planned_events") or 0,
        "block_output_dir": base + "/blocks", "manifest_url": base + "/source.json",
        "archive": archive, "scratch_bytes": scratch,
    })
    if generation:
        task["generation"] = task_manifest(spec, f"LHE_{index:06d}", "lhe", [], [], {
            "generator_profile": generation, "pool_name": pool, "seed": source["seed"],
            "retain_helac_diagnostics": source.get("retain_helac_diagnostics", False),
        })
    return task


def plan_run(spec_path, output_dir, log_level=None):
    """Write only submit-side records for a new run."""
    spec = deepcopy(spec_path) if isinstance(spec_path, dict) else load_run_spec(spec_path)
    if log_level:
        spec["log_level"] = log_level
    validate_source_inventory(spec)
    processing = spec["resources"]["processing"]
    recommended = max(10 * ESTIMATED_HEPMC_BYTES,
                      2 * ESTIMATED_HEPMC_BYTES + 4 * ESTIMATED_MINIAOD_BYTES)
    if processing.setdefault("disk_bytes", recommended) < recommended:
        logging.warning("processing disk request %s bytes is below the %s-byte estimate",
                        processing["disk_bytes"], recommended)
    runtime_dir = spec.get("runtime_dir")
    if not runtime_dir:
        raise ValueError("runtime_dir must identify a prepared CERN runtime")
    runtime_dir = Path(runtime_dir)
    generated = any(source.get("generator_profile") for source in spec["sources"])
    runtime = validate_runtime_release(runtime_dir, expected_release=spec["runtime_release"],
                                       expected_site="cern", check_helac=generated)
    if runtime.get("cern_workflow") != STATIC_WORKFLOW:
        raise ValueError("prepare a new CERN runtime for static_subdag")
    if spec.get("core_revision") and spec["core_revision"] != runtime["git_revision"]:
        raise ValueError("supplied core_revision does not match the prepared runtime")
    processing_profiles = {STAGE_RUNTIME["mix"]} | {
        runtime_for_profile(profile, "simulation")
        for campaign in spec["campaigns"] for profile in campaign["resolved_detector_variants"]
    }
    source_profiles = {STAGE_RUNTIME["lhe"]} if generated else set()
    required = {STAGE_RUNTIME[stage] for stage in CATEGORIES} | source_profiles | processing_profiles
    if not required <= set(runtime["profiles"]):
        raise ValueError("runtime release lacks required worker profiles")
    if generated and STAGE_RUNTIME["lhe"] not in runtime["bundles"]:
        raise ValueError("runtime release lacks the HELAC bundle")
    core = (runtime_dir / "controller/core").resolve()
    spec.update(core_revision=runtime["git_revision"], core_root=str(core),
                runtime_source_dirty=bool(runtime.get("source_dirty", False)))

    root = Path(output_dir).resolve()
    if root.exists() and any(root.iterdir()):
        raise FileExistsError("run directory is not empty")
    proxy = None
    if any(stage_requires_proxy(spec, stage) for stage in CATEGORIES):
        proxy = Path(resolve_proxy_path()).resolve()
        ensure_valid_proxy(proxy)
    path_text(root)
    submits = {}
    for stage in CATEGORIES:
        profiles = processing_profiles if stage == "processing" else source_profiles if stage == "source" else ()
        stage_proxy = proxy if stage_requires_proxy(spec, stage) else None
        if stage_proxy is None:
            logging.info("Skipping X.509 proxy check for local stage %s", stage)
        submits[stage] = worker_submit(root, core, runtime_dir, runtime, stage_proxy, stage, profiles)
    sources = [_source_task(spec, index, source) for index, source in enumerate(spec["sources"])]
    plan = task_manifest(spec, "PLAN", "plan", [], [], {
        "spec": spec, "submit_root": str(root), "processing_submit": submits.pop("processing"),
        "source_manifests": [task["parameters"]["manifest_url"] for task in sources],
    })
    reclaim = task_manifest(spec, "RECLAIM", "reclaim", [], [], {"spec": spec})
    dag = root_dag(root, spec, sources, plan, reclaim)

    root.mkdir(parents=True, exist_ok=True)
    atomic_json(root / "run.json", spec)
    (root / "submit").mkdir()
    write_dagman_config(root, spec["scheduler"])
    for stage, submit in submits.items():
        (root / "submit" / f"{stage}.sub").write_text(submit)
    (root / "submit/noop.sub").write_text("universe = local\nexecutable = /bin/true\nqueue\n")
    for task in (*sources, plan, reclaim):
        stem = node_stem(task["task_id"])
        atomic_json(root / "tasks" / f"{stem}.json", task)
        (root / "results" / stem).parent.mkdir(parents=True, exist_ok=True)
        (root / "logs" / log_stem(task["task_id"])).parent.mkdir(parents=True, exist_ok=True)
    path = root / "run.dag"
    path.write_text(dag)
    return {"run_id": spec["run_id"], "run_dir": str(root), "dag": str(path),
            "runtime_release": spec["runtime_release"], "source_files": len(sources),
            "max_processing_nodes": sum(c["max_processing_nodes"] for c in spec["campaigns"]),
            "submitted": False}
