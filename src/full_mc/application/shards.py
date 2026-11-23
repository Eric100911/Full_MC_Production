"""Local, transaction-bound orchestration of independent campaign shards.

DAGMan remains the execution authority. This controller accepts immutable worker
results and allocates finite inputs using durable local transactions.
No function in the scheduling seams contacts remote storage.
"""
import time
from collections import defaultdict
from copy import deepcopy
from functools import wraps
from pathlib import Path

from full_mc.application import admission_hints, control_events, producer_index, staging
from full_mc.application.merge_groups import merge_groups
from full_mc.application.result_validation import (
    validate_archive_result, validate_planner_result, validate_reclaim_result,
    validate_result_envelope, validate_source_inventory,
)
from full_mc.application.ledger import load_groups, sample_key, write_group
from full_mc.credentials import resolve_proxy_path
from full_mc.planning.tasks import artifact_reference, reclaim_references, staging_root, task_manifest
from full_mc.storage.archive import restore_members
from full_mc.domain.identity import shuffle_seed
from full_mc.domain.inventory import SourceBlock
from full_mc.domain.streams import allocate_shard, build_streams, next_matching, stream_key
from full_mc.io import atomic_json, read_json, write_immutable_json
from full_mc.local_paths import LAYOUT_MARKER, record_path
from full_mc.planning.graph import token, write_submit_files
from full_mc.planning.script_defer import (controller_render_override,
                                           resolve_script_defer)
from full_mc.planning.shards import (child_path, cleanup_name, shard_name,
                                     shard_ranges, source_name, write_child_dag,
                                     write_root_dag)
from full_mc.runtime.packages import validate_runtime_release
from full_mc.runtime.profiles import STAGE_RUNTIME, runtime_for_profile
from full_mc.settings import load_run_spec
from full_mc.workers.processing import validate_outputs as validate_processing_outputs

def _entry(function):
    def argument(args, kwargs, name, position):
        if name in kwargs:
            return kwargs[name]
        return args[position] if len(args) > position else None

    @wraps(function)
    def call(*args, **kwargs):
        try:
            if not control_events.enabled():
                try:
                    return function(*args, **kwargs)
                except staging.Deferred as exc:
                    return _answer(exc.code, reason=exc.reason)
                except staging.LockBusy:
                    return {"exit_code": 75, "reason": "local state lock busy"}
            run_dir = kwargs.get("run_dir", args[0] if args else None)
            task_id = kwargs.get("task_id")
            if task_id is None:
                if function.__name__ in {"prepare_source_product", "accept_result",
                                         "recover_archive_orphan"}:
                    task_id = argument(args, kwargs, "task_id", 1)
                elif function.__name__ == "prepare_source":
                    index = argument(args, kwargs, "source_index", 1)
                    task_id = source_name(int(index)) if index is not None else None
                elif function.__name__ == "prepare_shard":
                    campaign = argument(args, kwargs, "campaign", 1)
                    shard_index = argument(args, kwargs, "shard_index", 2)
                    task_id = (shard_name(campaign, int(shard_index))
                               if campaign is not None and shard_index is not None
                               else None)
                elif function.__name__ == "complete_source":
                    index = argument(args, kwargs, "source_index", 1)
                    task_id = source_name(int(index)) if index is not None else None
                elif function.__name__ == "complete_shard":
                    campaign = argument(args, kwargs, "campaign", 1)
                    shard_index = argument(args, kwargs, "shard_index", 2)
                    task_id = (shard_name(campaign, int(shard_index))
                               if campaign is not None and shard_index is not None
                               else None)
                elif function.__name__ == "prepare_cleanup":
                    pool = argument(args, kwargs, "pool", 1)
                    slot = argument(args, kwargs, "slot", 2)
                    task_id = (cleanup_name(pool, int(slot))
                               if pool is not None and slot is not None else None)
                elif len(args) > 1:
                    task_id = ":".join(str(value) for value in args[1:])
            metadata = {"run_dir": str(run_dir) if run_dir is not None else None,
                        "task": str(task_id) if task_id is not None else None}
            with control_events.operation(function.__name__, metadata) as event:
                try:
                    result = function(*args, **kwargs)
                except staging.Deferred as exc:
                    result = _answer(exc.code, reason=exc.reason)
                except staging.LockBusy:
                    result = {"exit_code": 75, "reason": "local state lock busy"}
                if event is not None:
                    event["outcome"] = str(
                        result.get("status", result.get("exit_code", "ok")))
                    if "exit_code" in result:
                        event["exit_code"] = result["exit_code"]
                    if "reason" in result:
                        event["reason"] = result["reason"]
                    event["admission_hint"] = bool(result.get("admission_hint", False))
                return result
        except staging.LockBusy:
            return {"exit_code": 75, "reason": "local state lock busy"}
    return call


def _answer(code=0, **extra):
    return dict(exit_code=code, **extra)


def _paths(run_dir):
    root = Path(run_dir).resolve()
    spec = read_json(root / "run.json")
    staging.require_schema(spec)
    control_events.update_metadata(run_id=spec.get("run_id"),
                                  runtime_release=spec.get("runtime_release"))
    return root, spec


def _manifest_path(root, name):
    return record_path(root, "ledger/shards", token(name))


def _receipt_path(root, task_id):
    return record_path(root, "ledger/accepted", token(task_id))


def _receipt(root, state, task_id):
    if task_id not in state["accepted"]:
        raise RuntimeError("task has no accepted result: " + task_id)
    timed = control_events.active()
    started = time.perf_counter() if timed else None
    try:
        return read_json(_receipt_path(root, task_id))
    finally:
        if timed:
            control_events.record_io("receipt_read", time.perf_counter() - started)


def _render(root, name, spec, *, allow_plan_accept_post_override=False):
    path = child_path(root, name)
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"existing child DAG is not a regular file: {path}")
        return _answer(dag=str(path), status="prepared", preserved_existing=True)
    manifest = read_json(_manifest_path(root, name))
    render_spec = deepcopy(spec)
    if allow_plan_accept_post_override:
        override = controller_render_override(root)
        if override is not None:
            scheduler = render_spec.setdefault("scheduler", {})
            defer = resolve_script_defer(scheduler)
            script_defer = scheduler.setdefault("script_defer", {})
            script_defer["default_seconds"] = defer["default_seconds"]
            script_defer["plan_accept_post_seconds"] = override
    path = write_child_dag(root, name, manifest["tasks"], manifest["dependencies"],
                           render_spec, manifest.get("pre_tasks", ()))
    return _answer(dag=str(path), status="prepared")


def _freeze(root, name, descriptor):
    write_immutable_json(_manifest_path(root, name), descriptor)


def _processing_scratch(limits):
    hepmc = limits["mixed_hepmc_bytes"]
    return max(10 * hepmc, 2 * hepmc + 4 * limits["miniaod_bytes"])


def plan_run(spec_path, output_dir, log_level=None):
    """Freeze a new campaign_shards run without submitting or remote I/O."""
    spec = deepcopy(spec_path) if isinstance(spec_path, dict) else load_run_spec(spec_path)
    if log_level:
        spec["log_level"] = log_level
    if spec.get("orchestration") != "campaign_shards":
        raise ValueError("run spec must explicitly declare orchestration: campaign_shards")
    validate_source_inventory(spec)
    limits = spec.setdefault("limits", {})
    limits.setdefault("miniaod_bytes", 2_000_000_000)
    for key in ("source_lhe_bytes", "source_block_bytes", "mixed_hepmc_bytes", "miniaod_bytes"):
        if type(limits.get(key)) is not int or limits[key] <= 0:
            raise ValueError(f"limits.{key} must be a positive output byte ceiling")
    scratch = _processing_scratch(limits)
    disk = spec.get("resources", {}).get("processing", {}).get("disk_bytes", scratch)
    if disk < scratch:
        raise ValueError("processing scratch exceeds its requested disk budget")
    limits.setdefault("shard_nodes", 10)
    limits.setdefault("target_window_nodes", 20)
    if limits["target_window_nodes"] <= 0:
        raise ValueError("target_window_nodes must be positive")
    ranges = shard_ranges(spec)
    streams = build_streams(spec)
    for source in spec["sources"]:
        token(source["pool"])
        token(source["source_id"])
        if int(source.get("seed", 0)) <= 0:
            raise ValueError("each source requires its explicit seed")
    if any(s["pool"] not in {v["pool"] for v in spec["sources"]}
           for c in spec["campaigns"] for s in c["sources"]):
        raise ValueError("campaign uses an undeclared source pool")
    runtime_dir = spec.get("runtime_dir")
    if not runtime_dir:
        raise ValueError("runtime_dir must identify a prepared reusable CERN runtime")
    runtime = validate_runtime_release(runtime_dir, expected_release=spec["runtime_release"],
                                       expected_site="cern")
    if spec.get("core_revision") and spec["core_revision"] != runtime["git_revision"]:
        raise ValueError("supplied core_revision does not match the prepared runtime")
    if runtime.get("cern_state_schema") != staging.FUSED_STATE_SCHEMA:
        raise ValueError("prepare a new CERN runtime for fused campaign shards")
    spec["state_schema"] = staging.FUSED_STATE_SCHEMA
    required = {STAGE_RUNTIME[s] for s in ("plan", "mix", "archive", "reclaim", "processing")}
    simulations = {runtime_for_profile(p, "simulation") for c in spec["campaigns"]
                   for p in c["resolved_detector_variants"]}
    required.update(simulations)
    if any(s.get("generator_profile") for s in spec["sources"]):
        required.add(STAGE_RUNTIME["lhe"])
    if not required <= set(runtime["profiles"]):
        raise ValueError("runtime release lacks required worker profiles")
    controller = Path(runtime_dir) / "controller/core"
    if not (controller / "mcprod").is_file():
        raise ValueError("runtime release lacks its frozen submit-host controller")
    spec.update(core_revision=runtime["git_revision"], core_root=str(controller.resolve()),
                runtime_source_dirty=bool(runtime.get("source_dirty", False)))
    proxy_path = resolve_proxy_path()
    root = Path(output_dir).resolve()
    if root.exists() and any(root.iterdir()):
        raise FileExistsError("run directory is not empty; use existing run for recovery")
    root.mkdir(parents=True, exist_ok=True)
    (root / LAYOUT_MARKER).touch()
    atomic_json(root / "run.json", spec)
    staging.initialize(root, {"streams": streams, "sources": {},
        "assigned_progress": {c["name"]: 0 for c in spec["campaigns"]},
        "target_progress": {c["name"]: 0 for c in spec["campaigns"]}},
        {source["pool"] for source in spec["sources"]})
    write_submit_files(root, spec["core_root"], runtime_dir, proxy_path,
                       processing_profiles={STAGE_RUNTIME["mix"], *simulations})
    write_root_dag(root, spec)
    return {"run_id": spec["run_id"], "run_dir": str(root), "dag": str(root / "run.dag"),
            "runtime_release": spec["runtime_release"], "source_files": len(spec["sources"]),
            "max_processing_nodes": sum(c["max_processing_nodes"] for c in spec["campaigns"]),
            "campaign_shards": len(ranges), "submitted": False}


def _inventory(root, spec, state):
    """Build pool indexes using the run's selected inventory ordering model."""
    refs, blocks, exhausted = {}, defaultdict(list), set()
    pools = {source["pool"] for source in spec["sources"]}
    segments = state["planner"]["inventory_segments"]["pools"]
    if set(segments) != pools:
        raise RuntimeError("PLAN inventory pools disagree with the frozen source spec")
    for pool in pools:
        for identity in segments[pool]:
            index = int(identity.removeprefix("PLAN_"))
            if index >= len(spec["sources"]) or spec["sources"][index]["pool"] != pool:
                raise RuntimeError("PLAN inventory segment disagrees with its frozen source")
            source = spec["sources"][index]
            for ref in _receipt(root, state, identity)["outputs"]:
                refs[ref["artifact_id"]] = ref
                blocks[pool].append(SourceBlock(ref["artifact_id"], pool,
                    source["source_id"], len(blocks[pool]), int(ref["events"]), ref["url"]))
        if all(state["planner"]["sources"].get(str(index)) == "skipped"
               or f"PLAN_{index:06d}" in state["accepted"]
               for index, source in enumerate(spec["sources"])
               if source["pool"] == pool):
            exhausted.add(pool)
    return dict(blocks), refs, exhausted


def _targets(spec, state):
    assigned = state["planner"]["assigned_progress"]
    targets = dict(assigned)
    totals = {c["name"]: int(c["max_processing_nodes"]) for c in spec["campaigns"]}
    for _ in range(int(spec["limits"].get("target_window_nodes", 20))):
        candidates = [name for name in totals if targets[name] < totals[name]]
        if not candidates:
            break
        selected = min(candidates, key=lambda name: (targets[name] / totals[name], name))
        targets[selected] += 1
    state["planner"]["target_progress"] = targets
    return targets


def _deficits(root, spec, state):
    blocks, _, _ = _inventory(root, spec, state)
    targets = _targets(spec, state)
    deficits = defaultdict(int)
    for campaign in spec["campaigns"]:
        name = campaign["name"]
        assigned = state["planner"]["assigned_progress"][name]
        gap = targets[name] - assigned
        if gap:
            gap = max(gap, min(int(spec["limits"].get("shard_nodes", 10)),
                               int(campaign["max_processing_nodes"]) - assigned))
        # Simulate whole BLOCK consumption in slot order. Event sums alone
        # would incorrectly reuse an oversized block in repeated same-pool slots.
        cursors = {pool: next_matching(stream["allocation_cursor"], stream)
                   for pool in {slot["pool"] for slot in campaign["sources"]}
                   for stream in [state["planner"]["streams"][stream_key(name, pool)]]}
        missing = defaultdict(int)
        for _ in range(gap):
            for slot in campaign["sources"]:
                pool = slot["pool"]
                stream = state["planner"]["streams"][stream_key(name, pool)]
                selected = blocks.get(pool, [])
                have = 0
                budget = int(slot["lhe_budget"])
                while have < budget and cursors[pool] < len(selected):
                    have += selected[cursors[pool]].events
                    cursors[pool] += stream["stride"]
                missing[pool] += max(0, budget - have)
        for pool, missing_events in missing.items():
            deficits[pool] = max(deficits[pool], missing_events)
    # An admitted source remains supply even before its PLAN result is accepted.
    for index, source in enumerate(spec["sources"]):
        if (state["planner"]["sources"].get(str(index)) == "admitted"
                and f"PLAN_{index:06d}" not in state["accepted"]):
            profile = source.get("resolved_generator_profile", {})
            hint = source.get("planned_events") or source.get("actual_events") or profile.get("planning_events_hint", 0)
            deficits[source["pool"]] = max(0, deficits[source["pool"]] - int(hint))
    return deficits


def _source_tasks(spec, index):
    source = spec["sources"][index]
    limits = spec["limits"]
    base = f"{staging_root(spec)}/sources/{source['pool']}/{source['source_id']}"
    generation = source.get("resolved_generator_profile")
    extra = {"generator_profile": deepcopy(generation)} if generation else {}
    original = artifact_reference(f"LHE_{index:06d}", base + "/source.lhe.gz", "lhe", **extra)
    if generation:
        original["sidecar_url"] = original["url"] + ".json"
    tasks, dependencies = [], []
    plan_id = f"PLAN_{index:06d}"
    if generation:
        lhe = task_manifest(spec, f"LHE_{index:06d}", "lhe", [], [original], dict(
            generator_profile=generation, pool_name=source["pool"], seed=int(source["seed"]),
            max_output_bytes=limits["source_lhe_bytes"], scratch_bytes=limits["source_lhe_bytes"] * 4))
        tasks.append(lhe)
        inputs = [original]
        dependencies.append(([lhe["task_id"]], plan_id))
    else:
        inputs = [artifact_reference(f"INPUT_{index:06d}", source["url"], "lhe", events=int(source["actual_events"]))]
    planner = task_manifest(spec, plan_id, "plan", inputs, [], {
        "pool_name": source["pool"], "primary_seed": int(source["seed"]),
        "group_id": source["source_id"], "events_per_block": spec["block_events"],
        "shuffle_seed": shuffle_seed(int(source["seed"])),
        "max_events_per_plan": int(source.get("planned_events", 0) or 0),
        "block_output_dir": base + "/blocks", "original_output": original,
        "original_max_bytes": limits["source_lhe_bytes"], "max_output_bytes": limits["source_block_bytes"],
        "scratch_bytes": 4 * (limits["source_lhe_bytes"] + limits["source_block_bytes"])})
    tasks.append(planner)
    archive_id = f"SOURCE_ARCHIVE_{index:06d}"
    archive = _archive(spec, archive_id, [original], limits["source_lhe_bytes"])
    archive["parameters"].update(archive_role="source", source_plan_task=plan_id)
    reclaim = _reclaim(spec, f"SOURCE_RECLAIM_{index:06d}", [original])
    tasks.extend([archive, reclaim])
    dependencies.extend([([plan_id], archive_id), ([archive_id], reclaim["task_id"])])
    reclaim["parameters"]["source_archive_task"] = archive_id
    return {"tasks": tasks, "dependencies": dependencies,
            "pre_tasks": [archive_id, reclaim["task_id"]],
            "source_index": index, "pool": source["pool"]}


def _archive(spec, identity, refs, upper_bytes):
    scratch = 3 * int(upper_bytes)
    disk = spec.get("resources", {}).get("archive", {}).get("disk_bytes")
    if disk is not None and scratch > int(disk):
        raise ValueError("archive scratch exceeds its requested disk budget")
    output = artifact_reference(identity, spec["storage"]["ihep_archive"].rstrip("/") + "/"
        + spec["run_id"] + "/" + identity + ".tar", "upstream-archive")
    output["sidecar_url"] = output["url"] + ".json"
    return task_manifest(spec, identity, "archive", refs, [output],
                        {"scratch_bytes": scratch})


def _reclaim(spec, identity, refs):
    return task_manifest(spec, identity, "reclaim", reclaim_references(refs), [],
                        {"staging_prefix": staging_root(spec)})


@_entry
def prepare_source(run_dir, source_index):
    root, spec = _paths(run_dir)
    index = int(source_index)
    if not 0 <= index < len(spec["sources"]):
        raise ValueError("unknown source slot")
    name = source_name(index)
    if admission_hints.enabled() and not _manifest_path(root, name).exists():
        hint_started = time.perf_counter() if control_events.active() else None
        reason = admission_hints.reject_source(root, spec, index)
        if hint_started is not None:
            control_events.record_duration("admission_hint", time.perf_counter() - hint_started)
        if reason:
            return _answer(75, reason=reason, admission_hint=True)
    with staging.transaction(root) as state:
        current = state["planner"]["sources"].get(str(index))
        if current == "skipped":
            return _answer(99, status="unneeded")
        if current is None:
            source = spec["sources"][index]
            relevant = [c for c in spec["campaigns"] if any(s["pool"] == source["pool"] for s in c["sources"])]
            if all(state["planner"]["assigned_progress"][c["name"]] >= int(c["max_processing_nodes"]) for c in relevant):
                state["planner"]["sources"][str(index)] = "skipped"
                return _answer(99, status="unneeded")
            if any(previous["pool"] == source["pool"] and str(j) not in state["planner"]["sources"]
                   for j, previous in enumerate(spec["sources"][:index])):
                raise staging.Deferred("waiting for earlier source admission in this pool")
            deficits = _deficits(root, spec, state)
            if deficits[source["pool"]] <= 0:
                return _answer(75, reason="pool already supplies the soft planning window")
            descriptor = _source_tasks(spec, index)
            _freeze(root, name, descriptor)
            state["planner"]["sources"][str(index)] = "admitted"
            state["status"] = "running"
    return _render(root, name, spec, allow_plan_accept_post_override=True)


def _processing_tasks(spec, allocation, refs):
    campaign = next(c for c in spec["campaigns"] if c["name"] == allocation["campaign"])
    tasks = []
    for node in allocation["nodes"]:
        identity = campaign["name"] + "_" + node["job_id"]
        inputs, slots = [], []
        for slot in node["sources"]:
            inputs.extend(deepcopy(refs[key]) for key in slot["block_refs"])
            slots.append({"slot": slot["slot"], "mode": slot["mode"],
                "inputs": slot["block_refs"], "rng_seed": slot["source_rng_seed"],
                "max_lhe_events": slot["lhe_events"], "target_hepmc_events": node["target_mixed_events"],
                "max_hadronization_retries": int(campaign["sources"][slot["slot"]].get("max_hadronization_retries", 5000))})
        parameters = {"campaign": campaign["name"], "analysis": campaign["analysis"],
            "job_id": node["job_id"], "target_mixed_events": node["target_mixed_events"],
            "global_job_index": node.get("global_job_index", node["job_index"]),
            "edm_event_id": node["edm_event_id"], "normal_shortfall_policy": spec["normal_shortfall_policy"],
            "unused_hepmc_warning_fraction": spec["unused_hepmc_warning_fraction"],
            "processing_environment": spec["processing_environment"]}
        mixed_base = spec["storage"]["ihep_archive"].rstrip("/") + "/" + spec["run_id"]
        hepmc = artifact_reference("HEPMC_" + identity, mixed_base + "/mixed/" + identity + ".hepmc.gz", "hepmc")
        hepmc["sidecar_url"] = hepmc["url"] + ".json"
        ceiling = spec["limits"]["mixed_hepmc_bytes"]
        mix = task_manifest(spec, "MIX_" + identity, "mix", inputs, [hepmc],
            dict(parameters, sources=slots, mixing_rng_seed=node["mixing_rng_seed"],
                 max_output_bytes=ceiling, scratch_bytes=ceiling * 10))
        variants = []
        for profile in campaign["resolved_detector_variants"]:
            variant = profile["profile_id"]
            detector_identity = f"{campaign['name']}_{variant}_{node['job_id']}"
            detector_parameters = dict(parameters, detector_profile_id=variant,
                detector_profile=deepcopy(profile), simulation_runtime=runtime_for_profile(profile, "simulation"),
                merge_runtime=runtime_for_profile(profile, "merge"), ntuple_runtime=runtime_for_profile(profile, "ntuple"))
            product = artifact_reference("MINIAOD_" + detector_identity,
                spec["storage"]["ihep_products"].rstrip("/") + "/" + spec["run_id"] + "/"
                + campaign["name"] + "/" + variant + "/" + node["job_id"] + ".root", "miniaod",
                detector_profile_id=variant, detector_profile=deepcopy(profile))
            mini = task_manifest(spec, "MINIAOD_" + detector_identity, "miniaod", [hepmc], [product], detector_parameters)
            product["sidecar_url"] = product["url"] + ".json"
            mini["parameters"]["max_output_bytes"] = spec["limits"]["miniaod_bytes"]
            variants.append(mini)
        outputs = [hepmc] + [mini["outputs"][0] for mini in variants]
        processing = task_manifest(spec, "PROCESS_" + identity, "processing", inputs,
                                  outputs, dict(scratch_bytes=_processing_scratch(spec["limits"])))
        processing.update(mix=mix, variants=variants, core_revision=spec["core_revision"])
        tasks.append(processing)
    return {"tasks": tasks, "dependencies": [], "allocation": allocation,
            "campaign": campaign["name"]}


@_entry
def prepare_shard(run_dir, campaign, shard_index):
    root, spec = _paths(run_dir)
    name = shard_name(campaign, shard_index)
    matches = [r for r in shard_ranges(spec) if r[0] == campaign and r[1] == int(shard_index)]
    if len(matches) != 1:
        raise ValueError("unknown campaign shard")
    _, _, start, width = matches[0]
    if admission_hints.enabled() and not _manifest_path(root, name).exists():
        hint_started = time.perf_counter() if control_events.active() else None
        reason = admission_hints.reject_shard(root, spec, campaign, start)
        if hint_started is not None:
            control_events.record_duration("admission_hint", time.perf_counter() - hint_started)
        if reason:
            return _answer(75, reason=reason, admission_hint=True)
    with staging.transaction(root) as state:
        if name not in state["allocations"]:
            if state["planner"]["assigned_progress"][campaign] != start:
                raise staging.Deferred("waiting for earlier assignments in this campaign")
            blocks, refs, exhausted = _inventory(root, spec, state)
            config = next(c for c in spec["campaigns"] if c["name"] == campaign)
            allocation = allocate_shard(config, blocks, state["planner"]["streams"], start, width,
                                        exhausted, int(config.get("global_job_index_start", 0)))
            if allocation is None:
                raise staging.Deferred("waiting for a complete shard input range")
            descriptor = _processing_tasks(spec, allocation, refs)
            _freeze(root, name, descriptor)
            for key, cursor in allocation["end_cursors"].items():
                state["planner"]["streams"][key]["allocation_cursor"] = cursor
            state["planner"]["assigned_progress"][campaign] = start + len(allocation["nodes"])
            state["allocations"][name] = {"campaign": campaign, "start_job_index": start,
                "node_count": len(allocation["nodes"]), "completed": False}
            _targets(spec, state)
            state["status"] = "running"
    return _render(root, name, spec)


def _prepare_source_product(root, spec, identity):
    prefix = "SOURCE_ARCHIVE_" if identity.startswith("SOURCE_ARCHIVE_") else "SOURCE_RECLAIM_"
    index = int(identity.removeprefix(prefix))
    descriptor = read_json(_manifest_path(root, source_name(index)))
    task = deepcopy(next(t for t in descriptor["tasks"] if t["task_id"] == identity))
    with staging.transaction(root) as state:
        plan = f"PLAN_{index:06d}"
        if plan not in state["accepted"]:
            raise staging.Deferred("source original has no accepted PLAN result")
        originals = _receipt(root, state, plan)["originals"]
        if prefix == "SOURCE_ARCHIVE_":
            task["inputs"] = deepcopy(originals)
        else:
            archive_id = task["parameters"]["source_archive_task"]
            if archive_id not in state["accepted"]:
                raise staging.Deferred("source original has no accepted archive")
            task["parameters"]["verified_archive"] = _receipt(root, state, archive_id)["outputs"][0]
            task["inputs"] = reclaim_references(originals)
    write_immutable_json(record_path(root, "tasks", identity), task)
    return _answer()


@_entry
def prepare_source_product(run_dir, task_id):
    root, spec = _paths(run_dir)
    identity = token(task_id)
    if not identity.startswith(("SOURCE_ARCHIVE_", "SOURCE_RECLAIM_")):
        raise ValueError("expected a fused run SOURCE archive or reclaim task")
    return _prepare_source_product(root, spec, identity)


def _validate_outputs(task, outcome):
    expected = {ref["artifact_id"]: ref for ref in task["outputs"]}
    outputs = outcome.get("outputs")
    if not isinstance(outputs, list) or len(outputs) != len(expected):
        raise RuntimeError("worker output membership differs from its frozen task")
    if {ref.get("artifact_id") for ref in outputs} != set(expected):
        raise RuntimeError("worker output identity differs from its frozen task")
    for output in outputs:
        ref = expected[output["artifact_id"]]
        if any(output.get(k) != ref.get(k) for k in ("artifact_id", "url", "kind")):
            raise RuntimeError("worker output differs from its frozen task")
        if "sidecar_url" in ref and output.get("sidecar_url") != ref["sidecar_url"]:
            raise RuntimeError("worker sidecar URL differs from its frozen task")
        if type(output.get("size_bytes")) is not int or output["size_bytes"] <= 0:
            raise RuntimeError("worker output has invalid byte size")


def _find_producer(root, state, artifact_id):
    if producer_index.enabled():
        return producer_index.find(root, state, artifact_id, _receipt)
    for identity, marker in state["accepted"].items():
        if marker["stage"] in {"lhe", "plan"}:
            result = _receipt(root, state, identity)
            for ref in result.get("outputs", []) + result.get("originals", []):
                if ref["artifact_id"] == artifact_id:
                    return ref
    raise RuntimeError("input has no accepted producer: " + artifact_id)


def _post_pre_status(pre_return_code, return_code):
    """Classify PRE outcome independently of worker exit (including -1004)."""
    pre = int(pre_return_code)
    if pre in (0, -1):
        return "run"
    if pre == 99 and int(return_code) == -1004:
        return "skip"
    return "failed"


def _pre_failure(pre_return_code):
    # Never return PRE's 75 here: POST DEFER is reserved for its own lock/wait.
    return _answer(1, reason="PRE failed or unsupported skip", pre_return_code=int(pre_return_code))


def _accept_validated_outcome(root, spec, state, identity, task, outcome):
    """Settle a previously validated task outcome in the caller's transaction."""
    stage = task["stage"]
    if stage == "plan":
        source_index = int(identity.removeprefix("PLAN_"))
        validate_planner_result(task, spec["sources"][source_index], outcome)
        p = task["parameters"]
        for index, block in enumerate(outcome["outputs"]):
            expected = f"{p['block_output_dir']}/block_{p['group_id']}_{index:06d}.lhe.gz"
            if block["url"] != expected or "sidecar_url" in block:
                raise ValueError("PLAN result does not name an exact run-owned LHE block")
        original = outcome["originals"][0]
        if original.get("sidecar_url") != p["original_output"].get("sidecar_url"):
            raise ValueError("PLAN result has an unexpected source-original sidecar")
        if spec["sources"][source_index].get("resolved_generator_profile"):
            produced = _receipt(root, state, f"LHE_{source_index:06d}")["outputs"][0]
            retained = outcome["originals"][0]
            if (outcome["actual_input_events"] != produced["events"]
                    or any(retained.get(k) != produced.get(k) for k in
                           ("artifact_id", "url", "kind", "size_bytes", "events", "sidecar_url"))):
                raise RuntimeError("generated PLAN original differs from accepted LHE producer")
    elif stage == "archive":
        validate_archive_result(task, outcome)
        _validate_outputs(task, outcome)
        for member in outcome["members"]:
            accepted = _find_producer(root, state, member["artifact_id"])
            if any(member.get(k) != accepted.get(k) for k in ("url", "kind", "size_bytes", "events")):
                raise RuntimeError("archive member differs from its accepted producer")
    elif stage == "reclaim":
        validate_reclaim_result(task, outcome)
    elif stage == "processing":
        validate_processing_outputs(task, outcome["outputs"])
    elif stage == "lhe":
        _validate_outputs(task, outcome)
        events = outcome["outputs"][0].get("events")
        if type(events) is not int or events <= 0:
            raise RuntimeError("LHE producer has no valid event count")
    else:
        raise ValueError(f"unexpected CERN task stage: {stage}")
    # Scratch locations are attempt-specific.
    outcome = dict(outcome, outputs=[{k: v for k, v in ref.items() if k != "local_path"}
                                     for ref in outcome["outputs"]])
    receipt = _receipt_path(root, identity)
    if stage == "reclaim" and receipt.is_file():
        previous = read_json(receipt)
        validate_reclaim_result(task, previous)
        outcome = previous
    write_immutable_json(receipt, outcome)
    state["accepted"][identity] = {"stage": stage}
    if stage == "plan":
        pool = spec["sources"][source_index]["pool"]
        pool_segments = state["planner"]["inventory_segments"]["pools"].get(pool)
        if pool_segments is None:
            raise RuntimeError("PLAN inventory has no segment list for source pool")
        pool_segments.append(identity)
    producer_index.record(root, state, identity, outcome, _receipt)


@_entry
def accept_result(run_dir, task_id, return_code=0, *, pre_return_code=0):
    if _post_pre_status(pre_return_code, return_code) != "run":
        return _pre_failure(pre_return_code)
    if int(return_code) != 0:
        return _answer(1 if int(return_code) == 75 else int(return_code),
                       reason="worker failed", worker_return_code=int(return_code))
    root, spec = _paths(run_dir)
    identity = token(task_id)
    task = read_json(record_path(root, "tasks", identity))
    with staging.transaction(root) as state:
        if identity in state["accepted"]:
            return _answer(status="accepted")
        outcome = read_json(record_path(root, "results", identity))
        validate_result_envelope(task, outcome, spec["runtime_release"])
        _accept_validated_outcome(root, spec, state, identity, task, outcome)
    return _answer(status="accepted")


def _without_archive_local_paths(outcome):
    value = deepcopy(outcome)
    outputs = value.get("outputs") if isinstance(value, dict) else None
    if not isinstance(outputs, list):
        raise ValueError("archive orphan result has no outputs list")
    for output in outputs:
        if not isinstance(output, dict):
            raise ValueError("archive orphan output is malformed")
        output.pop("local_path", None)
    return value


@_entry
def recover_archive_orphan(run_dir, task_id, *, confirm_no_active_writer=False):
    """Accept one archive orphan after allowing only its ephemeral local path to differ."""
    if confirm_no_active_writer is not True:
        raise ValueError("archive orphan recovery requires explicit no-active-writer confirmation")
    root, spec = _paths(run_dir)
    identity = token(task_id)
    with staging.transaction(root) as state:
        if identity in state["accepted"]:
            raise RuntimeError("archive orphan already has an accepted marker")
        task = read_json(record_path(root, "tasks", identity))
        if task.get("stage") != "archive":
            raise ValueError("archive orphan recovery only accepts archive tasks")
        outcome = read_json(record_path(root, "results", identity))
        receipt_path = _receipt_path(root, identity)
        orphan = read_json(receipt_path)
        validate_result_envelope(task, outcome, spec["runtime_release"])
        validate_result_envelope(task, orphan, spec["runtime_release"])
        if _without_archive_local_paths(outcome) != _without_archive_local_paths(orphan):
            raise RuntimeError("archive orphan differs beyond output local_path")
        index = state.get("producer_index", {})
        refs = {item.get("artifact_id") for item in orphan.get("outputs", [])}
        if any(artifact_id in index.get("artifacts", {}) for artifact_id in refs):
            raise RuntimeError("archive orphan output unexpectedly appears in producer index")
        _accept_validated_outcome(root, spec, state, identity, task, orphan)
    return _answer(status="accepted", recovered_orphan=True)


def _require_all(root, state, descriptor):
    missing = [task["task_id"] for task in descriptor["tasks"]
               if state["accepted"].get(task["task_id"], {}).get("stage") != task["stage"]]
    if missing:
        raise RuntimeError("child completion lacks accepted results: " + ", ".join(missing))
    durable = [task["task_id"] for task in descriptor["tasks"]
               if task["stage"] in {"archive", "processing"}]
    if not durable:
        raise RuntimeError("completed child has no durable archive or processing results")
    for identity in durable:
        _receipt(root, state, identity)


@_entry
def complete_source(run_dir, source_index, return_code=0, *, pre_return_code=0):
    pre_status = _post_pre_status(pre_return_code, return_code)
    if pre_status == "failed":
        return _pre_failure(pre_return_code)
    if pre_status == "skip":
        root, spec = _paths(run_dir)
        index = int(source_index)
        if not 0 <= index < len(spec["sources"]):
            raise ValueError("unknown source slot")
        state = read_json(root / "state.json")
        staging.require_schema(state)
        if state["planner"]["sources"].get(str(index)) != "skipped":
            return _answer(1, reason="source skip lacks committed unneeded-source state")
        return _answer(status="skipped")
    if int(return_code) != 0:
        return _answer(1 if int(return_code) == 75 else int(return_code),
                       reason="source child failed", worker_return_code=int(return_code))
    root, spec = _paths(run_dir)
    index = int(source_index)
    descriptor = read_json(_manifest_path(root, source_name(index)))
    with staging.transaction(root) as state:
        _require_all(root, state, descriptor)
        state["planner"]["sources"][str(index)] = "completed"
    return _answer(status="completed")


@_entry
def complete_shard(run_dir, campaign, shard_index, return_code=0, *, pre_return_code=0):
    # prepare_shard currently has no supported skip outcome.
    if _post_pre_status(pre_return_code, return_code) != "run":
        return _pre_failure(pre_return_code)
    if int(return_code) != 0:
        return _answer(1 if int(return_code) == 75 else int(return_code),
                       reason="campaign child failed", worker_return_code=int(return_code))
    root, spec = _paths(run_dir)
    name = shard_name(campaign, shard_index)
    descriptor = read_json(_manifest_path(root, name))
    with staging.transaction(root) as state:
        _require_all(root, state, descriptor)
        state["allocations"][name]["completed"] = True
    return _answer(status="completed")


def _require_finished_work(spec, state):
    if any(state["planner"]["sources"].get(str(i)) not in {"completed", "skipped"}
           for i in range(len(spec["sources"]))):
        raise RuntimeError("requires every finite source slot completed or skipped")
    expected = {shard_name(c, i) for c, i, _, _ in shard_ranges(spec)}
    if (set(state["allocations"]) != expected
            or any(not a["completed"] for a in state["allocations"].values())):
        raise RuntimeError("requires all campaign shards completed")


@_entry
def prepare_cleanup(run_dir, pool, slot):
    root, spec = _paths(run_dir)
    if int(slot) != 0 or pool not in {source["pool"] for source in spec["sources"]}:
        raise ValueError("unknown terminal cleanup slot")
    name = cleanup_name(pool, slot)
    with staging.transaction(root) as state:
        _require_finished_work(spec, state)
        refs = []
        for index, source in enumerate(spec["sources"]):
            if source["pool"] != pool or state["planner"]["sources"][str(index)] == "skipped":
                continue
            refs.extend(_receipt(root, state, f"PLAN_{index:06d}")["outputs"])
        task = _reclaim(spec, name, refs)
        task["parameters"]["terminal_blocks"] = True
        write_immutable_json(record_path(root, "tasks", name), task)
    return _answer(status="prepared", task_id=name)


def committed_archives(run_dir, state=None):
    root = Path(run_dir)
    state = read_json(root / "state.json") if state is None else state
    staging.require_schema(state)
    return [_receipt(root, state, identity) for identity in sorted(state["accepted"])
            if state["accepted"][identity]["stage"] == "archive"]



def restore_archived_sources(run_dir, archive_id, destination, artifact_ids=None):
    """Restore recorded original LHE members for an operator recovery or replan."""
    root = Path(run_dir).resolve()
    staging.require_schema(read_json(root / "run.json"))
    archives = committed_archives(root)
    archive = next((item for item in archives
                    if any(ref.get("artifact_id") == archive_id for ref in item.get("outputs", []))), None)
    if archive is None:
        raise ValueError("unknown verified archive artifact")
    selected = [item for item in archive["members"] if item.get("kind") == "lhe"]
    if artifact_ids is not None:
        wanted = set(artifact_ids)
        selected = [item for item in selected if item["artifact_id"] in wanted]
        if wanted != {item["artifact_id"] for item in selected}:
            raise ValueError("requested original is not recorded in this archive")
    if not selected:
        raise ValueError("archive has no selected original LHE members")
    output = next(ref for ref in archive["outputs"] if ref["artifact_id"] == archive_id)
    return {"archive_id": archive_id,
            "restored": restore_members(output, selected, destination)}


def committed_groups(run_dir, state=None):
    root, spec = _paths(run_dir)
    state = read_json(root / "state.json") if state is None else state
    staging.require_schema(state)
    return load_groups(root, spec, state["merge_group_counts"])


@_entry
def finalize_run(run_dir):
    root, spec = _paths(run_dir)
    with staging.transaction(root) as state:
        if state["status"] != "complete":
            _require_finished_work(spec, state)
            for pool in {source["pool"] for source in spec["sources"]}:
                identity = cleanup_name(pool, 0)
                if state["accepted"].get(identity, {}).get("stage") != "reclaim":
                    raise RuntimeError("finalizer requires terminal LHE block reclamation: " + identity)
                _receipt(root, state, identity)
            ordered = []
            for identity, marker in state["accepted"].items():
                if marker["stage"] != "processing":
                    continue
                task = read_json(record_path(root, "tasks", identity))
                outputs = _receipt(root, state, identity)["outputs"]
                for variant, ref in zip(task["variants"], outputs[1:]):
                    p = variant["parameters"]
                    ordered.append((p["campaign"], p["detector_profile_id"],
                                    int(p["job_id"].rsplit("BLOCK", 1)[1]), ref))
            groups_state = {"merge_buffers": {}, "merge_group_counts": {}}
            for campaign, profile, _, ref in sorted(ordered):
                groups_state["merge_buffers"].setdefault(sample_key(campaign, profile), []).append(ref)
            # Immutable groups first, state counters and status next, sentinel last.
            for campaign, profile, ordinal, group in merge_groups(spec, groups_state):
                write_group(root, campaign, profile, ordinal, group)
            state["merge_group_counts"] = groups_state["merge_group_counts"]
            state["status"] = "complete"
    (root / ".complete").touch()
    return _answer(status="complete", merge_groups=sum(state["merge_group_counts"].values()))
