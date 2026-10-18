"""Local, transaction-bound orchestration of independent campaign shards.

DAGMan remains the execution authority. This controller accepts immutable worker
results, allocates finite inputs and maintains the shared staging byte ledger.
No function in the scheduling seams contacts remote storage.
"""
from collections import defaultdict
from copy import deepcopy
from functools import wraps
from pathlib import Path

from full_mc.application import production as legacy
from full_mc.application import staging
from full_mc.application.ledger import load_groups, sample_key, write_group
from full_mc.credentials import resolve_proxy_path
from full_mc.domain.identity import shuffle_seed
from full_mc.domain.inventory import SourceBlock
from full_mc.domain.streams import allocate_shard, build_streams, may_still_need, next_matching, stream_key
from full_mc.io import atomic_json, read_json, write_immutable_json
from full_mc.local_paths import LAYOUT_MARKER, record_path
from full_mc.planning.graph import token, write_submit_files
from full_mc.planning.shards import (cleanup_name, shard_name, shard_ranges,
                                     source_name, write_child_dag, write_root_dag)
from full_mc.runtime.packages import validate_runtime_release
from full_mc.runtime.profiles import STAGE_RUNTIME, runtime_for_profile
from full_mc.settings import load_run_spec


def _entry(function):
    @wraps(function)
    def call(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except BlockingIOError:
            return {"exit_code": 75, "reason": "local state lock busy"}
    return call


def _answer(code=0, **extra):
    return dict(exit_code=code, **extra)


def _paths(run_dir):
    root = Path(run_dir).resolve()
    return root, read_json(root / "run.json")


def _manifest_path(root, name):
    return record_path(root, "ledger/shards", token(name))


def _receipt_path(root, task_id):
    return record_path(root, "ledger/accepted", token(task_id))


def _receipt(root, state, task_id):
    if task_id not in state["accepted"]:
        raise RuntimeError("task has no accepted result: " + task_id)
    return read_json(_receipt_path(root, task_id))


def _render(root, name, spec):
    manifest = read_json(_manifest_path(root, name))
    path = write_child_dag(root, name, manifest["tasks"], manifest["dependencies"],
                           spec, manifest.get("pre_tasks", manifest.get("claims", {})))
    return _answer(dag=str(path), status="prepared")


def _freeze(root, name, descriptor):
    write_immutable_json(_manifest_path(root, name), descriptor)


def _run_owned(spec, ref):
    return str(ref["url"]).startswith(legacy._staging(spec) + "/")


def _claim(task, ceiling):
    task["parameters"]["staging_claim_bytes"] = int(ceiling)
    return task


def _minimum_capacity(spec):
    """A complementary source tuple and one complete archive group must fit."""
    limits = spec["limits"]
    per_source = limits["source_lhe_bytes"] + limits["source_block_bytes"]
    headroom = int(limits.get("policy_headroom_bytes", 0))
    return max((len({s["pool"] for s in c["sources"]}) * per_source
                + max(min(int(limits.get("shard_nodes", 10)), int(c["max_processing_nodes"]))
                      * limits["mixed_hepmc_bytes"], headroom)
                for c in spec["campaigns"]), default=0)



def plan_run(spec_path, output_dir, log_level=None):
    """Freeze a new campaign_shards run without submitting or remote I/O."""
    spec = deepcopy(spec_path) if isinstance(spec_path, dict) else load_run_spec(spec_path)
    if log_level:
        spec["log_level"] = log_level
    legacy._authority(spec)
    limits = spec.setdefault("limits", {})
    for key in ("source_lhe_bytes", "source_block_bytes", "mixed_hepmc_bytes"):
        if type(limits.get(key)) is not int or limits[key] <= 0:
            raise ValueError(f"limits.{key} must be a positive output byte ceiling")
    limits.setdefault("shard_nodes", 10)
    limits.setdefault("target_window_nodes", 20)
    if limits["target_window_nodes"] <= 0:
        raise ValueError("target_window_nodes must be positive")
    ranges = shard_ranges(spec)
    streams = build_streams(spec)
    usable = int(spec["storage"]["staging_bytes"]) - int(spec["storage"]["reserve_bytes"])
    if usable < _minimum_capacity(spec):
        raise ValueError("staging cannot fit a complementary source tuple plus full shard MIX headroom")
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
    required = {STAGE_RUNTIME[s] for s in ("plan", "mix", "archive", "reclaim")}
    required.update(runtime_for_profile(p, "simulation") for c in spec["campaigns"]
                    for p in c["resolved_detector_variants"])
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
    staging.initialize(root, usable, {"streams": streams, "sources": {},
        "assigned_progress": {c["name"]: 0 for c in spec["campaigns"]},
        "target_progress": {c["name"]: 0 for c in spec["campaigns"]}})
    write_submit_files(root, spec["core_root"], runtime_dir, proxy_path)
    write_root_dag(root, spec)
    return {"run_id": spec["run_id"], "run_dir": str(root), "dag": str(root / "run.dag"),
            "runtime_release": spec["runtime_release"], "source_files": len(spec["sources"]),
            "max_processing_nodes": sum(c["max_processing_nodes"] for c in spec["campaigns"]),
            "campaign_shards": len(ranges), "submitted": False}


def _inventory(root, spec, state):
    """Pool indexes expose only their own frozen, contiguous accepted prefix."""
    refs, blocks, exhausted = {}, defaultdict(list), set()
    pools = {source["pool"] for source in spec["sources"]}
    for pool in pools:
        complete = True
        for index, source in enumerate(spec["sources"]):
            if source["pool"] != pool:
                continue
            status = state["planner"]["sources"].get(str(index))
            if status == "skipped":
                continue
            identity = f"PLAN_{index:06d}"
            if identity not in state["accepted"]:
                complete = False
                break
            for ref in _receipt(root, state, identity)["outputs"]:
                refs[ref["artifact_id"]] = ref
                blocks[pool].append(SourceBlock(ref["artifact_id"], pool,
                    source["source_id"], len(blocks[pool]), int(ref["events"]), ref["url"]))
        if complete:
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
    by_campaign = {}
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
        by_campaign[name] = dict(missing)
        for pool, missing_events in missing.items():
            deficits[pool] = max(deficits[pool], missing_events)
    # An admitted source remains supply even before its PLAN result is accepted.
    for index, source in enumerate(spec["sources"]):
        if (state["planner"]["sources"].get(str(index)) == "admitted"
                and f"PLAN_{index:06d}" not in state["accepted"]):
            profile = source.get("resolved_generator_profile", {})
            hint = source.get("planned_events") or source.get("actual_events") or profile.get("planning_events_hint", 0)
            deficits[source["pool"]] = max(0, deficits[source["pool"]] - int(hint))
            for missing in by_campaign.values():
                if source["pool"] in missing:
                    missing[source["pool"]] = max(0, missing[source["pool"]] - int(hint))
    return deficits, by_campaign


def _source_tasks(spec, index):
    source = spec["sources"][index]
    limits = spec["limits"]
    base = f"{legacy._staging(spec)}/sources/{source['pool']}/{source['source_id']}"
    generation = source.get("resolved_generator_profile")
    extra = {"generator_profile": deepcopy(generation)} if generation else {}
    original = legacy._ref(f"LHE_{index:06d}", base + "/source.lhe.gz", "lhe", **extra)
    if generation:
        original["sidecar_url"] = original["url"] + ".json"
    tasks, dependencies, claims = [], [], {}
    plan_id = f"PLAN_{index:06d}"
    if generation:
        lhe = _claim(legacy._task(spec, f"LHE_{index:06d}", "lhe", [], [original], dict(
            generator_profile=generation, pool_name=source["pool"], seed=int(source["seed"]),
            max_output_bytes=limits["source_lhe_bytes"], scratch_bytes=limits["source_lhe_bytes"] * 4)),
            limits["source_lhe_bytes"])
        tasks.append(lhe)
        claims[lhe["task_id"]] = limits["source_lhe_bytes"]
        inputs = [original]
        dependencies.append(([lhe["task_id"]], plan_id))
        planner_ceiling = limits["source_block_bytes"]
    else:
        inputs = [legacy._ref(f"INPUT_{index:06d}", source["url"], "lhe", events=int(source["actual_events"]))]
        planner_ceiling = limits["source_lhe_bytes"] + limits["source_block_bytes"]
    planner = _claim(legacy._task(spec, plan_id, "plan", inputs, [], {
        "pool_name": source["pool"], "primary_seed": int(source["seed"]),
        "group_id": source["source_id"], "events_per_block": spec["block_events"],
        "shuffle_seed": shuffle_seed(int(source["seed"])),
        "max_events_per_plan": int(source.get("planned_events", 0) or 0),
        "block_output_dir": base + "/blocks", "original_output": original,
        "original_max_bytes": limits["source_lhe_bytes"], "max_output_bytes": limits["source_block_bytes"],
        "scratch_bytes": 4 * (limits["source_lhe_bytes"] + limits["source_block_bytes"])}), planner_ceiling)
    tasks.append(planner)
    claims[plan_id] = planner_ceiling
    archive_id = f"SOURCE_ARCHIVE_{index:06d}"
    archive = _archive(spec, archive_id, [original], [], limits["source_lhe_bytes"])
    archive["parameters"].update(archive_role="source", source_plan_task=plan_id)
    reclaim = _reclaim(spec, f"SOURCE_RECLAIM_{index:06d}", [original])
    tasks.extend([archive, reclaim])
    dependencies.extend([([plan_id], archive_id), ([archive_id], reclaim["task_id"])])
    return {"tasks": tasks, "dependencies": dependencies, "claims": claims,
            "pre_tasks": list(claims) + [archive_id],
            "source_index": index, "pool": source["pool"]}


def _archive(spec, identity, refs, miniaods, upper_bytes):
    scratch = 3 * int(upper_bytes)
    disk = spec.get("resources", {}).get("archive", {}).get("disk_bytes")
    if disk is not None and scratch > int(disk):
        raise ValueError("archive scratch exceeds its requested disk budget")
    output = legacy._ref(identity, spec["storage"]["ihep_archive"].rstrip("/") + "/"
        + spec["run_id"] + "/" + identity + ".tar", "upstream-archive")
    return legacy._task(spec, identity, "archive", refs, [output],
                        {"component_miniaods": miniaods, "scratch_bytes": scratch})


def _reclaim(spec, identity, refs):
    return legacy._task(spec, identity, "reclaim", legacy._reclaim_refs(refs), [],
                        {"staging_prefix": legacy._staging(spec)})


def _work_can_free_bytes(root, spec, state):
    if any(status == "admitted" for status in state["planner"]["sources"].values()):
        return True
    if any(not value["completed"] for value in state["allocations"].values()):
        return True
    if any(not value["completed"] for value in state["cleanup_claims"].values()):
        return True
    return any(_cleanup_refs(root, spec, state, pool) for pool in {s["pool"] for s in spec["sources"]})


@_entry
def prepare_source(run_dir, source_index):
    root, spec = _paths(run_dir)
    index = int(source_index)
    if not 0 <= index < len(spec["sources"]):
        raise ValueError("unknown source slot")
    name = source_name(index)
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
                return _answer(75, reason="waiting for earlier source admission in this pool")
            deficits, by_campaign = _deficits(root, spec, state)
            if deficits[source["pool"]] <= 0:
                return _answer(75, reason="pool already supplies the soft planning window")
            descriptor = _source_tasks(spec, index)
            # Reserve a real campaign's complementary supply and full next
            # archive group as a threshold, not a second ledger claim. Unrelated
            # campaigns must not reserve one another's source headroom.
            per_source = spec["limits"]["source_lhe_bytes"] + spec["limits"]["source_block_bytes"]
            pending_pools = {s["pool"] for j, s in enumerate(spec["sources"])
                             if state["planner"]["sources"].get(str(j)) == "admitted"
                             and f"PLAN_{j:06d}" not in state["accepted"]}
            thresholds = []
            for campaign in relevant:
                missing = by_campaign[campaign["name"]]
                remaining = int(campaign["max_processing_nodes"]) - state["planner"]["assigned_progress"][campaign["name"]]
                if remaining <= 0 or missing.get(source["pool"], 0) <= 0:
                    continue
                complement = sum(per_source for pool, deficit in missing.items()
                                 if pool != source["pool"] and deficit > 0 and pool not in pending_pools)
                group_nodes = min(int(spec["limits"].get("shard_nodes", 10)), remaining)
                headroom = max(group_nodes * spec["limits"]["mixed_hepmc_bytes"],
                               int(spec["limits"].get("policy_headroom_bytes", 0)))
                thresholds.append(complement + headroom)
            threshold = min(thresholds)
            account = state["staging"]
            demand = account["resident_bytes"] + sum(account["claims"].values()) + sum(descriptor["claims"].values())
            if demand + threshold > account["usable_bytes"]:
                if not _work_can_free_bytes(root, spec, state):
                    raise RuntimeError("insufficient staging capacity for the next complete shard input range; "
                                       "no admitted source, allocated shard or eligible cleanup can free bytes")
                return _answer(75, reason="preserving complementary source and MIX headroom")
            if staging.reserve_bundle(state, descriptor["claims"]) == 75:
                return _answer(75, reason="staging capacity")
            _freeze(root, name, descriptor)
            state["planner"]["sources"][str(index)] = "admitted"
            state["status"] = "running"
    return _render(root, name, spec)


def _processing_tasks(spec, allocation, refs, name):
    campaign = next(c for c in spec["campaigns"] if c["name"] == allocation["campaign"])
    tasks, dependencies, miniaods, mixed, claims = [], [], [], [], {}
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
        hepmc = legacy._ref("HEPMC_" + identity, legacy._staging(spec) + "/mixed/" + identity + ".hepmc.gz", "hepmc")
        hepmc["sidecar_url"] = hepmc["url"] + ".json"
        ceiling = spec["limits"]["mixed_hepmc_bytes"]
        mix = _claim(legacy._task(spec, "MIX_" + identity, "mix", inputs, [hepmc],
            dict(parameters, sources=slots, staging_bundle=name, mixing_rng_seed=node["mixing_rng_seed"],
                 max_output_bytes=ceiling, scratch_bytes=ceiling * 10)), ceiling)
        tasks.append(mix)
        claims[mix["task_id"]] = ceiling
        for profile in campaign["resolved_detector_variants"]:
            variant = profile["profile_id"]
            detector_identity = f"{campaign['name']}_{variant}_{node['job_id']}"
            detector_parameters = dict(parameters, detector_profile_id=variant,
                detector_profile=deepcopy(profile), simulation_runtime=runtime_for_profile(profile, "simulation"),
                merge_runtime=runtime_for_profile(profile, "merge"), ntuple_runtime=runtime_for_profile(profile, "ntuple"))
            product = legacy._ref("MINIAOD_" + detector_identity,
                spec["storage"]["ihep_products"].rstrip("/") + "/" + spec["run_id"] + "/"
                + campaign["name"] + "/" + variant + "/" + node["job_id"] + ".root", "miniaod",
                detector_profile_id=variant, detector_profile=deepcopy(profile))
            mini = legacy._task(spec, "MINIAOD_" + detector_identity, "miniaod", [hepmc], [product], detector_parameters)
            tasks.append(mini)
            dependencies.append(([mix["task_id"]], mini["task_id"]))
            miniaods.append(mini)
        mixed.append(hepmc)
    archive = _archive(spec, "ARCHIVE_" + name, mixed,
        [m["outputs"][0] for m in miniaods], len(mixed) * spec["limits"]["mixed_hepmc_bytes"])
    reclaim = _reclaim(spec, "RECLAIM_" + name, mixed)
    tasks.extend([archive, reclaim])
    dependencies.extend([([m["task_id"] for m in miniaods], archive["task_id"]),
                         ([archive["task_id"]], reclaim["task_id"])])
    return {"tasks": tasks, "dependencies": dependencies, "claims": claims,
            "allocation": allocation, "campaign": campaign["name"],
            "archive_task": archive["task_id"]}


@_entry
def prepare_shard(run_dir, campaign, shard_index):
    root, spec = _paths(run_dir)
    name = shard_name(campaign, shard_index)
    matches = [r for r in shard_ranges(spec) if r[0] == campaign and r[1] == int(shard_index)]
    if len(matches) != 1:
        raise ValueError("unknown campaign shard")
    _, _, start, width = matches[0]
    with staging.transaction(root) as state:
        if name not in state["allocations"]:
            if state["planner"]["assigned_progress"][campaign] != start:
                return _answer(75, reason="waiting for earlier assignments in this campaign")
            blocks, refs, exhausted = _inventory(root, spec, state)
            config = next(c for c in spec["campaigns"] if c["name"] == campaign)
            allocation = allocate_shard(config, blocks, state["planner"]["streams"], start, width,
                                        exhausted, int(config.get("global_job_index_start", 0)))
            if allocation is None:
                return _answer(75, reason="waiting for a complete shard input range")
            descriptor = _processing_tasks(spec, allocation, refs, name)
            # The first MIX PRE atomically admits this complete archive group.
            # A half-produced shard must never pin bytes needed by its own siblings.
            _freeze(root, name, descriptor)
            for key, cursor in allocation["end_cursors"].items():
                state["planner"]["streams"][key]["allocation_cursor"] = cursor
            state["planner"]["assigned_progress"][campaign] = start + len(allocation["nodes"])
            state["allocations"][name] = {"campaign": campaign, "start_job_index": start,
                "node_count": len(allocation["nodes"]), "completed": False}
            _targets(spec, state)
            state["status"] = "running"
    return _render(root, name, spec)


@_entry
def admit_producer(run_dir, task_id):
    root, spec = _paths(run_dir)
    identity = token(task_id)
    if identity.startswith("SOURCE_ARCHIVE_"):
        index = int(identity.removeprefix("SOURCE_ARCHIVE_"))
        descriptor = read_json(_manifest_path(root, source_name(index)))
        task = deepcopy(next(t for t in descriptor["tasks"] if t["task_id"] == identity))
        with staging.transaction(root) as state:
            plan = f"PLAN_{index:06d}"
            if plan not in state["accepted"]:
                return _answer(75, reason="source original has no accepted PLAN result")
            task["inputs"] = deepcopy(_receipt(root, state, plan)["originals"])
        write_immutable_json(record_path(root, "tasks", identity), task)
        return _answer()
    task = read_json(record_path(root, "tasks", identity))
    ceiling = task["parameters"].get("staging_claim_bytes")
    if ceiling is None:
        raise ValueError("producer PRE requires an explicit frozen staging claim")
    with staging.transaction(root) as state:
        if identity.startswith(("LHE_", "PLAN_")) and identity not in state["staging"]["ceilings"]:
            raise RuntimeError("source child has no atomic source-bundle admission")
        bundle = task["parameters"].get("staging_bundle")
        claims = read_json(_manifest_path(root, bundle))["claims"] if bundle else {identity: ceiling}
        code = staging.reserve_bundle(state, claims)
    return _answer(code, reason="staging capacity" if code else "admitted")


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


def _validate_staged(spec, task, outcome):
    objects = outcome.get("staged_objects")
    if not isinstance(objects, list):
        raise RuntimeError("staging producer result has no exact physical object list")
    refs = outcome.get("outputs", [])
    if task["stage"] == "plan":
        new_originals = [r for r in outcome.get("originals", [])
                         if r["url"] not in {r["url"] for r in task["inputs"]}]
        refs = refs + new_originals
    expected = []
    for ref in refs:
        if not _run_owned(spec, ref):
            raise RuntimeError("producer claims an object outside run-owned staging")
        expected.append({key: ref[key] for key in ("artifact_id", "url", "kind", "size_bytes")})
        if "sidecar_url" in ref:
            expected.append(dict(artifact_id=ref["artifact_id"] + ":sidecar", url=ref["sidecar_url"],
                                 kind="sidecar", size_bytes=ref.get("sidecar_size_bytes")))
    if sorted(objects, key=lambda r: r["url"]) != sorted(expected, key=lambda r: r["url"]):
        raise RuntimeError("staged physical objects differ from accepted producer references")
    return objects


def _find_producer(root, state, artifact_id):
    for identity, marker in state["accepted"].items():
        if marker["stage"] in {"lhe", "plan", "mix"}:
            result = _receipt(root, state, identity)
            for ref in result.get("outputs", []) + result.get("originals", []):
                if ref["artifact_id"] == artifact_id:
                    return ref
    raise RuntimeError("input has no accepted producer: " + artifact_id)


def _validate_miniaod(root, spec, state, task, outcome):
    output = outcome["outputs"][0]
    actual = output.get("events")
    expected = _find_producer(root, state, task["inputs"][0]["artifact_id"])["events"]
    partial = output.get("partial")
    profile = task["parameters"]["detector_profile_id"]
    if (type(actual) is not int or not 0 < actual <= expected
            or output.get("expected_events") != expected or type(partial) is not bool
            or partial != (actual < expected) or outcome.get("partial") != partial
            or output.get("detector_profile_id") != profile
            or output.get("detector_profile") != task["parameters"]["detector_profile"]
            or output.get("count_source") != "edmFileUtil"
            or (partial and spec["normal_shortfall_policy"] != "report-and-truncate")):
        raise RuntimeError("MiniAOD count or provenance disagrees with its accepted HepMC")


def _physical_refs(state, refs):
    objects = state["staging"]["objects"]
    result = []
    for ref in refs:
        for url in [ref["url"]] + ([ref["sidecar_url"]] if "sidecar_url" in ref else []):
            if url not in objects:
                raise RuntimeError("reclaim reference was never staged by this run: " + url)
            result.append({k: objects[url][k] for k in ("artifact_id", "url", "kind", "size_bytes")})
    return result


@_entry
def accept_result(run_dir, task_id, return_code=0):
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
        legacy.validate_result_envelope(task, outcome, spec["runtime_release"])
        stage = task["stage"]
        if stage == "plan":
            legacy._validate_planner_result(root, spec, {"plan_task": identity,
                "source_index": int(identity.removeprefix("PLAN_"))}, outcome)
            source_index = int(identity.removeprefix("PLAN_"))
            if spec["sources"][source_index].get("resolved_generator_profile"):
                produced = _receipt(root, state, f"LHE_{source_index:06d}")["outputs"][0]
                retained = outcome["originals"][0]
                if (outcome["actual_input_events"] != produced["events"]
                        or any(retained.get(k) != produced.get(k) for k in
                               ("artifact_id", "url", "kind", "size_bytes", "events", "sidecar_url"))):
                    raise RuntimeError("generated PLAN original differs from accepted LHE producer")
        elif stage == "archive":
            legacy._validate_archive_result(root, spec, {"archive_task": identity}, outcome)
            for member in outcome["members"]:
                accepted = _find_producer(root, state, member["artifact_id"])
                if any(member.get(k) != accepted.get(k) for k in ("url", "kind", "size_bytes", "events")):
                    raise RuntimeError("archive member differs from its accepted producer")
        elif stage == "reclaim":
            legacy._validate_reclaim_result(root, spec, {"reclaim_task": identity}, outcome)
            staging.settle_reclaim(state, identity, _physical_refs(state, task["inputs"]), outcome["reclaimed"])
            if identity in state["cleanup_claims"]:
                state["cleanup_claims"][identity]["completed"] = True
        else:
            _validate_outputs(task, outcome)
            if stage in {"lhe", "mix"}:
                for output in outcome["outputs"]:
                    if type(output.get("events")) is not int or output["events"] <= 0:
                        raise RuntimeError("staging producer has no valid event count")
                if stage == "mix" and outcome["outputs"][0]["events"] > task["parameters"]["target_mixed_events"]:
                    raise RuntimeError("mixed event count exceeds frozen target")
            elif stage == "miniaod":
                _validate_miniaod(root, spec, state, task, outcome)
        if "staging_claim_bytes" in task["parameters"]:
            staging.settle_publication(state, identity, _validate_staged(spec, task, outcome))
        write_immutable_json(_receipt_path(root, identity), outcome)
        state["accepted"][identity] = {"stage": stage}
    return _answer(status="accepted")


def _require_all(root, state, descriptor):
    missing = [t["task_id"] for t in descriptor["tasks"] if t["task_id"] not in state["accepted"]]
    if missing:
        raise RuntimeError("child completion lacks accepted results: " + ", ".join(missing))
    archives = [t["task_id"] for t in descriptor["tasks"] if t["stage"] == "archive"]
    if not archives:
        raise RuntimeError("completed child has no durable archive")
    for identity in archives:
        _receipt(root, state, identity)


@_entry
def complete_source(run_dir, source_index, return_code=0):
    if int(return_code) != 0:
        return _answer(1 if int(return_code) == 75 else int(return_code),
                       reason="source child failed", worker_return_code=int(return_code))
    root, _ = _paths(run_dir)
    index = int(source_index)
    descriptor = read_json(_manifest_path(root, source_name(index)))
    with staging.transaction(root) as state:
        _require_all(root, state, descriptor)
        state["planner"]["sources"][str(index)] = "completed"
    return _answer(status="completed")


@_entry
def complete_shard(run_dir, campaign, shard_index, return_code=0):
    if int(return_code) != 0:
        return _answer(1 if int(return_code) == 75 else int(return_code),
                       reason="campaign child failed", worker_return_code=int(return_code))
    root, spec = _paths(run_dir)
    name = shard_name(campaign, shard_index)
    descriptor = read_json(_manifest_path(root, name))
    with staging.transaction(root) as state:
        _require_all(root, state, descriptor)
        state["allocations"][name]["completed"] = True
        # Completion order is independent of allocation order. Only a contiguous
        # finished prefix may advance the physical safe cursors.
        entries = sorted((value["start_job_index"], key, value)
                         for key, value in state["allocations"].items() if value["campaign"] == campaign)
        completed_nodes = 0
        for start, key, entry in entries:
            if start != completed_nodes or not entry["completed"]:
                break
            manifest = read_json(_manifest_path(root, key))["allocation"]
            for stream, cursor in manifest["end_cursors"].items():
                state["planner"]["streams"][stream]["safe_cursor"] = cursor
            completed_nodes += entry["node_count"]
        config = next(c for c in spec["campaigns"] if c["name"] == campaign)
        closed = completed_nodes == int(config["max_processing_nodes"])
        for pool in {s["pool"] for s in config["sources"]}:
            state["planner"]["streams"][stream_key(campaign, pool)]["closed"] = closed
    return _answer(status="completed")


def _pending_cleanup_urls(state):
    return {url for value in state["cleanup_claims"].values() if not value.get("completed", False)
            for url in value.get("urls", [])}


def _cleanup_refs(root, spec, state, pool):
    blocks, refs, _ = _inventory(root, spec, state)
    streams = [state["planner"]["streams"][stream_key(c["name"], pool)]
               for c in spec["campaigns"] if any(s["pool"] == pool for s in c["sources"])]
    archived_sources = set()
    for index, source in enumerate(spec["sources"]):
        if source["pool"] == pool and f"SOURCE_ARCHIVE_{index:06d}" in state["accepted"]:
            archived_sources.add(source["source_id"])
    pending = _pending_cleanup_urls(state)
    objects = state["staging"]["objects"]
    result = []
    for block in blocks.get(pool, []):
        if (block.source_id not in archived_sources
                or any(may_still_need(block.index, stream) for stream in streams)):
            continue
        ref = refs[block.artifact_id]
        physical = objects.get(ref["url"])
        if physical and physical["resident"] and ref["url"] not in pending:
            result.append(deepcopy(ref))
    return result


@_entry
def prepare_cleanup(run_dir, pool, slot):
    root, spec = _paths(run_dir)
    name = cleanup_name(pool, slot)
    # Each slot has one source/shard trigger, plus one terminal slot per pool.
    count = sum(s["pool"] == pool for s in spec["sources"]) + sum(
        any(s["pool"] == pool for s in next(c for c in spec["campaigns"] if c["name"] == campaign)["sources"])
        for campaign, _, _, _ in shard_ranges(spec))
    if pool not in {s["pool"] for s in spec["sources"]} or not 0 <= int(slot) <= count:
        raise ValueError("unknown pool cleanup slot")
    with staging.transaction(root) as state:
        previous = state["cleanup_claims"].get(name)
        if previous and previous.get("skipped"):
            return _answer(99, status="empty cleanup")
        if previous is None:
            refs = _cleanup_refs(root, spec, state, pool)
            if not refs:
                state["cleanup_claims"][name] = {"skipped": True, "completed": True, "urls": []}
                return _answer(99, status="empty cleanup")
            task = _reclaim(spec, name, refs)
            descriptor = {"tasks": [task], "dependencies": [], "claims": {}, "pool": pool}
            _freeze(root, name, descriptor)
            # Exact frozen set and pending references have the same commit marker.
            state["cleanup_claims"][name] = {"completed": False,
                "urls": [r["url"] for r in _physical_refs(state, task["inputs"])]}
    descriptor = read_json(_manifest_path(root, name))
    task = descriptor["tasks"][0]
    write_immutable_json(record_path(root, "tasks", name), task)
    return _answer(status="prepared", task_id=name)


def committed_archives(run_dir, state=None):
    root = Path(run_dir)
    state = read_json(root / "state.json") if state is None else state
    return [_receipt(root, state, identity) for identity in sorted(state["accepted"])
            if state["accepted"][identity]["stage"] == "archive"]


def committed_groups(run_dir, state=None):
    root, spec = _paths(run_dir)
    state = read_json(root / "state.json") if state is None else state
    return load_groups(root, spec, state["merge_group_counts"])


@_entry
def finalize_run(run_dir):
    root, spec = _paths(run_dir)
    with staging.transaction(root) as state:
        if state["status"] != "complete":
            if any(state["planner"]["sources"].get(str(i)) not in {"completed", "skipped"}
                   for i in range(len(spec["sources"]))):
                raise RuntimeError("finalizer requires every finite source slot completed or skipped")
            expected = {shard_name(c, i) for c, i, _, _ in shard_ranges(spec)}
            if (set(state["allocations"]) != expected
                    or any(not a["completed"] for a in state["allocations"].values())):
                raise RuntimeError("finalizer requires all campaign shards completed")
            if any(not value["completed"] for value in state["cleanup_claims"].values()):
                raise RuntimeError("finalizer requires all frozen cleanup operations completed")
            if state["staging"]["resident_bytes"] or state["staging"]["claims"]:
                raise RuntimeError("finalizer requires all run-owned staging reclaimed")
            ordered = []
            for identity, marker in state["accepted"].items():
                if marker["stage"] != "miniaod":
                    continue
                task = read_json(record_path(root, "tasks", identity))
                p = task["parameters"]
                ordered.append((p["campaign"], p["detector_profile_id"],
                                int(p["job_id"].rsplit("BLOCK", 1)[1]),
                                _receipt(root, state, identity)["outputs"][0]))
            groups_state = {"merge_buffers": {}, "merge_group_counts": {}}
            for campaign, profile, _, ref in sorted(ordered):
                groups_state["merge_buffers"].setdefault(sample_key(campaign, profile), []).append(ref)
            # Immutable groups first, state counters and status next, sentinel last.
            for campaign, profile, ordinal, group in legacy._merge_groups(spec, groups_state, flush=True):
                write_group(root, campaign, profile, ordinal, group)
            state["merge_group_counts"] = groups_state["merge_group_counts"]
            state["status"] = "complete"
    (root / ".complete").touch()
    return _answer(status="complete", merge_groups=sum(state["merge_group_counts"].values()))
