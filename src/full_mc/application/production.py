"""Single-root bounded production, from source preparation to archival handoff.

``state.json`` is bounded by the configured wave width.  Everything that grows
with the run - planned sources, verified archives, completed merge groups,
folded waves - is an immutable record under ``ledger/``, read back by position.
The PRE seam therefore never contacts EOS: it validates the local records a
producer already published, commits the previous wave, and prepares the next.
"""
from collections import defaultdict
from copy import deepcopy
import hashlib
import math
from pathlib import Path
import subprocess

from full_mc.application.ledger import (STATE_SCHEMA, archive_record, inventory_record,
                                        load_archives, load_inventory,
                                        load_wave, sample_key, wave_path, wave_receipt,
                                        write_archive, write_group, write_inventory,
                                        write_wave)
from full_mc.domain.allocation import allocate_wave
from full_mc.domain.inventory import SourceBlock
from full_mc.domain.models import Campaign, CampaignSource
from full_mc.domain.identity import shuffle_seed
from full_mc.credentials import resolve_proxy_path
from full_mc.io import atomic_json, read_json
from full_mc.planning.graph import write_root_dag, write_submit_files, write_tasks_dag, token
from full_mc.runtime.packages import validate_runtime_release
from full_mc.runtime.profiles import STAGE_RUNTIME, runtime_for_profile
from full_mc.settings import load_run_spec
from full_mc.storage.archive import restore_members
# A pure URL predicate, not a storage client: the seam must be able to tell
# run-owned XRootD staging from a local path without opening a connection.
from full_mc.storage.transfer import is_remote


def _campaigns(spec):
    return [Campaign(c["name"], c["analysis"], [
        CampaignSource(s["pool"], s["mode"], int(s["lhe_budget"]),
                       float(s.get("efficiency", 1))) for s in c["sources"]],
        int(c["target_mixed_events"]), int(c["max_processing_nodes"]),
        int(c["merge_events"]),
        [profile["profile_id"] for profile in c["resolved_detector_variants"]],
        list(c["resolved_detector_variants"])) for c in spec["campaigns"]]


def _task(spec, identity, stage, inputs, outputs, parameters):
    profile = parameters.get("detector_profile")
    if profile and stage in {"miniaod", "merge", "ntuple"}:
        role = {"miniaod": "simulation", "merge": "merge", "ntuple": "ntuple"}[stage]
        runtime = runtime_for_profile(profile, role)
    else:
        runtime = STAGE_RUNTIME[stage]
    return {"task_id": token(identity), "stage": stage, "runtime": runtime,
            "runtime_release": spec["runtime_release"], "log_level": spec["log_level"],
            "inputs": inputs, "outputs": outputs, "parameters": parameters}


def _ref(identity, url, kind, **extra):
    return dict(artifact_id=identity, url=url, kind=kind, **extra)


def _staging(spec):
    return spec["storage"]["cern_staging"].rstrip("/") + "/" + spec["run_id"]


def _authority(spec):
    if not spec.get("inventory_path"):
        return
    path = Path(spec["inventory_path"])
    expected = spec.get("inventory_sha256")
    if not expected or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ValueError("authoritative inventory checksum is missing or mismatched")
    inventory = read_json(path)
    if not inventory.get("complete"):
        raise ValueError("authoritative inventory is not complete")
    for source in spec["sources"]:
        if source.get("generator_profile"):
            continue
        files = inventory["pools"][source["pool"]]["files"]
        match = next((item for item in files if item["path"] == source["url"]), None)
        if (not match or match.get("status") != "ok"
                or int(match["actual_events"]) != int(source["actual_events"])
                or int(match["seed"]) != int(source["seed"])):
            raise ValueError(f"source disagrees with counted inventory: {source['source_id']}")


def plan_run(spec_path, output_dir, log_level=None):
    spec = load_run_spec(spec_path)
    if log_level:
        spec["log_level"] = log_level
    proxy_path = resolve_proxy_path()
    _authority(spec)
    limits = spec.setdefault("limits", {})
    for key in ("source_lhe_bytes", "source_block_bytes", "mixed_hepmc_bytes"):
        if int(limits.get(key, 0)) <= 0:
            raise ValueError(f"limits.{key} must be a positive output byte ceiling")
        limits[key] = int(limits[key])
    for key, default in (("source_files_per_step", 4), ("processing_nodes_per_wave", 10)):
        limits[key] = int(limits.get(key, default))
        if limits[key] <= 0:
            raise ValueError(f"limits.{key} must be positive")
    for source in spec["sources"]:
        token(source["pool"])
        token(source["source_id"])
        if int(source.get("seed", 0)) <= 0:
            raise ValueError("each source requires its explicit seed")
    campaigns = _campaigns(spec)
    known = {s["pool"] for s in spec["sources"]}
    if any(slot.pool not in known for c in campaigns for slot in c.sources):
        raise ValueError("campaign uses an undeclared source pool")
    runtime_dir = spec.get("runtime_dir")
    if not runtime_dir:
        raise ValueError("runtime_dir must identify a prepared reusable CERN runtime")
    runtime = validate_runtime_release(runtime_dir, expected_release=spec["runtime_release"],
                                       expected_site="cern")
    expected_revision = str(spec.get("core_revision", ""))
    if expected_revision and expected_revision != runtime["git_revision"]:
        raise ValueError("supplied core_revision does not match the prepared runtime")
    required = {STAGE_RUNTIME[s] for s in ("plan", "mix", "archive")}
    required.update(runtime_for_profile(profile, "simulation")
                    for campaign in spec["campaigns"]
                    for profile in campaign["resolved_detector_variants"])
    if any(s.get("generator_profile") for s in spec["sources"]):
        required.add(STAGE_RUNTIME["lhe"])
    if not required <= set(runtime["profiles"]):
        raise ValueError("runtime release lacks required worker profiles")
    spec["core_revision"] = runtime["git_revision"]
    spec["runtime_source_dirty"] = bool(runtime.get("source_dirty", False))
    controller = Path(runtime_dir) / "controller/core"
    if not (controller / "mcprod").is_file():
        raise ValueError("runtime release lacks its frozen submit-host controller")
    spec["core_root"] = str(controller.resolve())
    root = Path(output_dir).resolve()
    if root.exists() and any(root.iterdir()):
        raise FileExistsError("run directory is not empty; use the existing run for recovery")
    root.mkdir(parents=True, exist_ok=True)
    atomic_json(root / "run.json", spec)
    # Bound lazy phases without expanding any per-worker source inventory.  The
    # two trailing slots are structural rather than slack: a wave's PRE folds
    # wave i-1 and prepares wave i, so even the last real wave needs a successor
    # to fold it, and the terminal reclaim wave is prepared into a slot that
    # needs that successor in turn.  One slot short of this and the root DAG
    # runs every wave it declares, reports success, and never writes .complete.
    # Unused slots are not wasted: their PRE sees .complete and skips.
    steps = _root_step_count(spec, campaigns)
    atomic_json(root / "state.json", {
        "state_schema": STATE_SCHEMA, "status": "planned", "next_step": 0,
        "pending": {}, "pool_cursors": {}, "job_offsets": {},
        "merge_buffers": {}, "merge_group_counts": {}})
    write_submit_files(root, spec["core_root"], runtime_dir, proxy_path)
    write_root_dag(root, spec, steps, proxy_path)
    prepared = advance_wave(root, 0)
    return {"run_id": spec["run_id"], "run_dir": str(root), "dag": str(root / "run.dag"),
            "runtime_release": spec["runtime_release"], "source_files": len(spec["sources"]),
            "max_processing_nodes": sum(c.max_processing_nodes for c in campaigns),
            "lazy_steps": steps, "first_phase": prepared["phase"], "submitted": False}


def _root_step_count(spec, campaigns):
    """Use a compact bound only when explicit byte ceilings prove it safe."""
    sources = len(spec["sources"])
    nodes = sum(c.max_processing_nodes for c in campaigns)
    fallback = sources + nodes + 2
    limits = spec["limits"]
    if (limits["source_selection_mode"] != "prefetch"
            or sources > limits["source_files_per_step"]):
        return fallback
    usable = int(spec["storage"]["staging_bytes"]) - int(spec["storage"]["reserve_bytes"])
    source_upper = sources * (limits["source_lhe_bytes"] + limits["source_block_bytes"])
    mixed_upper = nodes * limits["mixed_hepmc_bytes"]
    if source_upper + mixed_upper > usable:
        return fallback
    archive_disk = int(spec["resources"]["archive"]["disk_bytes"])
    archive_cap = (archive_disk - 3 * sources * limits["source_lhe_bytes"]) // (
        3 * limits["mixed_hepmc_bytes"])
    if archive_cap <= 0:
        return fallback
    per_wave = min(limits["processing_nodes_per_wave"], archive_cap)
    # One source wave, enough processing waves, then two fold/reclaim slots.
    return 1 + (nodes + per_wave - 1) // per_wave + 2


def _committed(root, spec, state):
    """Committed inventory records, at or below this state's commit marker."""
    return load_inventory(root, spec, state["next_step"])


def _archived_ids(root, spec, state):
    """Artifact IDs a committed archive receipt proves durable."""
    return {member["artifact_id"]
            for receipt in load_archives(root, spec, state["next_step"])
            for member in receipt.get("members", [])}


def _staging_view(root, spec, state):
    """Ledger-derived view of the objects still occupying run-owned CERN staging.

    Blocks come from committed PLAN records in run-spec source order, which is
    the order the planner recorded.  Only blocks below a pool cursor (already
    consumed) and originals inside a committed archive receipt stop counting;
    no reader here contacts EOS.
    """
    blocks, resident, originals, prepared = {}, {}, [], set()
    archived = _archived_ids(root, spec, state)
    for record in _committed(root, spec, state):
        pool, source_id = record["pool"], record["source_id"]
        prepared.add(record["source_index"])
        for ref in record["producer_result"]["outputs"]:
            blocks.setdefault(pool, []).append(dict(ref, pool=pool, source_id=source_id))
        for ref in record["producer_result"]["originals"]:
            item = dict(ref, pool=pool, source_id=source_id)
            if item["artifact_id"] in archived:
                continue
            resident[item["artifact_id"]] = item
            originals.append(item["artifact_id"])
    for pool, refs in blocks.items():
        for ref in refs[int(state["pool_cursors"].get(pool, 0)):]:
            resident[ref["artifact_id"]] = ref
    return {"blocks": blocks, "resident": resident, "originals": originals,
            "prepared": prepared}


def _blocks(view):
    return {pool: [SourceBlock(ref["artifact_id"], pool, ref["source_id"], index,
                               int(ref["events"]), ref["url"])
                   for index, ref in enumerate(refs)]
            for pool, refs in view["blocks"].items()}


def _allocation(spec, state, view):
    usable = int(spec["storage"]["staging_bytes"]) - int(spec["storage"]["reserve_bytes"])
    mixed_bytes = spec["limits"]["mixed_hepmc_bytes"]
    fitting = (usable - _used_bytes(view)) // mixed_bytes
    limit = min(spec["limits"]["processing_nodes_per_wave"], fitting)
    archive_resource = spec.get("resources", {}).get("archive", {})
    if "disk_bytes" in archive_resource:
        originals = sum(int(view["resident"][key]["size_bytes"])
                        for key in view["originals"])
        archive_fit = (int(archive_resource["disk_bytes"]) - 3 * originals) // (3 * mixed_bytes)
        limit = min(limit, archive_fit)
    if limit <= 0 and any(view["blocks"].values()):
        raise RuntimeError("no processing node fits the staging and archive disk budgets")
    return allocate_wave(_campaigns(spec), _blocks(view), state["pool_cursors"],
                         max(0, limit), state["job_offsets"])


def _used_bytes(view):
    return sum(int(ref["size_bytes"]) for ref in view["resident"].values())


def _check_bytes(spec, view, requested):
    usable = int(spec["storage"]["staging_bytes"]) - int(spec["storage"]["reserve_bytes"])
    if _used_bytes(view) + requested > usable:
        raise RuntimeError("bounded wave cannot fit CERN staging; retain state and adjust explicit limits")


def _reclaim_refs(refs):
    """Exact staged objects a reclaim task may delete, deduplicated by identity.

    Only an explicitly recorded ``sidecar_url`` travels with a reference: PLAN
    blocks publish no EOS sidecar, so manufacturing ``url + ".json"`` would
    delete an object this run never staged.
    """
    seen, values = set(), []
    for ref in refs:
        identity = ref["artifact_id"]
        if identity in seen:
            continue
        seen.add(identity)
        item = {key: ref[key] for key in ("artifact_id", "url", "kind") if key in ref}
        if "sidecar_url" in ref:
            item["sidecar_url"] = ref["sidecar_url"]
        values.append(item)
    return values


def _staging_refs(refs):
    """Require every reclaim reference to use the production XRootD contract."""
    refs = list(refs)
    non_remote = [str(ref.get("url", "")) for ref in refs
                  if not is_remote(ref.get("url", ""))]
    if non_remote:
        raise RuntimeError(
            "production staging reference is not an XRootD URL: "
            + ", ".join(non_remote)
        )
    return refs


def _reclaim_task(spec, step, refs):
    """One reclaim task for exactly the references the seam recorded."""
    return _task(spec, f"RECLAIM_{step:06d}", "reclaim", refs, [],
                 {"staging_prefix": _staging(spec)})


def _source_phase(root, step, spec, state, view):
    mode = spec["limits"].get("source_selection_mode", "demand")
    needed = defaultdict(int)
    if mode == "demand":
        blocks = _blocks(view)
        from full_mc.domain.allocation import _take_budget
        for campaign in _campaigns(spec):
            if state["job_offsets"].get(campaign.name, 0) >= campaign.max_processing_nodes:
                continue
            cursors = dict(state["pool_cursors"])
            for slot in campaign.sources:
                picked = _take_budget(blocks.get(slot.pool, []), cursors.get(slot.pool, 0),
                                      slot.lhe_budget)
                if picked is None:
                    needed[slot.pool] += slot.lhe_budget
                else:
                    cursors[slot.pool] = picked[0]
    active_pools = {slot.pool for campaign in _campaigns(spec)
                    if state["job_offsets"].get(campaign.name, 0) < campaign.max_processing_nodes
                    for slot in campaign.sources}
    selected = []
    for index, source in enumerate(spec["sources"]):
        if index in view["prepared"]:
            continue
        if mode == "prefetch":
            if source["pool"] not in active_pools:
                continue
        elif needed[source["pool"]] <= 0:
            continue
        selected.append(index)
        if mode == "demand":
            profile = source.get("resolved_generator_profile", {})
            hint = source.get("actual_events", profile.get("planning_events_hint", 0))
            # Only a preparation hint; allocation uses the splitter's actual count.
            needed[source["pool"]] -= int(hint)
        if len(selected) == spec["limits"]["source_files_per_step"]:
            break
    if not selected:
        raise RuntimeError("selected inventory exhausted before the finite campaign targets were met")
    limits = spec["limits"]
    per_source = limits["source_lhe_bytes"] + limits["source_block_bytes"]
    usable = int(spec["storage"]["staging_bytes"]) - int(spec["storage"]["reserve_bytes"])
    fit = (usable - _used_bytes(view)) // per_source
    selected = selected[:max(0, fit)]
    if not selected:
        raise RuntimeError("no additional source fits the available CERN staging budget")
    tasks, dependencies, records = [], [], []
    for index in selected:
        source = spec["sources"][index]
        base = f"{_staging(spec)}/sources/{source['pool']}/{source['source_id']}"
        generation = source.get("resolved_generator_profile")
        original_extra = ({"generator_profile": deepcopy(generation)}
                          if generation else {})
        original = _ref(f"LHE_{index:06d}", base + "/source.lhe.gz", "lhe",
                        **original_extra)
        if generation:
            lhe = _task(spec, f"LHE_{index:06d}", "lhe", [], [original], dict(
                generator_profile=generation, pool_name=source["pool"], seed=int(source["seed"]),
                max_output_bytes=limits["source_lhe_bytes"],
                scratch_bytes=limits["source_lhe_bytes"] * 4))
            tasks.append(lhe)
            inputs = [original]
        else:
            inputs = [_ref(f"INPUT_{index:06d}", source["url"], "lhe",
                           events=int(source["actual_events"]))]
        planner = _task(spec, f"PLAN_{index:06d}", "plan", inputs, [], {
            "pool_name": source["pool"], "primary_seed": int(source["seed"]),
            "group_id": source["source_id"], "events_per_block": spec["block_events"],
            "shuffle_seed": shuffle_seed(int(source["seed"])),
            "max_events_per_plan": int(source.get("planned_events", 0) or 0),
            "block_output_dir": base + "/blocks", "original_output": original,
            "original_max_bytes": limits["source_lhe_bytes"],
            "max_output_bytes": limits["source_block_bytes"],
            "scratch_bytes": per_source * 4})
        tasks.append(planner)
        if generation:
            dependencies.append(([lhe["task_id"]], planner["task_id"]))
        records.append({"source_index": index, "plan_task": planner["task_id"]})
    path = write_tasks_dag(root, step, tasks, dependencies, spec)
    return {"step": step, "phase": "sources", "records": records,
            "task_ids": [t["task_id"] for t in tasks], "reclaim": [], "dag": str(path)}


def _process_phase(root, step, spec, view, allocation):
    tasks, dependencies, mini_records, mixed = [], [], [], []
    index = {ref["artifact_id"]: ref for refs in view["blocks"].values() for ref in refs}
    campaigns = {c["name"]: c for c in spec["campaigns"]}
    count = sum(len(c["nodes"]) for c in allocation["campaigns"])
    _check_bytes(spec, view, count * spec["limits"]["mixed_hepmc_bytes"])
    for group in allocation["campaigns"]:
        campaign = campaigns[group["campaign"]]
        for node in group["nodes"]:
            identity = campaign["name"] + "_" + node["job_id"]
            inputs, slots = [], []
            for slot in node["sources"]:
                refs = [index[key] for key in slot["block_refs"]]
                inputs += [{k: v for k, v in ref.items() if k != "source_id"} for ref in refs]
                slots.append({"slot": slot["slot"], "mode": slot["mode"],
                    "inputs": slot["block_refs"], "rng_seed": slot["source_rng_seed"],
                    "max_lhe_events": slot["lhe_events"],
                    "target_hepmc_events": node["target_mixed_events"],
                    "max_hadronization_retries": int(campaign["sources"][slot["slot"]].get(
                        "max_hadronization_retries", 5000))})
            parameters = {"campaign": campaign["name"], "analysis": campaign["analysis"],
                "job_id": node["job_id"], "target_mixed_events": node["target_mixed_events"],
                "edm_event_id": node["edm_event_id"],
                "normal_shortfall_policy": spec["normal_shortfall_policy"],
                "unused_hepmc_warning_fraction": spec["unused_hepmc_warning_fraction"],
                "processing_environment": spec["processing_environment"]}
            hepmc = _ref("HEPMC_" + identity, _staging(spec) + "/mixed/" + identity + ".hepmc.gz", "hepmc")
            mix = _task(spec, "MIX_" + identity, "mix", inputs, [hepmc],
                dict(parameters, sources=slots, mixing_rng_seed=node["mixing_rng_seed"],
                     max_output_bytes=spec["limits"]["mixed_hepmc_bytes"],
                     scratch_bytes=spec["limits"]["mixed_hepmc_bytes"] * 10))
            tasks.append(mix)
            for profile in campaign["resolved_detector_variants"]:
                variant = profile["profile_id"]
                detector_identity = f"{campaign['name']}_{variant}_{node['job_id']}"
                detector_parameters = dict(
                    parameters,
                    detector_profile_id=variant,
                    detector_profile=deepcopy(profile),
                    simulation_runtime=runtime_for_profile(profile, "simulation"),
                    merge_runtime=runtime_for_profile(profile, "merge"),
                    ntuple_runtime=runtime_for_profile(profile, "ntuple"),
                )
                product = _ref(
                    "MINIAOD_" + detector_identity,
                    spec["storage"]["ihep_products"].rstrip("/") + "/" + spec["run_id"]
                    + "/" + campaign["name"] + "/" + variant + "/" + node["job_id"] + ".root",
                    "miniaod", detector_profile_id=variant,
                    detector_profile=deepcopy(profile),
                )
                mini = _task(spec, "MINIAOD_" + detector_identity, "miniaod",
                             [hepmc], [product], detector_parameters)
                tasks.append(mini)
                dependencies.append(([mix["task_id"]], mini["task_id"]))
                mini_records.append({"campaign": campaign["name"],
                                     "detector_profile_id": variant,
                                     "detector_profile": deepcopy(profile),
                                     "mix_task_id": mix["task_id"],
                                     "job_id": node["job_id"],
                                     "task_id": mini["task_id"]})
            mixed.append(hepmc)
    archive_inputs = [view["resident"][key] for key in view["originals"]] + mixed
    original_bytes = sum(int(view["resident"][key]["size_bytes"])
                         for key in view["originals"])
    archive_scratch = 3 * (original_bytes + count * spec["limits"]["mixed_hepmc_bytes"])
    archive_disk = spec.get("resources", {}).get("archive", {}).get("disk_bytes")
    if archive_disk is not None and archive_scratch > int(archive_disk):
        raise RuntimeError("archive scratch exceeds its requested disk budget")
    archive_ref = _ref(f"ARCHIVE_{step:06d}", spec["storage"]["ihep_archive"].rstrip("/")
                       + "/" + spec["run_id"] + f"/wave_{step:06d}.tar", "upstream-archive")
    archive = _task(spec, f"ARCHIVE_{step:06d}", "archive", archive_inputs, [archive_ref], {
        "component_miniaods": [task["outputs"][0] for task in tasks if task["stage"] == "miniaod"],
        "scratch_bytes": archive_scratch})
    tasks.append(archive)
    dependencies.append(([r["task_id"] for r in mini_records], archive["task_id"]))
    # Deleting these is a separate worker task; recording them here keeps the
    # exact reference set a retried wave must delete in the wave receipt.
    deletable = [view["resident"][key] for key in view["originals"]]
    for pool, cursor in allocation.get("pool_cursors_after", {}).items():
        before = allocation.get("pool_cursors_before", {}).get(pool, 0)
        deletable.extend(view["blocks"].get(pool, [])[before:cursor])
    deletable.extend(mixed)
    reclaim = _reclaim_refs(deletable)
    reclaim_task = None
    reclaim_refs = _staging_refs(reclaim)
    if reclaim_refs:
        reclaim_task = _reclaim_task(spec, step, reclaim_refs)
        tasks.append(reclaim_task)
        dependencies.append(([archive["task_id"]], reclaim_task["task_id"]))
    path = write_tasks_dag(root, step, tasks, dependencies, spec)
    return {"step": step, "phase": "processing", "allocation": allocation,
            "task_ids": [t["task_id"] for t in tasks], "miniaods": mini_records,
            "mixed": mixed, "archive_task": archive["task_id"],
            "reclaim": reclaim, "reclaim_task": reclaim_task and reclaim_task["task_id"],
            "dag": str(path)}


def validate_result_envelope(task, outcome, runtime_release):
    """Structural check that a worker result belongs to its frozen task.

    The seam has no storage client: a producer already verified its upload, so
    only local consistency can be checked here.
    """
    if outcome.get("task_id") != task["task_id"]:
        raise RuntimeError("worker result has the wrong task ID")
    if outcome.get("stage") != task["stage"]:
        raise RuntimeError("worker result has the wrong stage")
    if outcome.get("status") != "ok":
        raise RuntimeError("worker result is not successful")
    if outcome.get("runtime_release") != runtime_release:
        raise RuntimeError("worker result has the wrong runtime release")


def validate_planner_ref(ref, source, parameters, index):
    """One staged block, checked against its frozen PLAN task and source."""
    expected_id = f"BLOCK:{source['pool']}:{source['source_id']}:{index:06d}"
    expected_prefix = str(parameters["block_output_dir"]).rstrip("/") + "/"
    events = ref.get("events")
    if ref.get("artifact_id") != expected_id:
        raise RuntimeError("planner block identity is invalid")
    if ref.get("kind") != "lhe-block":
        raise RuntimeError("planner output is not an LHE block")
    if not str(ref.get("url", "")).startswith(expected_prefix):
        raise RuntimeError("planner block URL is outside its output directory")
    if not str(ref.get("url", "")).endswith(".lhe.gz"):
        raise RuntimeError("planner block is not compressed LHE")
    if not isinstance(events, int) or isinstance(events, bool) or events <= 0:
        raise RuntimeError("planner block event count is invalid")
    if events > int(parameters["events_per_block"]):
        raise RuntimeError("planner block exceeds the configured block size")
    if int(ref.get("size_bytes", 0)) <= 0:
        raise RuntimeError("planner block has no declared byte size")


def _validate_planner_result(root, spec, item, outcome):
    """Verify a planner result against the immutable task written for its source."""
    task = read_json(root / "tasks" / (item["plan_task"] + ".json"))
    source = spec["sources"][item["source_index"]]
    parameters = task["parameters"]
    validate_result_envelope(task, outcome, spec["runtime_release"])
    declared_counts = [ref.get("events") for ref in task["inputs"]]
    input_events = sum(int(value) for value in declared_counts) if all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in declared_counts) else None
    actual_input_events = outcome.get("actual_input_events")
    if (task.get("stage") != "plan" or parameters.get("pool_name") != source["pool"]
            or parameters.get("group_id") != source["source_id"]
            or not isinstance(actual_input_events, int) or isinstance(actual_input_events, bool)
            or actual_input_events <= 0
            or (input_events is not None and actual_input_events != input_events)):
        raise RuntimeError("planner result disagrees with its frozen source task")
    refs = outcome.get("outputs")
    limit = int(parameters.get("max_events_per_plan", 0)) or actual_input_events
    if not isinstance(refs, list) or not refs:
        raise RuntimeError("planner result has no block outputs")
    expected_ids = ["BLOCK:%s:%s:%06d" % (source["pool"], source["source_id"], index)
                    for index in range(len(refs))]
    if [ref.get("artifact_id") for ref in refs] != expected_ids:
        raise RuntimeError("planner block identities do not match the frozen source")
    planned = 0
    for index, ref in enumerate(refs):
        validate_planner_ref(ref, source, parameters, index)
        if index + 1 < len(refs) and ref["events"] != int(parameters["events_per_block"]):
            raise RuntimeError("only the final planner block may be partial")
        planned += ref["events"]
    if planned != int(outcome.get("planned_events", -1)) or planned > limit:
        raise RuntimeError("planner result has an invalid planned event total")
    originals = outcome.get("originals")
    expected_original = parameters.get("original_output")
    if not isinstance(originals, list) or len(originals) != 1 or not isinstance(expected_original, dict):
        raise RuntimeError("planner result misses its retained source original")
    original = originals[0]
    for key in ("artifact_id", "url", "kind"):
        if original.get(key) != expected_original.get(key):
            raise RuntimeError("planner original does not match the frozen source task")
    if original.get("events") != actual_input_events or int(original.get("size_bytes", 0)) <= 0:
        raise RuntimeError("planner original fails count or size verification")


def _validate_archive_result(root, spec, pending, outcome):
    """Verify an archive result against its frozen task, locally.

    The archive worker already validated producer metadata and the downloaded
    member size before verifying the published archive. The local PRE seam
    repeats only structural checks: member identity, URL, kind, event count and
    declared size still have to match the frozen archive inputs. PRE does not
    remotely re-stat the HepMC.
    """
    task = read_json(root / "tasks" / (pending["archive_task"] + ".json"))
    validate_result_envelope(task, outcome, spec["runtime_release"])
    outputs = outcome.get("outputs")
    if not isinstance(outputs, list) or len(outputs) != 1 or len(task["outputs"]) != 1:
        raise RuntimeError("archive result must publish exactly its declared archive")
    for key in ("artifact_id", "url", "kind"):
        if outputs[0].get(key) != task["outputs"][0].get(key):
            raise RuntimeError("archive output does not match its frozen task")
    members = outcome.get("members")
    expected = {ref["artifact_id"]: ref for ref in task["inputs"]}
    if (not isinstance(members, list) or len(members) != len(expected)
            or {item.get("artifact_id") for item in members} != set(expected)):
        raise RuntimeError("archive membership does not match its frozen task")
    seen_names = set()
    for item in members:
        reference = expected[item["artifact_id"]]
        name = item.get("member")
        if (not name or Path(name).name != name or name in seen_names
                or any(item.get(key) != reference.get(key) for key in ("url", "kind"))
                or ("events" in reference and item.get("events") != reference["events"])
                or int(item.get("size_bytes", 0)) <= 0
                or ("size_bytes" in reference
                    and int(item["size_bytes"]) != int(reference["size_bytes"]))):
            raise RuntimeError("archive member result fails the frozen-task contract")
        seen_names.add(name)


def _validate_reclaim_result(root, spec, pending, outcome):
    """Every reference the reclaim task declared has to be accounted for.

    The seam advances pool cursors and job offsets only on this result: a wave
    whose staged deletions are unconfirmed is not a consumed wave, so a retry
    after a partial deletion replays it instead of moving on.
    """
    task = read_json(root / "tasks" / (pending["reclaim_task"] + ".json"))
    validate_result_envelope(task, outcome, spec["runtime_release"])
    entries = outcome.get("reclaimed")
    if not isinstance(entries, list):
        raise RuntimeError("reclaim result has no reclaimed objects")
    reported = {}
    for entry in entries:
        identity = entry.get("artifact_id")
        if not isinstance(identity, str) or identity in reported:
            raise RuntimeError("reclaim result reports an unknown or repeated artifact")
        reported[identity] = entry
    declared = {ref["artifact_id"]: ref for ref in task["inputs"]}
    if set(reported) != set(declared):
        raise RuntimeError("reclaim result does not account for every declared reference")
    for identity, entry in reported.items():
        expected = {("payload", str(declared[identity]["url"]))}
        if "sidecar_url" in declared[identity]:
            expected.add(("sidecar", str(declared[identity]["sidecar_url"])))
        objects = entry.get("objects")
        if not isinstance(objects, list):
            raise RuntimeError("reclaim result has no objects for " + identity)
        seen = set()
        for item in objects:
            key = (item.get("kind"), str(item.get("url")))
            if key not in expected or key in seen:
                raise RuntimeError("reclaim result objects disagree with " + identity)
            if item.get("disposition") not in ("removed", "already-missing"):
                raise RuntimeError("reclaim result has an unknown disposition")
            seen.add(key)
        if seen != expected:
            raise RuntimeError("reclaim result misses an object of " + identity)


def _validate_pending_results(root, spec, pending, results):
    """Every structural check the seam can make from local records alone."""
    if pending["phase"] == "sources":
        for item in pending["records"]:
            _validate_planner_result(root, spec, item, results[item["plan_task"]])
        return
    if pending["phase"] == "reclaim":
        _validate_reclaim_result(root, spec, pending, results[pending["reclaim_task"]])
        return
    _validate_archive_result(root, spec, pending, results[pending["archive_task"]])
    archived = {member["artifact_id"]: member
                for member in results[pending["archive_task"]]["members"]}
    for item in pending["miniaods"]:
        task = read_json(root / "tasks" / (item["task_id"] + ".json"))
        outcome = results[item["task_id"]]
        validate_result_envelope(task, outcome, spec["runtime_release"])
        output = outcome["outputs"][0]
        expected_profile = item["detector_profile_id"]
        hepmc = archived.get(task["inputs"][0]["artifact_id"])
        expected = hepmc.get("events") if hepmc else None
        actual = output.get("events")
        partial = output.get("partial")
        if (not isinstance(expected, int) or isinstance(expected, bool) or expected <= 0
                or not isinstance(actual, int) or isinstance(actual, bool)
                or not 0 < actual <= expected
                or output.get("expected_events") != expected
                or type(partial) is not bool or partial != (actual < expected)
                or outcome.get("partial") != partial
                or (partial and spec["normal_shortfall_policy"] != "report-and-truncate")):
            raise RuntimeError("MiniAOD event count disagrees with archived HepMC")
        if (task["parameters"].get("detector_profile_id") != expected_profile
                or output.get("detector_profile_id") != expected_profile):
            raise RuntimeError("MiniAOD provenance does not match its detector task")
    if pending.get("reclaim_task"):
        _validate_reclaim_result(root, spec, pending, results[pending["reclaim_task"]])


def _wave_results(root, spec, pending):
    """Worker results the fold consumes, from raw fragments or the wave receipt.

    Fragments are removed only once the receipt is durable, so a retry after a
    crash in that window must recover them from the receipt instead.
    """
    if pending["phase"] == "sources":
        needed = [item["plan_task"] for item in pending["records"]]
    elif pending["phase"] == "reclaim":
        needed = [pending["reclaim_task"]]
    else:
        needed = [pending["archive_task"]] + [item["task_id"] for item in pending["miniaods"]]
        if pending.get("reclaim_task"):
            needed.append(pending["reclaim_task"])
    results, missing = {}, []
    for task_id in needed:
        path = root / "results" / (task_id + ".json")
        if path.exists():
            results[task_id] = read_json(path)
        else:
            missing.append(task_id)
    if missing:
        if not wave_path(root, pending["step"]).exists():
            # Nothing was folded yet: say which worker the seam is waiting for
            # instead of blaming an absent receipt.
            raise RuntimeError("wave results are incomplete: " + ", ".join(missing))
        receipt = load_wave(root, pending["step"])
        if receipt.get("pending") != pending:
            raise RuntimeError("wave receipt does not describe the pending wave")
        for task_id in missing:
            if task_id not in receipt["results"]:
                raise RuntimeError("wave receipt misses the result for " + task_id)
            results[task_id] = receipt["results"][task_id]
    return results


def terminal_reclaim_refs(root, spec, state):
    """Staged blocks this run will never process, once their originals are archived.

    The terminal reclaim task publishes these instead of the submit host
    deleting them.  An unused block whose original is not yet inside a committed
    archive receipt is a hard error: the retained LHE has to be proven durable
    before the only other copy is removed.
    """
    archived = _archived_ids(root, spec, state)
    by_source = {(record["pool"], record["source_id"]): "LHE_%06d" % record["source_index"]
                 for record in _committed(root, spec, state)}
    candidates = []
    for pool, refs in _staging_view(root, spec, state)["blocks"].items():
        for ref in refs[int(state["pool_cursors"].get(pool, 0)):]:
            if by_source.get((ref["pool"], ref["source_id"])) not in archived:
                raise RuntimeError("unused block cannot be reclaimed before its original is archived")
            candidates.append(ref)
    return _reclaim_refs(candidates)


def restore_archived_sources(run_dir, archive_id, destination, artifact_ids=None):
    """Restore recorded original LHE members for an operator recovery or replan."""
    root = Path(run_dir).resolve()
    spec = read_json(root / "run.json")
    state = read_json(root / "state.json")
    archive = next((item for item in load_archives(root, spec, state["next_step"])
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


def _combined(spec, campaign, detector_profile, refs, ordinal):
    name = campaign["name"]
    variant = detector_profile["profile_id"]
    identity = f"{name}_{variant}_MERGE{ordinal:06d}"
    base = (spec["storage"]["ihep_products"].rstrip("/") + "/" + spec["run_id"]
            + "/" + name + "/" + variant)
    parameters = {"campaign": name, "analysis": campaign["analysis"],
                  "merge_group": identity, "sample_key": sample_key(name, variant),
                  "detector_profile_id": variant, "detector_profile": deepcopy(detector_profile),
                  "simulation_runtime": runtime_for_profile(detector_profile, "simulation"),
                  "merge_runtime": runtime_for_profile(detector_profile, "merge"),
                  "ntuple_runtime": runtime_for_profile(detector_profile, "ntuple"),
                  "mc_global_tag": detector_profile.get("mc_global_tag"),
                  "era": detector_profile.get("era")}
    provenance = {"detector_profile_id": variant,
                  "detector_profile": deepcopy(detector_profile),
                  "mc_global_tag": detector_profile.get("mc_global_tag"),
                  "era": detector_profile.get("era"),
                  "simulation_runtime": runtime_for_profile(detector_profile, "simulation"),
                  "merge_runtime": runtime_for_profile(detector_profile, "merge"),
                  "ntuple_runtime": runtime_for_profile(detector_profile, "ntuple")}
    outputs = {
        "merge": _ref(identity + "_merged", base + "/merged/" + identity + ".root", "miniaod", **provenance),
        "ntuple": _ref(identity + "_raw", base + "/ntuples/" + identity + ".root", "raw-ntuple", **provenance),
        "selection": _ref(identity + "_selected", base + "/selected/" + identity + ".root", "selected-ntuple", **provenance),
        "efficiency": _ref(identity + "_efficiency", base + "/efficiency/" + identity + ".tar", "efficiency-shard", **provenance)}
    if campaign["analysis"] == "JUP":
        outputs = {key: value for key, value in outputs.items() if key in ("merge", "ntuple")}
    subtasks = {stage: _task(spec, identity + "_" + stage, stage, refs if stage == "merge" else [],
                            [output], dict(parameters)) for stage, output in outputs.items()}
    return {"task_id": identity, "stage": "combined", "runtime_release": spec["runtime_release"],
            "log_level": spec["log_level"], "subtasks": subtasks}


def _merge_groups(spec, state, flush=False):
    """Complete merge groups as ``(campaign, profile, ordinal, group)`` records.

    Ordinals count the committed groups of one sample stream, so a retry that
    replays this function always derives the same identity for the same group.
    """
    written = []
    for campaign in spec["campaigns"]:
        name = campaign["name"]
        for detector_profile in campaign["resolved_detector_variants"]:
            variant = detector_profile["profile_id"]
            key = sample_key(name, variant)
            buffer = state["merge_buffers"].setdefault(key, [])
            ordinal = int(state["merge_group_counts"].get(key, 0))
            while buffer:
                total, end = 0, 0
                while end < len(buffer) and total < int(campaign["merge_events"]):
                    total += int(buffer[end]["events"])
                    end += 1
                if total < int(campaign["merge_events"]) and not flush:
                    break
                written.append((name, variant, ordinal,
                                _combined(spec, campaign, detector_profile, buffer[:end], ordinal)))
                del buffer[:end]
                ordinal += 1
            state["merge_group_counts"][key] = ordinal
    return written


def _finish_pending(root, spec, state):
    """Fold one prepared wave in the plan's retry-safe order.

    Every record write is create-or-compare, so an interrupted fold replays from
    wherever it stopped.  Raw result fragments are removed only after the wave
    receipt is durable, and ``state.json`` is the commit marker: a record written
    before that commit stays invisible to every reader.  A processing wave also
    has to account for its reclaim result before its cursors may advance, so a
    wave whose staged deletions are unconfirmed is replayed rather than skipped.
    """
    pending = state["pending"]
    # 1. results, from the raw fragments or (after a retry) the wave receipt
    results = _wave_results(root, spec, pending)
    _validate_pending_results(root, spec, pending, results)

    # 2. immutable inventory and archive records
    for item in pending.get("records", []):
        write_inventory(root, inventory_record(item["source_index"],
                                               spec["sources"][item["source_index"]],
                                               pending["step"], results[item["plan_task"]]))
    archive = results.get(pending.get("archive_task"))
    if archive is not None:
        write_archive(root, archive_record(pending["step"], pending["archive_task"],
                                           archive["outputs"], archive["members"]))

    # 3. bounded next state plus the merge groups this wave completes
    next_state = deepcopy(state)
    next_state["next_step"] = pending["step"] + 1
    next_state["pending"] = {}
    if pending["phase"] == "sources":
        groups = []
        _check_bytes(spec, _staging_view(root, spec, next_state), 0)
    elif pending["phase"] == "reclaim":
        # The terminal reclaim only removes leftovers of a finished run; the
        # cursors it is gated on already advanced with its processing wave.
        groups = []
    else:
        for item in pending["miniaods"]:
            next_state["merge_buffers"].setdefault(
                sample_key(item["campaign"], item["detector_profile_id"]),
                []).append(results[item["task_id"]]["outputs"][0])
        next_state["pool_cursors"] = pending["allocation"]["pool_cursors_after"]
        next_state["job_offsets"] = pending["allocation"]["job_offsets_after"]
        groups = _merge_groups(spec, next_state)

    # 4. one immutable file per completed merge group
    for campaign, profile, ordinal, group in groups:
        write_group(root, campaign, profile, ordinal, group)
    # 5. the receipt is what a retry replays once the fragments are gone
    write_wave(root, wave_receipt(pending, results, [item[3] for item in groups]))
    # 6. fragments are removed only after that receipt is durable
    for task_id in pending["task_ids"]:
        (root / "results" / (task_id + ".json")).unlink(missing_ok=True)
    # 7. the commit marker: every record above becomes visible exactly here
    atomic_json(root / "state.json", next_state)
    return next_state


def advance_wave(run_dir, step=None):
    root = Path(run_dir).resolve()
    spec, state = read_json(root / "run.json"), read_json(root / "state.json")
    if state.get("state_schema") != STATE_SCHEMA:
        raise RuntimeError("run state is not " + STATE_SCHEMA
                           + "; existing runs keep their frozen runtime and are not migrated")
    # Completion is terminal.  A rescue, a manual `internal advance-wave`, or a
    # crash between the completion commit and the sentinel write re-enters here
    # with .complete possibly missing.  Re-deriving leftover work instead would
    # re-prepare a reclaim wave that no longer has a successor to fold it, flip
    # the run back to "running", and wedge it with the sentinel never written.
    if state.get("status") == "complete":
        (root / ".complete").touch()
        return {"skip": True, "status": "complete",
                "merge_groups": sum(state.get("merge_group_counts", {}).values())}
    requested = state["next_step"] if step is None else int(step)
    if requested < state["next_step"]:
        return {"skip": True, "status": state["status"]}
    folded = state["pending"].get("phase") if state["pending"] else None
    if state["pending"]:
        if requested == state["pending"]["step"]:
            return {"phase": state["pending"]["phase"], "dag": state["pending"]["dag"],
                    "status": "prepared"}
        if requested != state["pending"]["step"] + 1:
            raise ValueError("wave advancement must be sequential")
        state = _finish_pending(root, spec, state)
    elif requested != state["next_step"]:
        raise ValueError("wave advancement must be sequential")
    complete = all(state["job_offsets"].get(c["name"], 0) >= int(c["max_processing_nodes"])
                   for c in spec["campaigns"])
    if complete:
        for campaign, profile, ordinal, group in _merge_groups(spec, state, flush=True):
            write_group(root, campaign, profile, ordinal, group)
        # A terminal reclaim still runs before completion is committed; the
        # wave that just folded was that reclaim when the spare slot was used.
        leftovers = ([] if folded == "reclaim"
                     else _staging_refs(terminal_reclaim_refs(root, spec, state)))
        if leftovers:
            reclaim = _reclaim_task(spec, requested, leftovers)
            path = write_tasks_dag(root, requested, [reclaim], [], spec)
            state["pending"] = {"step": requested, "phase": "reclaim",
                                "task_ids": [reclaim["task_id"]],
                                "reclaim_task": reclaim["task_id"], "reclaim": leftovers,
                                "dag": str(path)}
            state["status"] = "running"
            atomic_json(root / "state.json", state)
            return {"phase": "reclaim", "dag": str(path), "status": "prepared"}
        state["status"] = "complete"
        atomic_json(root / "state.json", state)
        (root / ".complete").touch()
        return {"skip": True, "status": "complete",
                "merge_groups": sum(state["merge_group_counts"].values())}
    view = _staging_view(root, spec, state)
    allocation = _allocation(spec, state, view)
    if any(group["nodes"] for group in allocation["campaigns"]):
        pending = _process_phase(root, requested, spec, view, allocation)
    else:
        pending = _source_phase(root, requested, spec, state, view)
    state["pending"], state["status"] = pending, "running"
    atomic_json(root / "state.json", state)
    return {"phase": pending["phase"], "dag": pending["dag"], "status": "prepared"}
