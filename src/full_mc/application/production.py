"""Single-root bounded production, from source preparation to archival handoff."""
from collections import defaultdict
from copy import deepcopy
import hashlib
import math
from pathlib import Path
import subprocess

from full_mc.domain.allocation import allocate_wave
from full_mc.domain.inventory import SourceBlock
from full_mc.domain.models import Campaign, CampaignSource
from full_mc.domain.identity import shuffle_seed
from full_mc.credentials import resolve_proxy_path
from full_mc.io import atomic_json, read_json
from full_mc.planning.graph import write_root_dag, write_submit_files, write_tasks_dag, token
from full_mc.runtime.packages import validate_runtime_release
from full_mc.runtime.profiles import STAGE_RUNTIME
from full_mc.settings import load_run_spec
from full_mc.storage.archive import restore_members
from full_mc.storage.transfer import TransferClient, artifact_ready


def _campaigns(spec):
    return [Campaign(c["name"], c["analysis"], [
        CampaignSource(s["pool"], s["mode"], int(s["lhe_budget"]),
                       float(s.get("efficiency", 1))) for s in c["sources"]],
        int(c["target_mixed_events"]), int(c["max_processing_nodes"]),
        int(c["merge_events"])) for c in spec["campaigns"]]


def _task(spec, identity, stage, inputs, outputs, parameters):
    return {"task_id": token(identity), "stage": stage, "runtime": STAGE_RUNTIME[stage],
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
        if source.get("generation"):
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
    required = {STAGE_RUNTIME[s] for s in ("plan", "mix", "miniaod", "archive")}
    if any(s.get("generation") for s in spec["sources"]):
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
    # Bound lazy phases without expanding any per-worker source inventory.
    steps = len(spec["sources"]) + sum(c.max_processing_nodes for c in campaigns) + 1
    atomic_json(root / "state.json", {
        "status": "planned", "next_step": 0, "pending": None,
        "prepared_sources": [], "blocks": {}, "pool_cursors": {},
        "job_offsets": {}, "resident": {}, "originals": [],
        "merge_buffers": {}, "groups": [], "archives": []})
    write_submit_files(root, spec["core_root"], runtime_dir, proxy_path)
    write_root_dag(root, spec, steps, proxy_path)
    prepared = advance_wave(root, 0)
    return {"run_id": spec["run_id"], "run_dir": str(root), "dag": str(root / "run.dag"),
            "runtime_release": spec["runtime_release"], "source_files": len(spec["sources"]),
            "max_processing_nodes": sum(c.max_processing_nodes for c in campaigns),
            "lazy_steps": steps, "first_phase": prepared["phase"], "submitted": False}


def _blocks(state):
    return {pool: [SourceBlock(ref["artifact_id"], pool, ref["source_id"], index,
                                int(ref["events"]), ref["url"])
                   for index, ref in enumerate(refs)]
            for pool, refs in state["blocks"].items()}


def _allocation(spec, state):
    usable = int(spec["storage"]["staging_bytes"]) - int(spec["storage"]["reserve_bytes"])
    fitting = (usable - _used_bytes(state)) // spec["limits"]["mixed_hepmc_bytes"]
    limit = min(spec["limits"]["processing_nodes_per_wave"], max(1, fitting))
    return allocate_wave(_campaigns(spec), _blocks(state), state["pool_cursors"],
                         limit, state["job_offsets"])


def _used_bytes(state):
    return sum(int(ref["size_bytes"]) for ref in state["resident"].values())


def _check_bytes(spec, state, requested):
    usable = int(spec["storage"]["staging_bytes"]) - int(spec["storage"]["reserve_bytes"])
    if _used_bytes(state) + requested > usable:
        raise RuntimeError("bounded wave cannot fit CERN staging; retain state and adjust explicit limits")


def _source_phase(root, step, spec, state):
    needed = defaultdict(int)
    blocks = _blocks(state)
    from full_mc.domain.allocation import _take_budget
    for campaign in _campaigns(spec):
        if state["job_offsets"].get(campaign.name, 0) >= campaign.max_processing_nodes:
            continue
        cursors = dict(state["pool_cursors"])
        for slot in campaign.sources:
            picked = _take_budget(blocks.get(slot.pool, []), cursors.get(slot.pool, 0), slot.lhe_budget)
            if picked is None:
                needed[slot.pool] += slot.lhe_budget
            else:
                cursors[slot.pool] = picked[0]
    selected = []
    for index, source in enumerate(spec["sources"]):
        if index in state["prepared_sources"] or needed[source["pool"]] <= 0:
            continue
        selected.append(index)
        hint = source.get("actual_events", source.get("generation", {}).get("max_events", 0))
        # Only a preparation hint; allocation later uses the splitter's actual count.
        needed[source["pool"]] -= int(hint)
        if len(selected) == spec["limits"]["source_files_per_step"]:
            break
    if not selected:
        raise RuntimeError("selected inventory exhausted before the finite campaign targets were met")
    limits = spec["limits"]
    per_source = limits["source_lhe_bytes"] + limits["source_block_bytes"]
    usable = int(spec["storage"]["staging_bytes"]) - int(spec["storage"]["reserve_bytes"])
    fit = (usable - _used_bytes(state)) // per_source
    selected = selected[:max(0, fit)]
    if not selected:
        raise RuntimeError("no additional source fits the available CERN staging budget")
    tasks, dependencies, records = [], [], []
    for index in selected:
        source = spec["sources"][index]
        base = f"{_staging(spec)}/sources/{source['pool']}/{source['source_id']}"
        original = _ref(f"LHE_{index:06d}", base + "/source.lhe.gz", "lhe")
        generation = source.get("generation")
        if generation:
            lhe = _task(spec, f"LHE_{index:06d}", "lhe", [], [original], dict(
                generation, pool_name=source["pool"], seed=int(source["seed"]),
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
            "task_ids": [t["task_id"] for t in tasks], "dag": str(path)}


def _process_phase(root, step, spec, state, allocation):
    tasks, dependencies, mini_records, mixed = [], [], [], []
    index = {ref["artifact_id"]: ref for refs in state["blocks"].values() for ref in refs}
    campaigns = {c["name"]: c for c in spec["campaigns"]}
    count = sum(len(c["nodes"]) for c in allocation["campaigns"])
    _check_bytes(spec, state, count * spec["limits"]["mixed_hepmc_bytes"])
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
            product = _ref("MINIAOD_" + identity,
                spec["storage"]["ihep_products"].rstrip("/") + "/" + spec["run_id"]
                + "/" + campaign["name"] + "/" + node["job_id"] + ".root", "miniaod")
            mini = _task(spec, "MINIAOD_" + identity, "miniaod", [hepmc], [product], parameters)
            tasks += [mix, mini]
            dependencies.append(([mix["task_id"]], mini["task_id"]))
            mini_records.append({"campaign": campaign["name"], "task_id": mini["task_id"]})
            mixed.append(hepmc)
    archive_inputs = [state["resident"][key] for key in state["originals"]] + mixed
    archive_ref = _ref(f"ARCHIVE_{step:06d}", spec["storage"]["ihep_archive"].rstrip("/")
                       + "/" + spec["run_id"] + f"/wave_{step:06d}.tar", "upstream-archive")
    archive = _task(spec, f"ARCHIVE_{step:06d}", "archive", archive_inputs, [archive_ref], {
        "component_miniaods": [task["outputs"][0] for task in tasks if task["stage"] == "miniaod"],
        "scratch_bytes": 3 * (_used_bytes(state) + count * spec["limits"]["mixed_hepmc_bytes"])})
    tasks.append(archive)
    dependencies.append(([r["task_id"] for r in mini_records], archive["task_id"]))
    path = write_tasks_dag(root, step, tasks, dependencies, spec)
    return {"step": step, "phase": "processing", "allocation": allocation,
            "task_ids": [t["task_id"] for t in tasks], "miniaods": mini_records,
            "mixed": mixed, "archive_task": archive["task_id"], "dag": str(path)}


def _result(root, identity, spec, expected_stage):
    outcome = read_json(root / "results" / (identity + ".json"))
    if (outcome.get("task_id") != identity or outcome.get("status") != "ok"
            or outcome.get("runtime_release") != spec["runtime_release"]
            or outcome.get("stage") != expected_stage):
        raise RuntimeError(f"worker result is not successful for this release: {identity}")
    return outcome


def _validate_planner_result(root, spec, item, outcome, client):
    """Verify a planner result against the immutable task written for its source."""
    task = read_json(root / "tasks" / (item["plan_task"] + ".json"))
    source = spec["sources"][item["source_index"]]
    parameters = task["parameters"]
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
        events = ref.get("events")
        if (ref.get("kind") != "lhe-block" or not isinstance(events, int)
                or isinstance(events, bool) or events <= 0
                or events > int(parameters["events_per_block"])
                or not str(ref.get("url", "")).startswith(
                    str(parameters["block_output_dir"]).rstrip("/") + "/")
                or not str(ref.get("url", "")).endswith(".lhe.gz")
                or int(ref.get("size_bytes", 0)) <= 0
                or client.size(ref["url"]) != int(ref["size_bytes"])):
            raise RuntimeError("planner block result fails the frozen-task contract")
        if index + 1 < len(refs) and events != int(parameters["events_per_block"]):
            raise RuntimeError("only the final planner block may be partial")
        planned += events
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
    if (original.get("events") != actual_input_events or int(original.get("size_bytes", 0)) <= 0
            or client.size(original["url"]) != int(original["size_bytes"])):
        raise RuntimeError("planner original fails count or size verification")


def _validate_archive_result(root, spec, pending, outcome, client):
    task = read_json(root / "tasks" / (pending["archive_task"] + ".json"))
    outputs = outcome.get("outputs")
    if not isinstance(outputs, list) or len(outputs) != 1 or len(task["outputs"]) != 1:
        raise RuntimeError("archive result must publish exactly its declared archive")
    for key in ("artifact_id", "url", "kind"):
        if outputs[0].get(key) != task["outputs"][0].get(key):
            raise RuntimeError("archive output does not match its frozen task")
    if not artifact_ready(outputs[0], client):
        raise RuntimeError("upstream archive is not verified")
    members = outcome.get("members")
    expected = {ref["artifact_id"]: ref for ref in task["inputs"]}
    if not isinstance(members, list) or {item.get("artifact_id") for item in members} != set(expected):
        raise RuntimeError("archive membership does not match its frozen task")
    seen_names = set()
    for item in members:
        reference = expected[item["artifact_id"]]
        name = item.get("member")
        if (not name or Path(name).name != name or name in seen_names
                or any(item.get(key) != reference.get(key) for key in ("url", "kind"))
                or ("events" in reference and item.get("events") != reference["events"])
                or int(item.get("size_bytes", 0)) <= 0
                or client.size(reference["url"]) != int(item["size_bytes"])):
            raise RuntimeError("archive member result fails the frozen-task contract")
        seen_names.add(name)


def _reclaim_staging(spec, state, refs, client):
    """Delete only exact, run-owned artifacts; unchanged state makes retries safe."""
    seen = set()
    for ref in refs:
        identity = ref["artifact_id"]
        if identity in seen:
            continue
        seen.add(identity)
        if not ref["url"].startswith(_staging(spec) + "/"):
            raise ValueError("refusing to reclaim an object outside this run's staging")
        for url in (ref["url"], ref.get("sidecar_url", ref["url"] + ".json")):
            try:
                client.remove(url)
            except FileNotFoundError:
                pass
        state["resident"].pop(identity, None)


def _terminal_cleanup(spec, state, client):
    """Reclaim unused staged blocks only after their retained originals are archived."""
    archived = {member["artifact_id"] for archive in state["archives"]
                for member in archive["members"]}
    originals = {(source["pool"], source["source_id"]): "LHE_%06d" % index
                 for index, source in enumerate(spec["sources"])}
    remaining = list(state["resident"].values())
    for ref in remaining:
        if ref.get("kind") == "lhe-block":
            source = (ref.get("pool"), ref.get("source_id"))
            if originals.get(source) not in archived:
                raise RuntimeError("unused block cannot be reclaimed before its original is archived")
    _reclaim_staging(spec, state, remaining, client)
    state["blocks"] = {pool: [] for pool in state["blocks"]}
    state["terminal_cleanup"] = sorted(ref["artifact_id"] for ref in remaining)


def restore_archived_sources(run_dir, archive_id, destination, artifact_ids=None):
    """Restore recorded original LHE members for an operator recovery or replan."""
    root = Path(run_dir).resolve()
    state = read_json(root / "state.json")
    archive = next((item for item in state.get("archives", [])
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


def _combined(spec, campaign, refs, ordinal):
    name = campaign["name"]
    identity = f"{name}_MERGE{ordinal:06d}"
    base = spec["storage"]["ihep_products"].rstrip("/") + "/" + spec["run_id"] + "/" + name
    parameters = {"campaign": name, "analysis": campaign["analysis"], "merge_group": identity}
    outputs = {
        "merge": _ref(identity + "_merged", base + "/merged/" + identity + ".root", "miniaod"),
        "ntuple": _ref(identity + "_raw", base + "/ntuples/" + identity + ".root", "raw-ntuple"),
        "selection": _ref(identity + "_selected", base + "/selected/" + identity + ".root", "selected-ntuple"),
        "efficiency": _ref(identity + "_efficiency", base + "/efficiency/" + identity + ".tar", "efficiency-shard")}
    if campaign["analysis"] == "JUP":
        outputs = {key: value for key, value in outputs.items() if key in ("merge", "ntuple")}
    subtasks = {stage: _task(spec, identity + "_" + stage, stage, refs if stage == "merge" else [],
                            [output], dict(parameters)) for stage, output in outputs.items()}
    return {"task_id": identity, "stage": "combined", "runtime_release": spec["runtime_release"],
            "log_level": spec["log_level"], "subtasks": subtasks}


def _merge_groups(spec, state, flush=False):
    for campaign in spec["campaigns"]:
        name = campaign["name"]
        buffer = state["merge_buffers"].setdefault(name, [])
        while buffer:
            total, end = 0, 0
            while end < len(buffer) and total < int(campaign["merge_events"]):
                total += int(buffer[end]["events"])
                end += 1
            if total < int(campaign["merge_events"]) and not flush:
                break
            ordinal = sum(g["subtasks"]["merge"]["parameters"]["campaign"] == name
                          for g in state["groups"])
            state["groups"].append(_combined(spec, campaign, buffer[:end], ordinal))
            del buffer[:end]


def _write_groups(root, spec, state):
    atomic_json(root / "merge_groups.json", {
        "run_id": spec["run_id"], "runtime_release": spec["runtime_release"],
        "core_revision": spec["core_revision"], "groups": state["groups"]})


def _finish_pending(root, spec, state):
    pending = state["pending"]
    client = TransferClient()
    if pending["phase"] == "sources":
        for item in pending["records"]:
            source = spec["sources"][item["source_index"]]
            outcome = _result(root, item["plan_task"], spec, "plan")
            _validate_planner_result(root, spec, item, outcome, client)
            pool = state["blocks"].setdefault(source["pool"], [])
            for ref in outcome["outputs"]:
                ref = dict(ref, pool=source["pool"], source_id=source["source_id"])
                if ref["artifact_id"] in state["resident"]:
                    raise RuntimeError("planner result reuses an existing staged block identity")
                pool.append(ref)
                state["resident"][ref["artifact_id"]] = ref
            original = dict(outcome["originals"][0], pool=source["pool"], source_id=source["source_id"])
            if original["artifact_id"] in state["resident"] or original["artifact_id"] in state["originals"]:
                raise RuntimeError("planner result reuses an existing retained original identity")
            state["resident"][original["artifact_id"]] = original
            state["originals"].append(original["artifact_id"])
            state["prepared_sources"].append(item["source_index"])
        _check_bytes(spec, state, 0)
    else:
        archive = _result(root, pending["archive_task"], spec, "archive")
        _validate_archive_result(root, spec, pending, archive, client)
        for item in pending["miniaods"]:
            output = _result(root, item["task_id"], spec, "miniaod")["outputs"][0]
            if not artifact_ready(output, client) or int(output.get("events", 0)) <= 0:
                raise RuntimeError("component MiniAOD is not verified")
            state["merge_buffers"].setdefault(item["campaign"], []).append(output)
        allocation = pending["allocation"]
        remove = [state["resident"][key] for key in state["originals"]]
        for pool, cursor in allocation["pool_cursors_after"].items():
            before = state["pool_cursors"].get(pool, 0)
            remove.extend(state["blocks"].get(pool, [])[before:cursor])
        remove += pending["mixed"]
        _reclaim_staging(spec, state, remove, client)
        state["pool_cursors"] = allocation["pool_cursors_after"]
        state["job_offsets"] = allocation["job_offsets_after"]
        state["originals"] = []
        state["archives"].append({"outputs": archive["outputs"], "members": archive["members"]})
        _merge_groups(spec, state)
    state["next_step"] = pending["step"] + 1
    state["pending"] = None
    atomic_json(root / "state.json", state)
    _write_groups(root, spec, state)

def advance_wave(run_dir, step=None):
    root = Path(run_dir).resolve()
    spec, state = read_json(root / "run.json"), read_json(root / "state.json")
    _write_groups(root, spec, state)
    requested = state["next_step"] if step is None else int(step)
    if requested < state["next_step"]:
        return {"skip": True, "status": state["status"]}
    if state["pending"]:
        if requested == state["pending"]["step"]:
            return {"phase": state["pending"]["phase"], "dag": state["pending"]["dag"],
                    "status": "prepared"}
        if requested != state["pending"]["step"] + 1:
            raise ValueError("wave advancement must be sequential")
        _finish_pending(root, spec, state)
    elif requested != state["next_step"]:
        raise ValueError("wave advancement must be sequential")
    complete = all(state["job_offsets"].get(c["name"], 0) >= int(c["max_processing_nodes"])
                   for c in spec["campaigns"])
    if complete:
        _merge_groups(spec, state, flush=True)
        _terminal_cleanup(spec, state, TransferClient())
        state["status"] = "complete"
        atomic_json(root / "state.json", state)
        _write_groups(root, spec, state)
        (root / ".complete").touch()
        return {"skip": True, "status": "complete", "merge_groups": len(state["groups"])}
    allocation = _allocation(spec, state)
    if any(group["nodes"] for group in allocation["campaigns"]):
        pending = _process_phase(root, requested, spec, state, allocation)
    else:
        pending = _source_phase(root, requested, spec, state)
    state["pending"], state["status"] = pending, "running"
    atomic_json(root / "state.json", state)
    return {"phase": pending["phase"], "dag": pending["dag"], "status": "prepared"}
