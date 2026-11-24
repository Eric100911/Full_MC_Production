"""Validate source inventories and worker results against frozen identities."""
import hashlib
from pathlib import Path

from full_mc.io import read_json


def validate_source_inventory(spec):
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


def validate_planner_result(task, source, outcome):
    """Verify a planner result against the immutable task written for its source."""
    parameters = task["parameters"]
    validate_result_envelope(task, outcome, task["runtime_release"])
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


def validate_archive_result(task, outcome):
    """Validate the exact frozen archive membership."""
    validate_result_envelope(task, outcome, task["runtime_release"])
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


def validate_reclaim_result(task, outcome):
    """Validate the exact frozen reclaim membership."""
    validate_result_envelope(task, outcome, task["runtime_release"])
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
