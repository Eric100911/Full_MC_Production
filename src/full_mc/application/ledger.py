"""Immutable local records for the bounded wave controller.

``state.json`` keeps only what the configured wave width bounds: status,
``next_step``, the pending descriptor, pool cursors, job offsets, and the
bounded merge buffers.  Everything that grows with the run - planned sources,
verified archives, completed merge groups, folded waves - is written once as an
immutable JSON file under ``ledger/`` and read back by position:

    ledger/
      inventory/<pool>/<source_id>.json
      archives/<step>.json
      groups/<campaign>/<profile>/<ordinal>.json
      waves/<step>.json

Every pool, source, campaign, and profile name used as a path component passes
the DAG token validator first.

Readers, not writers, decide committedness.  An inventory record whose
``accepted_step`` is at or above the state commit marker stays invisible, so a
record written immediately before a crash cannot be mistaken for a completed
transition.  Merge group ordinals below the counter in
``state["merge_group_counts"]`` are committed, and higher ordinals are never
returned, so handoff export cannot see a group the state has not accepted.
Archive restoration reads these receipts instead of a growing ``state`` list.

Record shapes: an inventory record wraps the PLAN producer result verbatim; an
archive receipt carries the verified archive output plus every member; a merge
group file is the combined task specification itself, so handoff export can
publish it unchanged; a wave receipt carries the pending descriptor, the
validated worker results of that wave, and the groups it wrote, which is
everything a retry needs once the raw result fragments are gone.

Every write goes through ``write_immutable_json``, which is create-or-compare:
an identical file means an earlier attempt already reached that point and a
different payload at the same path is a hard conflict.  Folding a wave is
therefore retry-safe at any point.

Public API:
    write_immutable_json(path, payload)             io.py create-or-compare write
    inventory_path(root, pool, source_id)           one source inventory record
    archive_path(root, step)                        one archive receipt
    group_path(root, campaign, profile, ordinal)    one merge group record
    wave_path(root, step)                           one wave receipt
    sample_key(campaign, profile)                   merge group counter key
    inventory_record(source_index, source, accepted_step, producer_result)
    archive_record(step, task_id, outputs, members)
    wave_receipt(pending, results, groups=())
    write_inventory(root, record)                   build-free immutable writers
    write_archive(root, record)
    write_group(root, campaign, profile, ordinal, group)
    write_wave(root, receipt)
    load_inventory(root, spec, accepted_before)     committed records, spec order
    load_archives(root, spec, step_before)          committed receipts, step order
    load_groups(root, spec, counts)                 committed groups, sample order
    load_wave(root, step)                           one wave receipt
    STATE_SCHEMA                                    "mcprod-state-v2"
"""
from pathlib import Path

from full_mc.io import read_json, write_immutable_json
from full_mc.planning.graph import token

STATE_SCHEMA = "mcprod-state-v2"
INVENTORY_SCHEMA = "mcprod-source-inventory-v1"
ARCHIVE_SCHEMA = "mcprod-archive-receipt-v1"
WAVE_SCHEMA = "mcprod-wave-receipt-v1"

# A merge group record is the combined task specification itself, not an
# envelope: handoff export must be able to publish it unchanged.
GROUP_STAGE = "combined"

LEDGER_DIRECTORY = "ledger"


def _padded(value, what):
    number = int(value)
    if number < 0:
        raise ValueError(f"{what} must be nonnegative")
    return f"{number:06d}"


def _record(path, schema):
    record = read_json(path)
    if record.get("schema") != schema:
        raise RuntimeError(f"unsupported ledger record: {path}")
    return record


def inventory_path(root, pool, source_id):
    """Path of the inventory record for one pooled source."""
    return (Path(root) / LEDGER_DIRECTORY / "inventory" / token(pool)
            / (token(source_id) + ".json"))


def archive_path(root, step):
    return Path(root) / LEDGER_DIRECTORY / "archives" / (_padded(step, "step") + ".json")


def group_path(root, campaign, profile, ordinal):
    return (Path(root) / LEDGER_DIRECTORY / "groups" / token(campaign)
            / token(profile) / (_padded(ordinal, "ordinal") + ".json"))


def wave_path(root, step):
    return Path(root) / LEDGER_DIRECTORY / "waves" / (_padded(step, "step") + ".json")


def sample_key(campaign, profile):
    """Counter key for one campaign/detector-profile merge stream."""
    return f"{campaign}::{profile}"


def inventory_record(source_index, source, accepted_step, producer_result):
    """Wrap a validated PLAN producer result without altering it."""
    return {"schema": INVENTORY_SCHEMA, "accepted_step": int(accepted_step),
            "source_index": int(source_index), "pool": token(source["pool"]),
            "source_id": token(source["source_id"]),
            "producer_result": producer_result}


def archive_record(step, task_id, outputs, members):
    """Verified upstream archive: its published output plus every member."""
    return {"schema": ARCHIVE_SCHEMA, "step": int(step), "task_id": token(task_id),
            "outputs": list(outputs), "members": list(members)}


def wave_receipt(pending, results, groups=()):
    """One folded wave: its pending descriptor, validated results, and groups."""
    return {"schema": WAVE_SCHEMA, "step": int(pending["step"]),
            "phase": pending["phase"], "pending": pending,
            "results": dict(results), "groups": list(groups)}


def write_inventory(root, record):
    path = inventory_path(root, record["pool"], record["source_id"])
    write_immutable_json(path, record)
    return path


def write_archive(root, record):
    path = archive_path(root, record["step"])
    write_immutable_json(path, record)
    return path


def write_group(root, campaign, profile, ordinal, group):
    """Write one combined task specification verbatim as its own immutable file."""
    path = group_path(root, campaign, profile, ordinal)
    write_immutable_json(path, group)
    return path


def write_wave(root, receipt):
    path = wave_path(root, receipt["step"])
    write_immutable_json(path, receipt)
    return path


def load_inventory(root, spec, accepted_before):
    """Committed inventory records, in run-spec source order.

    A record is committed only when its ``accepted_step`` is below the caller's
    state commit marker (normally ``state["next_step"]``).  The marker already
    covers the retry window, so the caller passes it instead of a step.
    """
    records = []
    for index, source in enumerate(spec["sources"]):
        path = inventory_path(root, source["pool"], source["source_id"])
        if not path.exists():
            continue
        record = _record(path, INVENTORY_SCHEMA)
        accepted = record.get("accepted_step")
        if not isinstance(accepted, int) or isinstance(accepted, bool):
            raise RuntimeError(f"inventory record has no accepted step: {path}")
        if accepted >= int(accepted_before):
            continue
        if (record.get("source_index") != index or record.get("pool") != source["pool"]
                or record.get("source_id") != source["source_id"]):
            raise RuntimeError(f"inventory record disagrees with its source: {path}")
        records.append(record)
    return records


def load_archives(root, spec, step_before):
    """Committed archive receipts in step order, for restoration.

    ``spec`` pins the run's archive area so a receipt belonging to another run
    is rejected rather than restored.
    """
    directory = Path(root) / LEDGER_DIRECTORY / "archives"
    if not directory.is_dir():
        return []
    base = str(spec.get("storage", {}).get("ihep_archive", "")).rstrip("/")
    run_id = str(spec.get("run_id", ""))
    prefix = base + "/" + run_id + "/" if base and run_id else ""
    records = []
    for path in sorted(directory.glob("*.json")):
        record = _record(path, ARCHIVE_SCHEMA)
        step = record.get("step")
        if not isinstance(step, int) or isinstance(step, bool):
            raise RuntimeError(f"archive receipt has no step: {path}")
        if path.stem != _padded(step, "step"):
            raise RuntimeError(f"archive receipt filename disagrees with its step: {path}")
        if step >= int(step_before):
            continue
        if prefix:
            for output in record.get("outputs", []):
                if not str(output.get("url", "")).startswith(prefix):
                    raise RuntimeError(f"archive receipt is outside this run: {path}")
        records.append(record)
    records.sort(key=lambda item: item["step"])
    return records


def load_groups(root, spec, counts):
    """Committed merge group specifications, in campaign, profile, ordinal order.

    ``counts`` maps a ``sample_key`` to the number of committed ordinals for
    that stream.  Ordinals at or above the count are uncommitted and are never
    returned, even when their file already exists.
    """
    records = []
    for campaign in spec["campaigns"]:
        for profile in campaign["resolved_detector_variants"]:
            variant = profile["profile_id"]
            for ordinal in range(int(counts.get(sample_key(campaign["name"], variant), 0))):
                path = group_path(root, campaign["name"], variant, ordinal)
                if not path.exists():
                    raise RuntimeError(f"committed merge group is missing: {path}")
                record = read_json(path)
                if record.get("stage") != GROUP_STAGE or not record.get("task_id"):
                    raise RuntimeError(f"unsupported merge group record: {path}")
                records.append(record)
    return records


def load_wave(root, step):
    path = wave_path(root, step)
    if not path.exists():
        raise RuntimeError(f"wave receipt is missing: {path}")
    return _record(path, WAVE_SCHEMA)
