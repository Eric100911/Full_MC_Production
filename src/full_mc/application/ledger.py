"""Immutable merge-group records and their committed ordinal counts."""
from pathlib import Path

from full_mc.io import read_json, write_immutable_json
from full_mc.planning.graph import token

LEDGER_DIRECTORY = "ledger"
GROUP_STAGE = "combined"


def _padded(value, what):
    number = int(value)
    if number < 0:
        raise ValueError(f"{what} must be nonnegative")
    return f"{number:06d}"


def group_path(root, campaign, profile, ordinal):
    return (Path(root) / LEDGER_DIRECTORY / "groups" / token(campaign)
            / token(profile) / (_padded(ordinal, "ordinal") + ".json"))


def sample_key(campaign, profile):
    """Counter key for one campaign/detector-profile merge stream."""
    return f"{campaign}::{profile}"


def write_group(root, campaign, profile, ordinal, group):
    """Write one combined task specification verbatim as its own immutable file."""
    path = group_path(root, campaign, profile, ordinal)
    write_immutable_json(path, group)
    return path


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
