"""Validate frozen selections against an exact counted LHE inventory."""
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
