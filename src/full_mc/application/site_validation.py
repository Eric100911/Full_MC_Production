"""Local evidence required before submitting the isolated shard workflow."""
from pathlib import Path

from full_mc.io import read_json


PROOF_SCHEMA = "cern-afs-lock-proof-v1"
REQUIRED_CHECKS = (
    "mutual_exclusion", "read_after_unlock", "crash_recovery", "pre_defer",
    "pre_skip", "dynamic_subdag", "autorescue",
)


def validate_site(spec):
    """Read a G3 evidence record; never query or mutate a site here.

    The record is supplied after an authorized test, not manufactured by run
    planning. A new run directory is not evidence of an independent quota.
    """
    if spec.get("orchestration") != "campaign_shards":
        return
    validation = spec.get("site_validation", {})
    if not isinstance(validation, dict):
        raise RuntimeError("site_validation must be an object")
    storage = spec.get("storage", {})
    usable = int(storage.get("staging_bytes", 0)) - int(storage.get("reserve_bytes", 0))
    isolated = validation.get("isolated_budget_bytes")
    if (type(isolated) is not int or isolated <= 0 or isolated != usable):
        raise RuntimeError("campaign_shards requires an explicitly isolated staging budget "
                           "matching staging_bytes minus reserve_bytes")
    proof_path = validation.get("proof_path")
    if not proof_path or not Path(proof_path).is_absolute():
        raise RuntimeError("campaign_shards requires an absolute G3 proof_path before submission")
    proof = read_json(proof_path)
    if not isinstance(proof, dict) or proof.get("schema") != PROOF_SCHEMA:
        raise RuntimeError("unsupported CERN AFS/DAGMan validation record")
    schedds = proof.get("schedds", [])
    if (not isinstance(schedds, list) or any(not isinstance(s, str) or not s.strip()
                                           for s in schedds)
            or len(set(schedds)) < 2):
        raise RuntimeError("G3 evidence must identify at least two different schedds")
    checks = proof.get("checks", {})
    if not isinstance(checks, dict) or any(checks.get(key) is not True
                                         for key in REQUIRED_CHECKS):
        raise RuntimeError("G3 AFS/DAGMan checks have not all passed")
    evidence = proof.get("evidence", [])
    if (not isinstance(evidence, list) or not evidence
            or any(not isinstance(path, str) or not Path(path).is_file()
                   for path in evidence)):
        raise RuntimeError("G3 evidence must reference existing local test records")
