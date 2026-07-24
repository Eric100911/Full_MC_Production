#!/usr/bin/env python3
"""Validate exact, nonconsecutive recovery job specifications."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
TMP_ROOT = Path("/tmp/chiw")
PILOT_IDS = [100, 1095, 1191, 1287, 1383, 1479, 1575, 1670, 1767, 1862]
PILOT_POSITIONS = [0, 105, 211, 316, 422, 527, 633, 738, 844, 949]
PILOT_GG_SEEDS = [40100, 40205, 40311, 40416, 40522, 40627, 40733, 40838, 40944, 41049]

sys.path.insert(0, str(BASE_DIR))
from dag_generator import (  # noqa: E402
    deterministic_seed,
    load_campaign_job_spec,
    load_campaign_planning_spec,
)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    full = read_json(BASE_DIR / "common/campaign_job_specs/jjp_mc_v4_1_full.json")
    pilot = read_json(BASE_DIR / "common/campaign_job_specs/jjp_mc_v4_1_pilot.json")
    full_jobs = full["compact_jobs"]
    pilot_jobs = pilot["compact_jobs"]
    planning = load_campaign_planning_spec(
        str(
            BASE_DIR
            / "common/campaign_job_specs/jjp_efficiency_balanced_pilot_v2.json"
        ),
        ["JJP_DPS1_MC_v4_1", "JJP_TPS_MC_v4_1"],
        str(BASE_DIR / "generated/lhe_inventory_jjp_20260717_184812.json"),
    )
    assert planning is not None
    assert planning["planning"]["block_events"] == 1000
    assert planning["campaigns"]["JJP_DPS1_MC_v4_1"]["source_lhe_budgets"] == [
        860,
        1000,
    ]
    assert planning["campaigns"]["JJP_TPS_MC_v4_1"]["source_lhe_budgets"] == [
        1000,
        1000,
        977,
    ]
    assert planning["campaigns"]["JJP_DPS1_MC_v4_1"]["processing_nodes"] == 10
    assert planning["campaigns"]["JJP_TPS_MC_v4_1"]["processing_nodes"] == 10
    assert planning["storage"]["hard_limit_bytes"] == 5_000_000_000_000
    assert planning["storage"]["capacity_status"] == "within_warning_limit"

    assert len(full_jobs["job_ids"]) == 950
    assert len(set(full_jobs["job_ids"])) == 950
    assert full_jobs["legacy_outer_indices"] == list(range(950))
    assert not all(
        right == left + 1
        for left, right in zip(full_jobs["job_ids"], full_jobs["job_ids"][1:])
    )
    assert pilot["pilot_manifest_positions"] == PILOT_POSITIONS
    assert pilot_jobs["legacy_outer_indices"] == PILOT_POSITIONS
    assert pilot_jobs["job_ids"] == PILOT_IDS
    assert pilot_jobs["pool_seeds"]["pool_gg"] == PILOT_GG_SEEDS
    assert pilot_jobs["job_ids"] == [
        full_jobs["job_ids"][position] for position in PILOT_POSITIONS
    ]

    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="campaign_spec_", dir=TMP_ROOT) as tmp:
        root = Path(tmp)
        inventory_path = root / "inventory.json"
        inventory = {
            "pools": {
                "pool_jpsi_CSCO_g": {
                    "files": [{
                        "path": "root://example///store/jpsi_1095.lhe.gz",
                        "seed": 1095,
                        "actual_events": 83479,
                    }]
                },
                "pool_gg": {
                    "files": [{
                        "path": "root://example///store/gg_40205.lhe.gz",
                        "seed": 40205,
                        "actual_events": 24756,
                    }]
                },
            }
        }
        inventory_path.write_text(json.dumps(inventory) + "\n", encoding="utf-8")
        digest = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
        spec_path = root / "spec.json"
        spec = {
            "version": 1,
            "source_inventory_sha256": digest,
            "campaigns": {
                "JJP_DPS1_MC_v4_1": {"physics_campaign": "JJP_DPS1"},
                "JJP_TPS_MC_v4_1": {"physics_campaign": "JJP_TPS"},
            },
            "compact_jobs": {
                "legacy_outer_indices": [105],
                "job_ids": [1095],
                "pool_seeds": {
                    "pool_jpsi_CSCO_g": [1095],
                    "pool_gg": [40205],
                },
            },
        }
        spec_path.write_text(json.dumps(spec) + "\n", encoding="utf-8")
        loaded = load_campaign_job_spec(
            str(spec_path),
            ["JJP_DPS1_MC_v4_1", "JJP_TPS_MC_v4_1"],
            str(inventory_path),
        )
        dps = loaded["JJP_DPS1_MC_v4_1"][0]
        tps = loaded["JJP_TPS_MC_v4_1"][0]
        assert dps["job_id"] == 1095
        assert dps["pools"]["pool_jpsi_CSCO_g"]["shuffle_seed"] == 205037
        assert tps["pools"]["pool_gg"]["shuffle_seed"] == 40205037
        expected_input = "BLOCK:pool_jpsi_CSCO_g:205:000000"
        assert dps["source_rng_seeds"][0] == deterministic_seed(
            f"JJP_DPS1|JOB000105_BLOCK000000|source=0|{expected_input}|normal"
        )

        inventory["pools"]["pool_gg"]["files"][0]["actual_events"] = 1
        inventory_path.write_text(json.dumps(inventory) + "\n", encoding="utf-8")
        try:
            load_campaign_job_spec(
                str(spec_path), ["JJP_TPS_MC_v4_1"], str(inventory_path)
            )
        except ValueError as exc:
            assert "inventory hash mismatch" in str(exc)
        else:
            raise AssertionError("changed inventory was not rejected")

    print("[OK] Recovery specs preserve 950 exact nonconsecutive inventory IDs")
    print("[OK] Pilot selects the locked ten manifest positions and pool seeds")
    print("[OK] Compact specs require the authoritative inventory hash")
    print("[OK] V2 spec freezes efficiency budgets, global blocks, and storage gate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
