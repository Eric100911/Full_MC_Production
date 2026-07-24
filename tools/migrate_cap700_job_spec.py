#!/usr/bin/env python3
"""Build exact MC_v4_1 campaign job specs from cap700 artifacts and inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


CAMPAIGNS = {
    "JJP_DPS1_MC_v4_1": "JJP_DPS1",
    "JJP_TPS_MC_v4_1": "JJP_TPS",
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cap700-dir", required=True, type=Path)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--full-output", required=True, type=Path)
    parser.add_argument("--pilot-output", required=True, type=Path)
    args = parser.parse_args()

    inventory = read_json(args.inventory)
    by_path = {
        pool: {item["path"]: item for item in data["files"]}
        for pool, data in inventory["pools"].items()
    }
    planning_dir = args.cap700_dir / "node_configs" / "planning"
    full = {
        "version": 1,
        "description": "Exact cap700 exposure-complete recovery mapping.",
        "source_inventory": str(args.inventory),
        "source_inventory_sha256": file_sha256(args.inventory),
        "cap700_metadata_sha256": file_sha256(args.cap700_dir / "metadata.json"),
        "reproducibility_scheme": "cap700-v1",
        "campaigns": {
            name: {"physics_campaign": physics_campaign}
            for name, physics_campaign in CAMPAIGNS.items()
        },
        "compact_jobs": {
            "legacy_outer_indices": [],
            "job_ids": [],
            "pool_seeds": {
                "pool_jpsi_CSCO_g": [],
                "pool_gg": [],
            },
        },
    }
    seen_job_ids = set()
    for legacy_index in range(950):
        plans = {
            "pool_jpsi_CSCO_g": read_json(
                planning_dir / f"PLAN_JpsiG_CSCO_{legacy_index}.json"
            ),
            "pool_gg": read_json(planning_dir / f"PLAN_GG_{legacy_index}.json"),
        }
        seeds = {}
        for pool_name, plan in plans.items():
            path = plan["lhe_paths"][0]
            item = by_path.get(pool_name, {}).get(path)
            if item is None or int(item.get("actual_events") or 0) < 700:
                raise SystemExit(f"invalid counted inventory entry for {path}")
            expected_legacy_seed = (100 if pool_name == "pool_jpsi_CSCO_g" else 40100) + legacy_index
            expected_plan = {
                "primary_seed": expected_legacy_seed,
                "group_id": str(expected_legacy_seed),
                "events_per_block": 350,
                "max_events_per_plan": 700,
                "shuffle_mode": "stratified",
                "n_strata": "auto",
                "drop_incomplete_last_block": False,
            }
            for key, expected in expected_plan.items():
                if plan.get(key) != expected:
                    raise SystemExit(
                        f"unexpected cap700 {key} for {path}: "
                        f"{plan.get(key)!r} != {expected!r}"
                    )
            if (
                plan.get("lhe_event_counts")
                and plan["lhe_event_counts"] != [int(item["actual_events"])]
            ):
                raise SystemExit(f"cap700 inventory count mismatch for {path}")
            if int(plan["shuffle_seed"]) != expected_legacy_seed * 1000 + 37:
                raise SystemExit(f"unexpected cap700 shuffle seed for {path}")
            seeds[pool_name] = int(item["seed"])
        job_id = seeds["pool_jpsi_CSCO_g"]
        if job_id in seen_job_ids:
            raise SystemExit(f"duplicate inventory job ID: {job_id}")
        seen_job_ids.add(job_id)
        full["compact_jobs"]["legacy_outer_indices"].append(legacy_index)
        full["compact_jobs"]["job_ids"].append(job_id)
        for pool_name, seed in seeds.items():
            full["compact_jobs"]["pool_seeds"][pool_name].append(seed)

    pilot_positions = [round(index * 949 / 9) for index in range(10)]
    pilot = {key: value for key, value in full.items() if key != "compact_jobs"}
    pilot["description"] = "Ten evenly spaced cap700 recovery pilot records."
    pilot["pilot_manifest_positions"] = pilot_positions
    pilot["compact_jobs"] = {
        "legacy_outer_indices": [
            full["compact_jobs"]["legacy_outer_indices"][position]
            for position in pilot_positions
        ],
        "job_ids": [full["compact_jobs"]["job_ids"][position] for position in pilot_positions],
        "pool_seeds": {
            pool: [values[position] for position in pilot_positions]
            for pool, values in full["compact_jobs"]["pool_seeds"].items()
        },
    }
    for path, payload in ((args.full_output, full), (args.pilot_output, pilot)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
