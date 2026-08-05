#!/usr/bin/env python3
"""Regression tests for post-planner campaign/shard allocation."""

import json
import subprocess
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL = REPO_ROOT / "tools" / "allocate_campaign_shards.py"
TMP_ROOT = Path("/tmp/chiw")


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def main() -> int:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="allocate_campaign_shards_",
        dir=TMP_ROOT,
    ) as temporary:
        root = Path(temporary)
        source_entries = []
        manifests = {
            "normal_a": ("normal", [1000, 137, 63]),
            "normal_b": ("normal", [1000, 800]),
            "phi_a": ("phi", [977, 23, 954]),
        }
        for name, (pool, block_events) in manifests.items():
            manifest_path = root / f"{name}.json"
            write_json(
                manifest_path,
                {
                    "blocks": [
                        {"block_index": index, "n_events": events}
                        for index, events in enumerate(block_events)
                    ]
                },
            )
            source_entries.append({
                "pool": pool,
                "path": str(manifest_path),
            })

        source_path = root / "source_manifests.json"
        output_path = root / "allocation.json"
        config_path = root / "config.json"
        write_json(source_path, source_entries)
        write_json(
            config_path,
            {
                "source_manifests_path": str(source_path),
                "output_path": str(output_path),
                "shard_processing_nodes": 1,
                "campaign_allocation_order": ["TPS", "DPS"],
                "campaigns": {
                    "TPS": {
                        "campaign_inputs": ["normal", "normal", "phi"],
                        "source_lhe_budgets": [1000, 137, 977],
                        "max_processing_nodes": 1,
                        "expected_shards": 1,
                    },
                    "DPS": {
                        "campaign_inputs": ["normal", "normal"],
                        "source_lhe_budgets": [800, 800],
                        "max_processing_nodes": 0,
                        "expected_shards": 1,
                    },
                },
            },
        )

        subprocess.run(
            ["python3", str(TOOL), "--config", str(config_path)],
            check=True,
        )
        allocation = json.loads(output_path.read_text(encoding="utf-8"))
        tps = allocation["campaigns"]["TPS"]
        dps = allocation["campaigns"]["DPS"]

        # TPS consumes two distinct normal blocks from the shared pool.
        assert tps["pool_start_blocks"] == {"normal": 0, "phi": 0}
        assert tps["pool_end_blocks"] == {"normal": 2, "phi": 1}
        assert tps["processing_nodes"] == 1

        # DPS starts exactly where TPS stopped. Its first slot needs two
        # partial blocks (63 + 1000) to cross the 800-event budget.
        assert dps["pool_start_blocks"] == {"normal": 2}
        assert dps["pool_end_blocks"] == {"normal": 5}
        assert dps["processing_nodes"] == 1
        assert dps["planned_blocks_by_slot"] == [2, 1]
        assert dps["shards"][0]["pool_start_blocks"] == {"normal": 2}
        assert dps["shards"][0]["pool_end_blocks"] == {"normal": 5}

    print("Campaign shard allocation tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
