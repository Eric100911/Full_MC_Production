"""Contracts for deterministic per-campaign source consumption streams."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.domain.inventory import SourceBlock
from full_mc.domain.models import Campaign, CampaignSource
from full_mc.domain.streams import (FinalInputShortfall, allocate_shard, build_streams,
                                    next_matching, stream_key)


def campaign(max_nodes=4, budgets=(5,), pools=("pool",)):
    sources = [CampaignSource(pool, "normal", budget, 1.0)
               for pool, budget in zip(pools, budgets)]
    return Campaign("JJP", "JJP", sources, 4, max_nodes, 100,
                    ["detector"], [{"profile_id": "detector"}])


def blocks(pool="pool", count=20, events=5):
    return [SourceBlock(f"BLOCK:{pool}:s:{index:06d}", pool, "s", index, events, "/s.lhe")
            for index in range(count)]


class StreamContracts(unittest.TestCase):
    def test_config_validation_and_default_streams(self):
        spec = {"campaigns": [{"name": "JJP", "sources": [{"pool": "pool"}]}],
                "consumption_streams": [{"campaign": "JJP", "pool": "pool",
                                         "stride": 5, "offset": 0}]}
        result = build_streams(spec)
        self.assertEqual(result[stream_key("JJP", "pool")],
                         {"stride": 5, "offset": 0, "allocation_cursor": 0})
        self.assertEqual(build_streams({"campaigns": spec["campaigns"]})[
            stream_key("JJP", "pool")]["stride"], 1)
        for bad in (True, 1.0, "2", 0, -1):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                build_streams({**spec, "consumption_streams": [
                    {"campaign": "JJP", "pool": "pool", "stride": bad}]})
        for config in ([{"campaign": "JJP", "pool": "missing"}],
                       [{"campaign": "JJP", "pool": "pool"},
                        {"campaign": "JJP", "pool": "pool"}],
                       [{"campaign": "JJP", "pool": "pool", "stride": 3, "offset": 3}]):
            with self.subTest(config=config), self.assertRaises(ValueError):
                build_streams({**spec, "consumption_streams": config})
        for config in (None, {}, "invalid", [None], ["invalid"]):
            with self.subTest(config=config), self.assertRaises(ValueError):
                build_streams({**spec, "consumption_streams": config})

    def test_variable_budgets_repeated_slots_share_cursor_and_ids_stay_stable(self):
        spec = campaign(max_nodes=2, budgets=(6, 4), pools=("pool", "pool"))
        stream_map = build_streams({"campaigns": [spec]})
        inventory = {"pool": blocks(count=8, events=3)}
        manifest = allocate_shard(spec, inventory, stream_map, 0, 1, set())
        self.assertIsNotNone(manifest)
        node = manifest["nodes"][0]
        self.assertEqual([slot["block_refs"] for slot in node["sources"]],
                         [[inventory["pool"][0].artifact_id, inventory["pool"][1].artifact_id],
                          [inventory["pool"][2].artifact_id, inventory["pool"][3].artifact_id]])
        again = allocate_shard(spec, inventory, stream_map, 0, 1, set())
        self.assertEqual(manifest, again)
        self.assertEqual(manifest["end_cursors"], {stream_key("JJP", "pool"): 4})
        self.assertEqual(node["edm_event_id"], again["nodes"][0]["edm_event_id"])
        self.assertEqual([s["source_rng_seed"] for s in node["sources"]],
                         [s["source_rng_seed"] for s in again["nodes"][0]["sources"]])

    def test_global_job_start_changes_edm_and_shower_seeds(self):
        base = campaign(max_nodes=1)
        later = Campaign(base.name, base.analysis, base.sources, base.target_mixed_events,
                         base.max_processing_nodes, base.merge_events,
                         base.detector_variants, base.resolved_detector_variants,
                         global_job_index_start=10)
        inventory = {"pool": blocks(count=2)}
        first = allocate_shard(base, inventory, build_streams({"campaigns": [base]}), 0, 1, set())
        next_stage = allocate_shard(later, inventory, build_streams({"campaigns": [later]}), 0, 1, set())
        self.assertEqual(next_stage["nodes"][0]["global_job_index"], 10)
        self.assertNotEqual(first["nodes"][0]["edm_event_id"], next_stage["nodes"][0]["edm_event_id"])
        self.assertNotEqual(first["nodes"][0]["sources"][0]["source_rng_seed"],
                            next_stage["nodes"][0]["sources"][0]["source_rng_seed"])

    def test_temporary_shortage_rolls_back_the_entire_requested_shard(self):
        spec = campaign(max_nodes=3)
        stream_map = build_streams({"campaigns": [spec]})
        before = {key: dict(value) for key, value in stream_map.items()}
        result = allocate_shard(spec, {"pool": blocks(count=1)}, stream_map, 0, 2, set())
        self.assertIsNone(result)
        self.assertEqual(stream_map, before)

    def test_exhausted_tail_is_reported_but_empty_slot_is_an_error(self):
        spec = campaign(max_nodes=1)
        stream_map = build_streams({"campaigns": [spec]})
        inventory = {"pool": blocks(count=1, events=3)}
        manifest = allocate_shard(spec, inventory, stream_map, 0, 1, {"pool"})
        self.assertEqual(manifest["nodes"][0]["sources"][0]["lhe_events"], 3)
        self.assertEqual(manifest["shortfalls"][0]["missing_events"], 2)
        with self.assertRaisesRegex(FinalInputShortfall, "no remaining blocks"):
            allocate_shard(spec, {"pool": []}, stream_map, 0, 1, {"pool"})


if __name__ == "__main__":
    unittest.main()
