"""Allocation and wave bookkeeping invariants, without a scheduler/storage service."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.domain.models import Source, Campaign, CampaignSource
from full_mc.domain.inventory import source_blocks
from full_mc.domain.allocation import allocate_campaign_wave
from full_mc.planning.capacity import complete_node_capacity


def campaign(max_nodes=2, repeated=False):
    slots = [CampaignSource("pool", "normal", 10, 1.0)]
    if repeated:
        slots.append(CampaignSource("pool", "normal", 10, 1.0))
    return Campaign("JJP_DPS1", "JJP", slots, 5, max_nodes, 100,
                    ["run3_2022_legacy_premix_v1"],
                    [{"profile_id": "run3_2022_legacy_premix_v1"}])


class AllocationContracts(unittest.TestCase):
    def test_inventory_order_is_not_replaced_by_sorted_source_ids(self):
        sources = [Source("pool", "z9", 9, "/z.lhe", 10, 1),
                   Source("pool", "a2", 2, "/a.lhe", 10, 1)]
        blocks = source_blocks(sources, 10)["pool"]
        self.assertEqual([b.source_id for b in blocks], ["z9", "a2"])

    def test_repeated_pool_slots_consume_distinct_blocks_and_keep_tail(self):
        blocks = source_blocks([Source("pool", "s", 1, "/s.lhe", 35, 1)], 10)
        spec = campaign(repeated=True)
        result = allocate_campaign_wave(spec, blocks, {}, 2)
        self.assertEqual(len(result["nodes"]), 2)
        refs = [ref for node in result["nodes"] for slot in node["sources"]
                for ref in slot["block_refs"]]
        self.assertEqual(len(refs), len(set(refs)))
        self.assertEqual(result["nodes"][-1]["sources"][-1]["lhe_events"], 5)
        self.assertEqual(complete_node_capacity(blocks, spec), len(result["nodes"]))

    def test_campaign_limit_applies_across_waves(self):
        blocks = source_blocks([Source("pool", "s", 1, "/s.lhe", 100, 1)], 10)
        result = allocate_campaign_wave(campaign(), blocks, {"pool": 1}, 10, 1)
        self.assertEqual(len(result["nodes"]), 1)
        self.assertRegex(result["nodes"][0]["job_id"], r"JOB\d+_BLOCK\d+")
        done = allocate_campaign_wave(campaign(), blocks, {"pool": 2}, 10, 2)
        self.assertEqual(done["nodes"], [])


if __name__ == "__main__":
    unittest.main()
