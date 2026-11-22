"""Ledger contracts: immutable writes and committedness at read time."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application.ledger import (archive_path, archive_record, group_path,
                                        inventory_path, inventory_record, load_archives,
                                        load_groups, load_inventory, load_wave,
                                        sample_key, wave_path, wave_receipt,
                                        write_archive, write_group, write_inventory,
                                        write_wave)
from full_mc.io import read_json, write_immutable_json


def make_spec(root):
    return {
        "run_id": "ledger-run",
        "storage": {"ihep_archive": str(root / "ihep" / "archive")},
        # Deliberately not sorted: readers must follow this order, not the filesystem's.
        "sources": [{"pool": "pool_b", "source_id": "source_000003"},
                    {"pool": "pool_a", "source_id": "source_000001"},
                    {"pool": "pool_b", "source_id": "source_000007"}],
        "campaigns": [{"name": "C", "resolved_detector_variants": [
            {"profile_id": "run3_2022_v1"}, {"profile_id": "run3_2023_v2"}]}],
    }


class ImmutableWrites(unittest.TestCase):
    def test_identical_payload_is_an_idempotent_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "record.json"
            payload = {"schema": "x", "step": 1, "values": [1, 2, 3]}
            write_immutable_json(path, payload)
            write_immutable_json(path, dict(payload))
            self.assertEqual(read_json(path), payload)

    def test_conflicting_content_is_fatal(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "record.json"
            write_immutable_json(path, {"schema": "x", "step": 1})
            with self.assertRaises(RuntimeError) as raised:
                write_immutable_json(path, {"schema": "x", "step": 2})
            self.assertIn("immutable record conflict", str(raised.exception))
            self.assertEqual(read_json(path), {"schema": "x", "step": 1})


class InventoryContracts(unittest.TestCase):
    def test_uncommitted_record_stays_invisible(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = make_spec(root)
            source = spec["sources"][0]
            record = inventory_record(0, source, accepted_step=4,
                                      producer_result={"task_id": "PLAN_000000"})
            write_inventory(root, record)
            self.assertEqual(load_inventory(root, spec, accepted_before=5), [record])
            # The state commit marker has not reached this record yet.
            self.assertEqual(load_inventory(root, spec, accepted_before=4), [])
            self.assertEqual(load_inventory(root, spec, accepted_before=1), [])

    def test_reader_follows_run_spec_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = make_spec(root)
            # Write in an order that disagrees with both the spec and sorting.
            for index in (2, 0, 1):
                source = spec["sources"][index]
                write_inventory(root, inventory_record(
                    index, source, accepted_step=0,
                    producer_result={"task_id": f"PLAN_{index:06d}"}))
            loaded = load_inventory(root, spec, accepted_before=1)
            self.assertEqual([item["source_index"] for item in loaded], [0, 1, 2])
            self.assertEqual([item["source_id"] for item in loaded],
                             ["source_000003", "source_000001", "source_000007"])

    def test_record_disagreeing_with_its_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = make_spec(root)
            source = spec["sources"][0]
            write_inventory(root, inventory_record(1, source, accepted_step=0,
                                                   producer_result={}))
            with self.assertRaises(RuntimeError):
                load_inventory(root, spec, accepted_before=1)


class ArchiveContracts(unittest.TestCase):
    def test_receipts_are_returned_in_step_order_below_the_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = make_spec(root)
            base = str(root / "ihep" / "archive") + "/ledger-run"
            for step in (2, 0, 1):
                write_archive(root, archive_record(
                    step, f"ARCHIVE_{step:06d}",
                    [{"artifact_id": f"ARCHIVE_{step:06d}",
                      "url": f"{base}/wave_{step:06d}.tar"}],
                    [{"artifact_id": "LHE_000000", "kind": "lhe"}]))
            loaded = load_archives(root, spec, step_before=2)
            self.assertEqual([item["step"] for item in loaded], [0, 1])
            self.assertEqual([item["step"] for item in
                              load_archives(root, spec, step_before=3)], [0, 1, 2])


class GroupContracts(unittest.TestCase):
    def test_only_ordinals_below_the_committed_count_are_returned(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = make_spec(root)
            groups = []
            for ordinal in range(3):
                group = {"task_id": f"C_run3_2022_v1_MERGE{ordinal:06d}", "stage": "combined",
                         "subtasks": {"merge": {"parameters": {"sample_key":
                                     sample_key("C", "run3_2022_v1")}}}}
                write_group(root, "C", "run3_2022_v1", ordinal, group)
                groups.append(group)
            counts = {sample_key("C", "run3_2022_v1"): 2}
            loaded = load_groups(root, spec, counts)
            self.assertEqual(loaded, groups[:2])
            self.assertTrue(group_path(root, "C", "run3_2022_v1", 2).exists())

    def test_a_missing_committed_ordinal_is_fatal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaises(RuntimeError):
                load_groups(root, make_spec(root), {sample_key("C", "run3_2022_v1"): 1})


class WaveContracts(unittest.TestCase):
    def test_round_trip_of_every_record_kind(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = make_spec(root)
            source = spec["sources"][1]
            inventory = inventory_record(1, source, accepted_step=3, producer_result={
                "task_id": "PLAN_000001", "stage": "plan", "status": "ok",
                "outputs": [{"artifact_id": "BLOCK:pool_a:source_000001:000000",
                             "url": "root://store/blocks/block_000000.lhe.gz",
                             "kind": "lhe-block", "events": 1000, "size_bytes": 42}]})
            base = str(root / "ihep" / "archive") + "/ledger-run"
            archive = archive_record(3, "ARCHIVE_000003",
                                     [{"artifact_id": "ARCHIVE_000003",
                                       "url": f"{base}/wave_000003.tar"}],
                                     [{"artifact_id": "LHE_000001", "kind": "lhe",
                                       "events": 1000, "size_bytes": 42}])
            group = {"task_id": "C_run3_2022_v1_MERGE000000", "stage": "combined",
                     "subtasks": {"merge": {"inputs": [{"artifact_id": "MINIAOD_x"}]}}}
            pending = {"step": 3, "phase": "processing", "task_ids": ["MIX_x"],
                       "allocation": {"campaigns": []}}
            results = {"MIX_x": {"task_id": "MIX_x", "status": "ok", "stage": "mix"}}
            receipt = wave_receipt(pending, results, [group])

            write_inventory(root, inventory)
            write_archive(root, archive)
            write_group(root, "C", "run3_2022_v1", 0, group)
            write_wave(root, receipt)

            # A retry after a crash must reproduce the same files, not conflict.
            write_inventory(root, inventory)
            write_archive(root, archive)
            write_group(root, "C", "run3_2022_v1", 0, group)
            write_wave(root, receipt)

            self.assertEqual(load_inventory(root, spec, accepted_before=4), [inventory])
            self.assertEqual(load_archives(root, spec, step_before=4), [archive])
            self.assertEqual(load_groups(root, spec, {sample_key("C", "run3_2022_v1"): 1}),
                             [group])
            self.assertEqual(load_wave(root, 3), receipt)


class PathContracts(unittest.TestCase):
    def test_records_land_where_the_layout_says(self):
        root = Path("/run")
        self.assertEqual(inventory_path(root, "pool_a", "source_000001"),
                         Path("/run/ledger/inventory/pool_a/source_000001.json"))
        self.assertEqual(archive_path(root, 7), Path("/run/ledger/archives/000007.json"))
        self.assertEqual(wave_path(root, 7), Path("/run/ledger/waves/000007.json"))
        self.assertEqual(group_path(root, "C", "run3_v1", 2),
                         Path("/run/ledger/groups/C/run3_v1/000002.json"))

    def test_unusable_path_components_are_rejected(self):
        root = Path("/run")
        with self.assertRaises(ValueError):
            inventory_path(root, "pool/../../escape", "source_000001")
        with self.assertRaises(ValueError):
            inventory_path(root, "pool_a", "source 000001")
        with self.assertRaises(ValueError):
            group_path(root, "C", "run3 v1", 0)
        with self.assertRaises(ValueError):
            group_path(root, "C:d", "run3_v1", 0)
        with self.assertRaises(ValueError):
            archive_path(root, -1)


if __name__ == "__main__":
    unittest.main()
