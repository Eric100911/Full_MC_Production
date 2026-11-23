"""Ledger contracts: immutable writes and committedness at read time."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application.ledger import group_path, load_groups, sample_key, write_group
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


if __name__ == "__main__":
    unittest.main()
