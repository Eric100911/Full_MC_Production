"""Focused recovery tests for immutable records and controller state commits."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import shards, staging
from full_mc.io import read_json, write_immutable_json
from tests.test_core_shards import ShardFixture


def fail_once(function, error):
    calls = 0

    def wrapped(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error
        return function(*args, **kwargs)

    return wrapped


class ImmutableRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = ShardFixture("runTest")

    def test_json_equivalent_tuple_retry_and_conflict(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            path = Path(temporary) / "manifest.json"
            initial = {"tasks": [{"id": "A"}], "dependencies": [("A", "B")]}
            write_immutable_json(path, initial)
            first_bytes = path.read_bytes()

            write_immutable_json(path, {"tasks": [{"id": "A"}],
                                        "dependencies": [["A", "B"]]})
            self.assertEqual(path.read_bytes(), first_bytes)
            with self.assertRaisesRegex(RuntimeError, "immutable record conflict"):
                write_immutable_json(path, {"tasks": [], "dependencies": []})
            self.assertEqual(path.read_bytes(), first_bytes)

    def test_source_manifest_survives_state_replace_failure_and_retry(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            run = self.fixtures.fixture(temporary)
            before = (run / "state.json").read_bytes()
            real_write = staging._write_state
            with patch.object(staging, "_write_state",
                              side_effect=fail_once(real_write, OSError("before replace"))):
                with self.assertRaisesRegex(OSError, "before replace"):
                    shards.prepare_source(run, 0)

            manifest = shards._manifest_path(run, "SOURCE_000000")
            self.assertTrue(manifest.is_file())
            self.assertEqual((run / "state.json").read_bytes(), before)
            frozen = manifest.read_bytes()

            result = shards.prepare_source(run, 0)
            self.assertEqual(result["exit_code"], 0)
            self.assertEqual(manifest.read_bytes(), frozen)
            self.assertEqual(read_json(run / "state.json")["planner"]["sources"]["0"],
                             "admitted")

    def test_shard_manifest_survives_state_replace_failure_and_retry(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            run = self.fixtures.fixture(temporary)
            self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)
            self.fixtures.source_plan(run)
            before = (run / "state.json").read_bytes()
            real_write = staging._write_state
            with patch.object(staging, "_write_state",
                              side_effect=fail_once(real_write, OSError("before replace"))):
                with self.assertRaisesRegex(OSError, "before replace"):
                    shards.prepare_shard(run, "C", 0)

            manifest = shards._manifest_path(run, "SHARD_C_000000")
            self.assertTrue(manifest.is_file())
            self.assertEqual((run / "state.json").read_bytes(), before)
            frozen = manifest.read_bytes()

            result = shards.prepare_shard(run, "C", 0)
            self.assertEqual(result["exit_code"], 0)
            self.assertEqual(manifest.read_bytes(), frozen)
            self.assertIn("SHARD_C_000000", read_json(run / "state.json")["allocations"])

    def test_result_receipt_replays_after_state_replace_failure(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            run = self.fixtures.fixture(temporary)
            shards.prepare_source(run, 0)
            real_write = staging._write_state
            with patch.object(staging, "_write_state",
                              side_effect=fail_once(real_write, OSError("before replace"))):
                with self.assertRaisesRegex(OSError, "before replace"):
                    self.fixtures.source_plan(run)

            receipt = shards._receipt_path(run, "PLAN_000000")
            self.assertTrue(receipt.is_file())
            self.assertNotIn("PLAN_000000", read_json(run / "state.json")["accepted"])
            result = shards.accept_result(run, "PLAN_000000")
            self.assertEqual(result["status"], "accepted")
            self.assertIn("PLAN_000000", read_json(run / "state.json")["accepted"])

    def test_existing_equal_record_retries_directory_sync_after_failure(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            path = Path(temporary) / "receipt.json"
            payload = {"task_id": "PLAN_000000", "outputs": []}
            real_sync = __import__("full_mc.io", fromlist=["_sync_directory"])._sync_directory

            calls = 0

            def fail_after_publish(directory):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("directory fsync")
                return real_sync(directory)

            with patch("full_mc.io._sync_directory", side_effect=fail_after_publish):
                with self.assertRaisesRegex(OSError, "directory fsync"):
                    write_immutable_json(path, payload)
                self.assertEqual(read_json(path), payload)
                # The equal-record retry repeats the missing durability barrier.
                write_immutable_json(path, payload)

    def test_parent_directory_sync_failure_precedes_publish_and_retries(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            path = Path(temporary) / "new" / "ledger" / "record.json"
            real_sync = __import__("full_mc.io", fromlist=["_sync_directory"])._sync_directory
            failed = False

            def fail_parent_once(directory):
                nonlocal failed
                if Path(directory) == path.parent and not failed:
                    failed = True
                    raise OSError("parent directory fsync")
                return real_sync(directory)

            with patch("full_mc.io._sync_directory", side_effect=fail_parent_once):
                with self.assertRaisesRegex(OSError, "parent directory fsync"):
                    write_immutable_json(path, {"schema": "probe"})
                self.assertFalse(path.exists())
                write_immutable_json(path, {"schema": "probe"})

            self.assertEqual(read_json(path), {"schema": "probe"})

    def test_concurrent_different_writers_never_replace_the_winner(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            path = Path(temporary) / "shared.json"
            barrier = threading.Barrier(2)

            def publish(value):
                barrier.wait()
                try:
                    write_immutable_json(path, {"value": value})
                    return "created"
                except RuntimeError as error:
                    if "immutable record conflict" not in str(error):
                        raise
                    return "conflict"

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(publish, ("first", "second")))

            self.assertCountEqual(results, ["created", "conflict"])
            self.assertIn(read_json(path)["value"], {"first", "second"})


if __name__ == "__main__":
    unittest.main()
