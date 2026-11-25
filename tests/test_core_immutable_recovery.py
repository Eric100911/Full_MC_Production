"""Focused recovery tests for immutable JSON records."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.io import read_json, write_immutable_json


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
    def test_json_equivalent_tuple_retry_and_conflict(self):
        with tempfile.TemporaryDirectory() as temporary:
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

    def test_existing_equal_record_retries_directory_sync_after_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
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
        with tempfile.TemporaryDirectory() as temporary:
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
        with tempfile.TemporaryDirectory() as temporary:
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
