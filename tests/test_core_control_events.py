"""Opt-in controller event compatibility and schema contracts."""
from contextlib import redirect_stderr
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import shards, staging
from full_mc.io import read_json
from tests.test_core_control_transactions import SCRATCH, new_run
from tests.test_core_shards import ShardFixture


class ControlEventTests(unittest.TestCase):
    def test_opt_in_events_preserve_return_and_state_schema(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
            run = new_run(directory)
            before = (run / "state.json").read_bytes()
            state_before = read_json(run / "state.json")
            with patch.dict(os.environ, {"MCPROD_CONTROL_EVENTS": "0",
                                         "MCPROD_ADMISSION_HINTS": "0"}):
                without_events = shards.prepare_shard(
                    run_dir=run, campaign="C", shard_index=0)
            captured = io.StringIO()
            with patch.dict(os.environ, {"MCPROD_CONTROL_EVENTS": "1",
                                         "MCPROD_ADMISSION_HINTS": "0"}), \
                    redirect_stderr(captured):
                with_events = shards.prepare_shard(
                    run_dir=run, campaign="C", shard_index=0)

            self.assertEqual(without_events, with_events)
            self.assertEqual(without_events["exit_code"], 75)
            self.assertEqual((run / "state.json").read_bytes(), before)
            state_after = read_json(run / "state.json")
            self.assertEqual(state_after, state_before)
            self.assertEqual(set(state_after), set(state_before))

            lines = captured.getvalue().splitlines()
            self.assertEqual(len(lines), 1)
            event = json.loads(lines[0])
            self.assertEqual(event["event"], "mcprod_control_operation")
            self.assertEqual(event["schema_version"], 1)
            self.assertEqual(event["operation"], "prepare_shard")
            self.assertEqual(event["task"], "SHARD_C_000000")
            self.assertEqual(event["pid"], os.getpid())
            self.assertEqual(event["run_id"], "offline")
            self.assertEqual(event["runtime_release"], "test-release")
            self.assertEqual(event["outcome"], "75")
            self.assertEqual(event["admission_hint"], False)
            self.assertFalse(event["state_committed"])
            self.assertEqual(event["commit_outcome"], "deferred")
            self.assertEqual(event["controller_build"], "unversioned")
            self.assertTrue(event["timestamp_utc"].endswith("+00:00"))
            self.assertGreaterEqual(event["duration_seconds"], 0)
            for phase in ("lock_attempt", "state_read", "state_parse", "entry_check",
                          "transaction_body"):
                self.assertIn(phase, event["phases_seconds"])
            self.assertNotIn("exit_check", event["phases_seconds"])
            self.assertNotIn("serialize", event["phases_seconds"])
            self.assertNotIn("commit", event["phases_seconds"])

    def test_commit_event_distinguishes_replacement_noop_and_deferred(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
            run = new_run(directory)
            captured = io.StringIO()
            with patch.dict(os.environ, {"MCPROD_CONTROL_EVENTS": "1",
                                         "MCPROD_ADMISSION_HINTS": "0"}), \
                    redirect_stderr(captured):
                admitted = shards.prepare_source(run_dir=run, source_index=0)
                no_op = shards.prepare_source(run_dir=run, source_index=0)
                deferred = shards.prepare_shard(
                    run_dir=run, campaign="C", shard_index=0)

            self.assertEqual(admitted["exit_code"], 0)
            self.assertEqual(no_op["exit_code"], 0)
            self.assertEqual(deferred["exit_code"], 75)
            events = [json.loads(line) for line in captured.getvalue().splitlines()]
            self.assertEqual(len(events), 3)
            self.assertEqual([item["commit_outcome"] for item in events],
                             ["replacement_synced", "no_op", "deferred"])
            self.assertEqual([item["state_committed"] for item in events],
                             [True, False, False])
            self.assertIn("file_fsync", events[0]["phases_seconds"])
            self.assertIn("replace", events[0]["phases_seconds"])
            self.assertIn("directory_fsync", events[0]["phases_seconds"])

    def test_logging_oserror_does_not_change_controller_result(self):
        class BrokenStderr:
            def write(self, _value):
                raise OSError("synthetic logging failure")

            def flush(self):
                raise OSError("synthetic logging failure")

        with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
            run = new_run(directory)
            with patch.dict(os.environ, {"MCPROD_CONTROL_EVENTS": "1",
                                         "MCPROD_ADMISSION_HINTS": "0"}), \
                    patch("sys.stderr", BrokenStderr()):
                result = shards.prepare_shard(
                    run_dir=run, campaign="C", shard_index=0)
            self.assertEqual(result["exit_code"], 75)
            self.assertEqual(
                result["reason"], "waiting for a complete shard input range")

    def test_directory_fsync_failure_reports_visible_unsynced_state(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
            run = new_run(directory)
            captured = io.StringIO()
            original_write = staging._write_state
            real_fsync = os.fsync

            def fail_directory_fsync(root, state, previous=None):
                calls = 0

                def fail_second_sync(fd):
                    nonlocal calls
                    calls += 1
                    if calls == 2:
                        raise OSError("synthetic directory fsync failure")
                    return real_fsync(fd)

                with patch("full_mc.application.staging.os.fsync",
                           side_effect=fail_second_sync):
                    return original_write(root, state, previous)

            with patch.dict(os.environ, {"MCPROD_CONTROL_EVENTS": "1",
                                         "MCPROD_ADMISSION_HINTS": "0"}), \
                    redirect_stderr(captured), \
                    patch.object(staging, "_write_state",
                                 side_effect=fail_directory_fsync):
                with self.assertRaisesRegex(OSError, "directory fsync failure"):
                    shards.prepare_source(run_dir=run, source_index=0)

            state = read_json(run / "state.json")
            self.assertEqual(state["planner"]["sources"]["0"], "admitted")
            event = json.loads(captured.getvalue())
            self.assertFalse(event["state_committed"])
            self.assertEqual(event["commit_outcome"], "visible_unsynced")

    def test_positional_accept_result_event_uses_canonical_task_id(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
            run = new_run(directory)
            with patch.dict(os.environ, {"MCPROD_CONTROL_EVENTS": "0",
                                         "MCPROD_ADMISSION_HINTS": "0"}):
                self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)
                ShardFixture().source_plan(run)

            captured = io.StringIO()
            with patch.dict(os.environ, {"MCPROD_CONTROL_EVENTS": "1",
                                         "MCPROD_ADMISSION_HINTS": "0"}), \
                    redirect_stderr(captured):
                result = shards.accept_result(run, "PLAN_000000", 0)

            self.assertEqual(result["status"], "accepted")
            event = json.loads(captured.getvalue())
            self.assertEqual(event["operation"], "accept_result")
            self.assertEqual(event["task"], "PLAN_000000")


if __name__ == "__main__":
    unittest.main()
