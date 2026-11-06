"""Fault and concurrency cases for the opt-in PLAN inventory migration."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import os
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import shards, staging
from full_mc.io import read_json
from tests import test_core_inventory_segments as inventory_fixture


class InventoryMigrationFaultTest(unittest.TestCase):
    def setUp(self):
        self.fixture = inventory_fixture.InventorySegmentTest()

    def test_exact_deployed_03_rejects_v2_in_an_independent_ledger(self):
        """Run the deployed old writer against a disposable v2-shaped state."""
        old_src = Path(
            "/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/"
            "controller-releases/v06_01-control-20261030-03/controller/core/src"
        )
        if not (old_src / "full_mc/application/staging.py").is_file():
            self.skipTest("deployed v06_01-control-20261030-03 source is unavailable")
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = Path(directory) / "run"
            (run / "staging").mkdir(parents=True)
            (run / "staging/ledger.lock").write_bytes(b"")
            state = b'{"state_schema":"mcprod-shard-state-v2"}\n'
            (run / "state.json").write_bytes(state)
            code = (
                "import sys\n"
                "from full_mc.application.staging import transaction\n"
                "with transaction(sys.argv[1]):\n    pass\n"
            )
            env = dict(os.environ, PYTHONPATH=str(old_src), PYTHONDONTWRITEBYTECODE="1")
            result = subprocess.run([sys.executable, "-c", code, str(run)], env=env,
                                    text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unsupported staging state schema", result.stderr)
            self.assertEqual((run / "state.json").read_bytes(), state)

    def test_process_death_after_durable_receipt_retries_one_segment(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture.run_fixture(Path(directory), v2=True)
            identity, _, _ = self.fixture.add_plan_records(run, 10)
            code = r'''
import os, sys
from pathlib import Path
from unittest.mock import patch
from full_mc.application import shards
original = shards.write_immutable_json
def publish_then_die(path, payload):
    result = original(path, payload)
    if Path(path).parent.name == "000000" or "ledger/accepted" in str(path):
        os._exit(77)
    return result
with patch.object(shards.legacy, "_validate_planner_result", lambda *a, **k: None):
    shards.write_immutable_json = publish_then_die
    shards.accept_result(sys.argv[1], "PLAN_000010")
'''
            env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"),
                       PYTHONDONTWRITEBYTECODE="1")
            child = subprocess.run([sys.executable, "-c", code, str(run)], env=env,
                                   text=True, capture_output=True)
            self.assertEqual(child.returncode, 77, child.stderr)

            state = read_json(run / "state.json")
            self.assertNotIn(identity, state["accepted"])
            self.assertEqual(state["planner"]["inventory_segments"]["pools"]["p"], [])
            receipt_path = shards._receipt_path(run, identity)
            self.assertTrue(receipt_path.is_file())
            self.assertEqual(read_json(receipt_path)["task_id"], identity)

            self.assertEqual(self.fixture.accept(run, identity)["exit_code"], 0)
            self.assertEqual(self.fixture.accept(run, identity)["exit_code"], 0)
            state = read_json(run / "state.json")
            self.assertEqual(state["planner"]["inventory_segments"]["pools"]["p"], [identity])
            staging._check(state)

    def test_directory_fsync_failure_after_replace_keeps_marker_and_append_consistent(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture.run_fixture(Path(directory), v2=True)
            identity, _, _ = self.fixture.add_plan_records(run, 10)
            real_fsync = os.fsync
            fail_directory = {"enabled": False}

            def fsync_with_injected_directory_failure(fd):
                if (fail_directory["enabled"]
                        and stat.S_ISDIR(os.fstat(fd).st_mode)):
                    raise OSError("injected directory fsync failure")
                return real_fsync(fd)

            real_publish = shards.write_immutable_json

            def publish_and_arm_failure(path, payload):
                result = real_publish(path, payload)
                if "/ledger/accepted/" in str(path):
                    fail_directory["enabled"] = True
                return result

            with patch.object(staging.os, "fsync", side_effect=fsync_with_injected_directory_failure):
                with patch.object(shards, "write_immutable_json", side_effect=publish_and_arm_failure):
                    with patch.object(shards.legacy, "_validate_planner_result",
                                      lambda *a, **k: None):
                        with self.assertRaisesRegex(OSError, "injected directory fsync failure"):
                            shards.accept_result(run, identity)

            # Replace happened before the directory sync fault. The visible
            # state must contain both the accepted marker and its one segment.
            state = read_json(run / "state.json")
            self.assertIn(identity, state["accepted"])
            self.assertEqual(state["planner"]["inventory_segments"]["pools"]["p"], [identity])
            self.assertTrue(shards._receipt_path(run, identity).is_file())
            staging._check(state)

            self.assertEqual(self.fixture.accept(run, identity)["exit_code"], 0)
            state = read_json(run / "state.json")
            self.assertEqual(state["planner"]["inventory_segments"]["pools"]["p"], [identity])

    def test_checkpoint_survives_pre_replace_migration_failure_and_retry(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture.run_fixture(Path(directory), sources=1)
            (run / ".controller-switch").mkdir()
            checkpoint = run / ".controller-switch" / "inventory-v2-checkpoint.json"
            with patch.object(staging, "_write_state", side_effect=OSError("before replace")):
                with self.assertRaisesRegex(OSError, "before replace"):
                    shards.migrate_inventory_v2(run, checkpoint_path=checkpoint)
            self.assertTrue(checkpoint.is_file())
            self.assertEqual(read_json(run / "state.json")["state_schema"], staging.STATE_SCHEMA)

            self.assertEqual(shards.migrate_inventory_v2(
                run, checkpoint_path=checkpoint)["status"], "migrated")
            state = read_json(run / "state.json")
            expected = read_json(checkpoint)
            self.assertEqual(state["state_schema"], staging.INVENTORY_STATE_SCHEMA)
            self.assertEqual(state["planner"]["inventory_segments"],
                             expected["inventory_segments"])

    def test_checkpointed_migration_retries_after_replace_directory_fsync_error(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture.run_fixture(Path(directory), sources=1)
            (run / ".controller-switch").mkdir()
            checkpoint = run / ".controller-switch" / "inventory-v2-checkpoint.json"
            real_fsync = os.fsync
            fail_directory = {"enabled": False}

            def fsync_with_migration_directory_failure(fd):
                if fail_directory["enabled"] and stat.S_ISDIR(os.fstat(fd).st_mode):
                    raise OSError("injected migration directory fsync failure")
                return real_fsync(fd)

            real_publish = shards.write_immutable_json

            def publish_checkpoint_then_arm(path, payload):
                result = real_publish(path, payload)
                if Path(path) == checkpoint:
                    fail_directory["enabled"] = True
                return result

            with patch.object(staging.os, "fsync",
                              side_effect=fsync_with_migration_directory_failure):
                with patch.object(shards, "write_immutable_json",
                                  side_effect=publish_checkpoint_then_arm):
                    with self.assertRaisesRegex(OSError, "migration directory fsync failure"):
                        shards.migrate_inventory_v2(run, checkpoint_path=checkpoint)

            # The state replace is visible despite its directory-sync error.
            state = read_json(run / "state.json")
            self.assertEqual(state["state_schema"], staging.INVENTORY_STATE_SCHEMA)
            self.assertTrue(checkpoint.is_file())
            self.assertEqual(shards.migrate_inventory_v2(
                run, checkpoint_path=checkpoint)["status"], "already migrated")
            self.assertEqual(read_json(run / "state.json"), state)

    def test_accept_post_racing_migration_commits_one_segment(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture.run_fixture(Path(directory), sources=1)
            identity, _, _ = self.fixture.add_plan_records(run, 0)
            barrier = threading.Barrier(2)

            def post_with_retry():
                barrier.wait(timeout=5)
                for _ in range(500):
                    result = self.fixture.accept(run, identity)
                    if result["exit_code"] == 0:
                        return result
                    if result["exit_code"] != 75:
                        raise AssertionError(result)
                    time.sleep(0.002)
                raise AssertionError("PLAN POST did not finish during migration race")

            def migrate_with_retry():
                barrier.wait(timeout=5)
                for _ in range(1000):
                    try:
                        return shards.migrate_inventory_v2(run)
                    except staging.LockBusy:
                        time.sleep(0.01)
                raise AssertionError("migration did not acquire ledger lock")

            with patch.object(shards.legacy, "_validate_planner_result",
                              lambda *a, **k: None):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    post_future = executor.submit(post_with_retry)
                    migration_future = executor.submit(migrate_with_retry)
                    self.assertEqual(post_future.result(timeout=25)["exit_code"], 0)
                    self.assertIn(migration_future.result(timeout=25)["status"],
                                  {"migrated", "already migrated"})

            state = read_json(run / "state.json")
            self.assertEqual(state["state_schema"], staging.INVENTORY_STATE_SCHEMA)
            self.assertEqual(state["planner"]["inventory_segments"]["pools"]["p"], [identity])
            staging._check(state)


if __name__ == "__main__":
    unittest.main()
