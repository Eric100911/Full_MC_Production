"""Focused contracts for no-mutation controller deferrals."""
import fcntl
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import shards, staging
from full_mc.io import atomic_json, read_json
from full_mc.local_paths import record_path
from tests.test_core_shards import ShardFixture


SCRATCH = Path("/tmp/chiw/v06_01_implementation/transactions")
SCRATCH.mkdir(parents=True, exist_ok=True)


def new_run(directory):
    return ShardFixture().fixture(Path(directory))


class ControlTransactionTests(unittest.TestCase):
    def temporary_run(self):
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name), new_run(temporary.name)

    def assert_deferred_without_commit(self, operation, run, expected_reason):
        before = (run / "state.json").read_bytes()
        original_check, original_serialize = staging._check_inventory_segments, staging._serialize
        with patch.object(staging, "_check_inventory_segments", wraps=original_check) as check, \
                patch.object(staging, "_serialize",
                             wraps=original_serialize) as serialize:
            results = [operation() for _ in range(3)]
        self.assertEqual([result["exit_code"] for result in results], [75, 75, 75])
        self.assertTrue(all(expected_reason in result["reason"] for result in results))
        self.assertEqual(check.call_count, 3)  # entry validation only
        self.assertEqual(serialize.call_count, 0)
        self.assertEqual((run / "state.json").read_bytes(), before)

    def test_earlier_source_guard_is_pure_deferred(self):
        _, run = self.temporary_run()
        spec = read_json(run / "run.json")
        spec["sources"].append(dict(spec["sources"][0], source_id="s2", seed=20))
        atomic_json(run / "run.json", spec)
        self.assert_deferred_without_commit(
            lambda: shards.prepare_source(run, 1), run,
            "waiting for earlier source admission")

    def test_earlier_and_incomplete_shard_guards_are_pure_deferred(self):
        _, run = self.temporary_run()
        self.assert_deferred_without_commit(
            lambda: shards.prepare_shard(run, "C", 0), run,
            "complete shard input range")
        with staging.transaction(run) as state:
            state["planner"]["assigned_progress"]["C"] = 1
        self.assert_deferred_without_commit(
            lambda: shards.prepare_shard(run, "C", 0), run,
            "earlier assignments")

    def test_source_archive_waiting_for_plan_is_pure_deferred(self):
        _, run = self.temporary_run()
        self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)
        self.assert_deferred_without_commit(
            lambda: shards.prepare_source_product(run, "SOURCE_ARCHIVE_000000"), run,
            "no accepted PLAN result")

    def test_source_target_hint_deferral_still_commits_mutation(self):
        _, run = self.temporary_run()
        spec = read_json(run / "run.json")
        spec["sources"].append(dict(spec["sources"][0], source_id="s2", seed=20))
        atomic_json(run / "run.json", spec)
        self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)
        ShardFixture().source_plan(run)
        with staging.transaction(run) as state:
            state["planner"]["target_progress"] = {campaign["name"]: 0
                                                    for campaign in spec["campaigns"]}
        before = read_json(run / "state.json")["planner"]["target_progress"]
        result = shards.prepare_source(run, 1)
        self.assertEqual(result["exit_code"], 75)
        self.assertIn("soft planning window", result["reason"])
        after = read_json(run / "state.json")
        self.assertNotEqual(after["planner"]["target_progress"], before)
        self.assertNotIn("1", after["planner"]["sources"])

    def test_non_lock_blocking_io_error_is_not_reported_as_lock_busy(self):
        _, run = self.temporary_run()
        with patch.object(
                staging, "_check_inventory_segments",
                side_effect=BlockingIOError("synthetic read fault")):
            with self.assertRaisesRegex(BlockingIOError, "synthetic read fault"):
                shards.prepare_shard(run, "C", 0)

    def test_actual_nonblocking_flock_contention_is_reported_as_busy(self):
        _, run = self.temporary_run()
        with (run / "ledger/state.lock").open("r+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = shards.prepare_shard(run, "C", 0)
        self.assertEqual(result["exit_code"], 75)
        self.assertEqual(result["reason"], "local state lock busy")

    def test_post_retries_real_lock_busy_then_accepts_once(self):
        _, run = self.temporary_run()
        self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)
        identity = "PLAN_000000"
        p = read_json(record_path(run, "tasks", identity))["parameters"]
        output = dict(artifact_id="BLOCK:pool:s1:000000", kind="lhe-block", events=5,
                      url=p["block_output_dir"] + "/block_s1_000000.lhe.gz", size_bytes=20)
        outcome = dict(task_id=identity, stage="plan", status="ok", runtime_release="test-release",
                       outputs=[output], originals=[dict(p["original_output"], events=40, size_bytes=100)],
                       actual_input_events=40, planned_events=5)
        atomic_json(record_path(run, "results", identity), outcome)

        before = (run / "state.json").read_bytes()
        receipt = shards._receipt_path(run, identity)
        self.assertFalse(receipt.exists())

        with (run / "ledger/state.lock").open("r+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            busy = [shards.accept_result(run, identity, 0) for _ in range(3)]
            self.assertEqual([result["exit_code"] for result in busy], [75, 75, 75])
            self.assertTrue(all(result["reason"] == "local state lock busy"
                                for result in busy))
            self.assertEqual((run / "state.json").read_bytes(), before)
            self.assertFalse(receipt.exists())

            worker_failure = shards.accept_result(run, identity, 75)
            self.assertEqual(worker_failure["exit_code"], 1)
            self.assertEqual(worker_failure["worker_return_code"], 75)
            self.assertEqual((run / "state.json").read_bytes(), before)
            self.assertFalse(receipt.exists())

        accepted = shards.accept_result(run, identity, 0)
        self.assertEqual(accepted, {"exit_code": 0, "status": "accepted"})
        accepted_state_bytes = (run / "state.json").read_bytes()
        accepted_state = read_json(run / "state.json")
        self.assertEqual(accepted_state["accepted"][identity], {"stage": "plan"})
        self.assertEqual(read_json(receipt), outcome)
        receipt_bytes = receipt.read_bytes()

        duplicate = shards.accept_result(run, identity, 0)
        self.assertEqual(duplicate, {"exit_code": 0, "status": "accepted"})
        self.assertEqual((run / "state.json").read_bytes(), accepted_state_bytes)
        self.assertEqual(receipt.read_bytes(), receipt_bytes)
        self.assertEqual(accepted_state["planner"]["inventory_segments"]["pools"]["pool"], [identity])


if __name__ == "__main__":
    unittest.main()
