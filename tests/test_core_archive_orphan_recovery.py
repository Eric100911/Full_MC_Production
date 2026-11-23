"""Narrow offline recovery tests for an orphan archive acceptance receipt."""
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import shards
from full_mc.cli import build_parser, dispatch
from full_mc.io import atomic_json, read_json, write_immutable_json
from full_mc.local_paths import record_path
from tests.test_core_shards import ShardFixture


class ArchiveOrphanRecoveryTests(ShardFixture):
    def prepare_archive(self, run):
        shards.prepare_source(run, 0)
        self.source_plan(run)
        identity = "SOURCE_ARCHIVE_000000"
        shards.prepare_source_product(run, identity)
        return identity

    def make_orphan(self, run, *, current_change=None, receipt_change=None):
        identity = self.prepare_archive(run)
        task = self.task(run, identity)
        state = read_json(run / "state.json")
        members = []
        for index, ref in enumerate(task["inputs"]):
            accepted = shards._find_producer(run, state, ref["artifact_id"])
            members.append(dict(accepted, member=f"{index:06d}_source.lhe.gz"))
        output = dict(task["outputs"][0], size_bytes=1200, local_path="/scratch/worker-a/upstream.tar")
        outcome = dict(task_id=identity, stage="archive", status="ok",
                       runtime_release="test-release", outputs=[output], members=members)
        orphan = deepcopy(outcome)
        orphan["outputs"][0].pop("local_path")
        if receipt_change:
            receipt_change(orphan)
        if current_change:
            current_change(outcome)
        receipt_path = shards._receipt_path(run, identity)
        result_path = record_path(run, "results", identity)
        write_immutable_json(receipt_path, orphan)
        atomic_json(result_path, outcome)
        return identity, receipt_path, result_path, orphan, outcome

    def test_accepts_only_local_path_difference_and_preserves_both_records(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            identity, receipt_path, result_path, orphan, outcome = self.make_orphan(run)
            before_receipt = receipt_path.read_bytes()
            before_result = result_path.read_bytes()
            recovered = shards.recover_archive_orphan(run, identity, confirm_no_active_writer=True)
            self.assertEqual(recovered["status"], "accepted")
            self.assertTrue(recovered["recovered_orphan"])
            self.assertEqual(receipt_path.read_bytes(), before_receipt)
            self.assertEqual(result_path.read_bytes(), before_result)
            state = read_json(run / "state.json")
            self.assertEqual(state["accepted"][identity], {"stage": "archive"})
            self.assertEqual(read_json(receipt_path), orphan)
            self.assertEqual(read_json(result_path), outcome)

    def test_rejects_remote_output_or_member_difference_without_state_change(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            def change(value):
                value["outputs"][0]["url"] += ".other"
            identity, receipt_path, result_path, _, _ = self.make_orphan(run, receipt_change=change)
            before_state = (run / "state.json").read_bytes()
            with self.assertRaisesRegex(RuntimeError, "differs beyond output local_path"):
                shards.recover_archive_orphan(run, identity, confirm_no_active_writer=True)
            self.assertEqual((run / "state.json").read_bytes(), before_state)
            self.assertNotIn(identity, read_json(run / "state.json")["accepted"])
            self.assertTrue(receipt_path.exists())
            self.assertTrue(result_path.exists())

    def test_rejects_existing_acceptance_marker(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            identity, _, _, _, _ = self.make_orphan(run)
            state = read_json(run / "state.json")
            state["accepted"][identity] = {"stage": "archive"}
            atomic_json(run / "state.json", state)
            with self.assertRaisesRegex(RuntimeError, "already has an accepted marker"):
                shards.recover_archive_orphan(run, identity, confirm_no_active_writer=True)

    def test_rejects_non_archive_task(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            shards.prepare_source(run, 0)
            self.source_plan(run)
            shards.prepare_shard(run, "C", 0)
            identity = "PROCESS_C_JOB000000_BLOCK000000"
            with self.assertRaisesRegex(ValueError, "only accepts archive"):
                shards.recover_archive_orphan(run, identity, confirm_no_active_writer=True)

    def test_cli_routes_narrow_recovery_action(self):
        args = build_parser().parse_args([
            "recovery", "accept-archive-orphan", "--run-dir", "/tmp/run",
            "--task-id", "ARCHIVE_SHARD_C_000000", "--confirm-no-active-writer"])
        with patch("full_mc.application.shards.recover_archive_orphan",
                   return_value={"exit_code": 0, "status": "accepted"}) as recover:
            self.assertEqual(dispatch(args), {"exit_code": 0, "status": "accepted"})
            recover.assert_called_once_with(Path("/tmp/run"), "ARCHIVE_SHARD_C_000000",
                                            confirm_no_active_writer=True)


if __name__ == "__main__":
    unittest.main()
