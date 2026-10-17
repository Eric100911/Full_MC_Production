"""Synthetic workers and transfers, using the real local admission protocol.

The simulated files and transactions live under /tmp/chiw. This proves no CERN
OpenAFS cross-host locking or actual Condor execution semantics.
"""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests import test_core_shards as fixtures
from full_mc.application import shards, staging
from full_mc.io import atomic_json, read_json
from full_mc.planning.shards import shard_name


class SyntheticFlow(fixtures.ShardFixture):
    def accepted(self, run, identity, **extra):
        # Fake producers publish bounded physical files; URLs remain the frozen
        # production references. PRE/POST never reads those physical files.
        for ref in extra.get("staged_objects", []):
            path = run.parent / "fake-staging" / ref["artifact_id"].replace(":", "_")
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b"x" * ref["size_bytes"])
        return super().accepted(run, identity, **extra)

    def reclaim_result(self, run, identity):
        task = self.task(run, identity)
        state = read_json(run / "state.json")
        for ref in shards._physical_refs(state, task["inputs"]):
            path = run.parent / "fake-staging" / ref["artifact_id"].replace(":", "_")
            path.unlink(missing_ok=True)
        return super().reclaim_result(run, identity)

    def assert_accounting(self, run):
        state = read_json(run / "state.json")
        account = state["staging"]
        size = sum(p.stat().st_size for p in (run.parent / "fake-staging").glob("*"))
        self.assertEqual(size, account["resident_bytes"])
        self.assertLessEqual(size + sum(account["claims"].values()), account["usable_bytes"])
        self.assertGreaterEqual(size, 0)

    def prepare_all(self, run):
        self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)
        self.source_plan(run)
        self.source_done(run)
        for name in ("C", "D"):
            for i in range(2):
                self.assertEqual(shards.prepare_shard(run, name, i)["exit_code"], 0)
        self.assert_accounting(run)

    def test_slow_shard_does_not_block_other_shards_or_safe_cleanup(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            with fixtures.remote_io_blocked():
                self.prepare_all(run)
                c0 = read_json(shards._manifest_path(run, "SHARD_C_000000"))["allocation"]
                c1 = read_json(shards._manifest_path(run, "SHARD_C_000001"))["allocation"]
                d0 = read_json(shards._manifest_path(run, "SHARD_D_000000"))["allocation"]
                refs = lambda a: [r for n in a["nodes"] for s in n["sources"] for r in s["block_refs"]]
                self.assertEqual(refs(c0), refs(d0))
                self.assertFalse(set(refs(c0)) & set(refs(c1)))
                self.finish_shard(run, "C", 1)
                self.finish_shard(run, "D", 0)
                self.finish_shard(run, "D", 1)
                state = read_json(run / "state.json")
                self.assertEqual(state["planner"]["streams"]["C::pool"]["safe_cursor"], 0)
                self.assertEqual(state["planner"]["streams"]["D::pool"]["safe_cursor"], 4)
                self.assertTrue(state["planner"]["streams"]["D::pool"]["closed"])
                self.assertEqual(shards.prepare_cleanup(run, "pool", 0)["exit_code"], 99)
                self.assertFalse((run / ".complete").exists())
                self.assert_accounting(run)
                self.finish_shard(run, "C", 0)
                state = read_json(run / "state.json")
                self.assertEqual(state["planner"]["streams"]["C::pool"]["safe_cursor"], 4)
                self.assertTrue(state["planner"]["streams"]["C::pool"]["closed"])
                self.assertEqual(shards.prepare_cleanup(run, "pool", 1)["exit_code"], 0)
                self.reclaim_result(run, "CLEANUP_pool_000001")
                self.assert_accounting(run)
                # Remaining trigger slots and the terminal slot finish immediately.
                for slot in range(2, 6):
                    self.assertEqual(shards.prepare_cleanup(run, "pool", slot)["exit_code"], 99)
                result = shards.finalize_run(run)
                self.assertEqual(result["status"], "complete")
                self.assertEqual(result["merge_groups"], 4)
                self.assertEqual(len(shards.committed_archives(run)), 5)
                self.assertEqual(len(shards.committed_groups(run)), 4)
                self.assertTrue((run / ".complete").exists())
                groups = [group["subtasks"]["merge"]["inputs"] for group in shards.committed_groups(run)]
                self.assertEqual([ref[0]["artifact_id"].split("_")[1] for ref in groups], ["C", "C", "D", "D"])
                before = (run / "state.json").read_bytes()
                shards.finalize_run(run)
                self.assertEqual((run / "state.json").read_bytes(), before)

    def test_archive_reclaim_frees_bytes_for_deferred_independent_producer(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            # This budget fits exactly one input source tuple plus one MIX.
            with staging.transaction(run) as state:
                state["staging"]["usable_bytes"] = 3000
            self.prepare_all(run)
            for campaign in ("C", "D"):
                identity = f"MIX_{campaign}_JOB000000_BLOCK000000"
                self.assertEqual(shards.admit_producer(run, identity)["exit_code"], 0)
                task = self.task(run, identity)
                output = dict(task["outputs"][0], size_bytes=983, sidecar_size_bytes=17, events=1)
                objects = [{k: output[k] for k in ("artifact_id", "url", "kind", "size_bytes")},
                           dict(artifact_id=output["artifact_id"] + ":sidecar", url=output["sidecar_url"], kind="sidecar", size_bytes=17)]
                self.accepted(run, identity, outputs=[output], staged_objects=objects)
            waiting = "MIX_D_JOB000000_BLOCK000001"
            self.assertEqual(shards.admit_producer(run, waiting)["exit_code"], 75)
            self.finish_shard(run, "D", 0, mix=False)
            self.assertEqual(shards.admit_producer(run, waiting)["exit_code"], 0)
            self.assert_accounting(run)
            # C's slow detector remains unfinished while unrelated D reclaims.
            state = read_json(run / "state.json")
            self.assertFalse(state["allocations"]["SHARD_C_000000"]["completed"])

    def test_cleanup_freezes_pending_refs_and_retries_same_manifest(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            self.prepare_all(run)
            for c in ("C", "D"):
                for i in range(2):
                    self.finish_shard(run, c, i)
            self.assertEqual(shards.prepare_cleanup(run, "pool", 0)["exit_code"], 0)
            manifest = shards._manifest_path(run, "CLEANUP_pool_000000")
            before = manifest.read_bytes()
            self.assertEqual(shards.prepare_cleanup(run, "pool", 1)["exit_code"], 99)
            shards.prepare_cleanup(run, "pool", 0)
            self.assertEqual(manifest.read_bytes(), before)
            self.reclaim_result(run, "CLEANUP_pool_000000")
            account = deepcopy(read_json(run / "state.json")["staging"])
            shards.accept_result(run, "CLEANUP_pool_000000")
            self.assertEqual(read_json(run / "state.json")["staging"], account)
            self.assert_accounting(run)

    def test_acceptance_state_commit_interruption_replays_immutable_receipt(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            shards.prepare_source(run, 0)
            original_write = staging._write_state
            with patch.object(staging, "_write_state", side_effect=RuntimeError("interrupted state commit")):
                with self.assertRaisesRegex(RuntimeError, "interrupted state commit"):
                    self.source_plan(run)
            self.assertTrue(shards._receipt_path(run, "PLAN_000000").exists())
            self.assertNotIn("PLAN_000000", read_json(run / "state.json")["accepted"])
            self.assertEqual(shards.accept_result(run, "PLAN_000000")["exit_code"], 0)
            self.assert_accounting(run)

    def test_mix_claims_cover_whole_archive_group_atomically(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory, nodes=2, campaigns=1)
            spec = read_json(run / "run.json")
            spec["limits"]["shard_nodes"] = 2
            atomic_json(run / "run.json", spec)
            shards.prepare_source(run, 0)
            self.source_plan(run)
            self.source_done(run)
            shards.prepare_shard(run, "C", 0)
            with staging.transaction(run) as state:
                state["staging"]["usable_bytes"] = 2100
            first = "MIX_C_JOB000000_BLOCK000000"
            self.assertEqual(shards.admit_producer(run, first)["exit_code"], 75)
            self.assertEqual(read_json(run / "state.json")["staging"]["claims"], {})
            with staging.transaction(run) as state:
                state["staging"]["usable_bytes"] = 2160
            self.assertEqual(shards.admit_producer(run, first)["exit_code"], 0)
            self.assertEqual(set(read_json(run / "state.json")["staging"]["claims"]),
                             {first, "MIX_C_JOB000000_BLOCK000001"})
            self.assert_accounting(run)


if __name__ == "__main__":
    unittest.main()
