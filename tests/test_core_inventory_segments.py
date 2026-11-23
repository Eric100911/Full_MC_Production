"""Contracts for acceptance-ordered PLAN inventory."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import shards, staging
from full_mc.domain.streams import allocate_shard, build_streams
from full_mc.io import atomic_json, read_json
from full_mc.local_paths import record_path
from full_mc.planning.shards import shard_ranges


class InventorySegmentTest(unittest.TestCase):
    def run_fixture(self, root, sources=13):
        run = root / "run"
        run.mkdir()
        source_specs = [{"pool": "p", "source_id": f"s{i}", "seed": i + 1}
                        for i in range(sources)]
        atomic_json(run / "run.json", {"state_schema": staging.FUSED_STATE_SCHEMA, "run_id": "segments", "runtime_release": "test",
                    "sources": source_specs,
                    "campaigns": [{"name": "C", "sources": [
                        {"pool": "p", "mode": "normal", "lhe_budget": 5}]}]})
        planner = {"sources": {str(i): "admitted" for i in range(sources)}}
        staging.initialize(run, planner, {"p"})
        return run

    def add_plan_records(self, run, index, *, accepted=False):
        identity = f"PLAN_{index:06d}"
        ref = {"artifact_id": f"BLOCK:p:s{index}:000000",
               "url": f"root://stage.invalid/blocks/block_s{index}_000000.lhe.gz",
               "kind": "lhe-block", "size_bytes": 20, "events": 5}
        task = {"task_id": identity, "stage": "plan", "parameters": {"block_output_dir": "root://stage.invalid/blocks",
                "group_id": f"s{index}", "original_output": {}}}
        runtime_release = read_json(run / "run.json").get("runtime_release", "test")
        outcome = {"task_id": identity, "stage": "plan", "status": "ok",
                   "runtime_release": runtime_release, "outputs": [ref], "originals": [{}]}
        for kind, payload in (("tasks", task), ("results", outcome)):
            path = record_path(run, kind, identity)
            path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(path, payload)
        if accepted:
            path = shards._receipt_path(run, identity)
            path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(path, outcome)
        return identity, ref, outcome

    def accept(self, run, identity):
        with patch.object(shards, "validate_planner_result"):
            return shards.accept_result(run, identity)

    def test_post_acceptance_order_does_not_renumber_existing_blocks(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory))
            identity, _, _ = self.add_plan_records(run, 11)
            self.assertEqual(self.accept(run, identity)["exit_code"], 0)
            state = read_json(run / "state.json")
            blocks, _, _ = shards._inventory(run, read_json(run / "run.json"), state)
            self.assertEqual([(block.source_id, block.index) for block in blocks["p"]], [("s11", 0)])

            # Freeze a pending allocation against the first accepted segment.
            state["allocations"]["SHARD_C_000000"] = {
                "campaign": "C", "start_job_index": 0, "node_count": 1, "completed": False}
            atomic_json(run / "state.json", state)
            manifest = shards._manifest_path(run, "SHARD_C_000000")
            manifest.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(manifest, {"allocation": {"campaign": "C", "start_job_index": 0,
                "nodes": [{"sources": [{"slot": 0, "pool": "p",
                    "block_refs": ["BLOCK:p:s11:000000"]}]}]}})
            manifest_before = manifest.read_bytes()

            identity, _, _ = self.add_plan_records(run, 10)
            self.assertEqual(self.accept(run, identity)["exit_code"], 0)
            state = read_json(run / "state.json")
            blocks, _, _ = shards._inventory(run, read_json(run / "run.json"), state)
            self.assertEqual([(block.source_id, block.index) for block in blocks["p"]],
                             [("s11", 0), ("s10", 1)])
            self.assertEqual(manifest.read_bytes(), manifest_before)
            for _ in range(10):
                self.assertEqual(self.accept(run, "PLAN_000010")["exit_code"], 0)
            state = read_json(run / "state.json")
            self.assertEqual(state["planner"]["inventory_segments"]["pools"]["p"],
                             ["PLAN_000011", "PLAN_000010"])

    def test_concurrent_posts_are_serialized_and_idempotent(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory))
            for index in (10, 11):
                self.add_plan_records(run, index)

            def post(identity):
                for _ in range(200):
                    result = shards.accept_result(run, identity)
                    if result["exit_code"] == 0:
                        return result
                    if result["exit_code"] != 75:
                        raise AssertionError(result)
                    time.sleep(0.01)
                raise AssertionError("concurrent PLAN POST did not acquire the ledger lock")

            with patch.object(shards, "validate_planner_result"):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    results = list(executor.map(post, ("PLAN_000011", "PLAN_000010")))
            self.assertTrue(all(result["exit_code"] == 0 for result in results))
            for index in (10, 11):
                for _ in range(10):
                    self.assertEqual(self.accept(run, f"PLAN_{index:06d}")["exit_code"], 0)
            state = read_json(run / "state.json")
            self.assertEqual(len(state["planner"]["inventory_segments"]["pools"]["p"]), 2)
            self.assertEqual({"PLAN_000010", "PLAN_000011"},
                             set(state["planner"]["inventory_segments"]["pools"]["p"]))

    def test_receipt_before_state_commit_retries_without_duplicate_segment(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory))
            identity, _, _ = self.add_plan_records(run, 10)
            writer = staging._write_state
            with patch.object(staging, "_write_state", side_effect=OSError("injected commit failure")):
                with self.assertRaisesRegex(OSError, "injected commit failure"):
                    self.accept(run, identity)
            self.assertTrue(shards._receipt_path(run, identity).is_file())
            self.assertNotIn(identity, read_json(run / "state.json")["accepted"])
            with patch.object(staging, "_write_state", wraps=writer):
                self.assertEqual(self.accept(run, identity)["exit_code"], 0)
            self.assertEqual(read_json(run / "state.json")["planner"]["inventory_segments"]["pools"]["p"],
                             [identity])

    def test_inventory_ordinals_feed_repeated_strided_pool_slots(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=5)
            spec = read_json(run / "run.json")
            spec["campaigns"][0].update({"analysis": "JJP", "target_mixed_events": 5,
                "max_processing_nodes": 1, "merge_events": 5, "detector_variants": ["test"],
                "resolved_detector_variants": [{"profile_id": "test"}]})
            spec["campaigns"][0]["sources"] = [
                {"pool": "p", "mode": "normal", "lhe_budget": 5},
                {"pool": "p", "mode": "normal", "lhe_budget": 5}]
            spec["consumption_streams"] = [
                {"campaign": "C", "pool": "p", "stride": 2, "offset": 1},
                {"campaign": "D", "pool": "p", "stride": 2, "offset": 0}]
            spec["campaigns"].append({"name": "D", "analysis": "JJP",
                "target_mixed_events": 5, "max_processing_nodes": 1, "merge_events": 5,
                "detector_variants": ["test"], "resolved_detector_variants": [{"profile_id": "test"}],
                "sources": [{"pool": "p", "mode": "normal", "lhe_budget": 5}]})
            atomic_json(run / "run.json", spec)
            order = (3, 0, 2, 1, 4)
            state = read_json(run / "state.json")
            for index in order:
                identity, _, outcome = self.add_plan_records(run, index, accepted=True)
                state["accepted"][identity] = {"stage": "plan"}
            state["planner"]["inventory_segments"]["pools"]["p"] = [
                f"PLAN_{index:06d}" for index in order]
            atomic_json(run / "state.json", state)
            blocks, _, exhausted = shards._inventory(run, spec, read_json(run / "state.json"))
            streams = build_streams(spec)
            allocation = allocate_shard(spec["campaigns"][0], blocks, streams, 0, 1, exhausted)
            self.assertEqual([slot["block_refs"] for slot in allocation["nodes"][0]["sources"]],
                             [["BLOCK:p:s0:000000"], ["BLOCK:p:s1:000000"]])
            other = allocate_shard(spec["campaigns"][1], blocks, streams, 0, 1, exhausted)
            self.assertEqual(other["nodes"][0]["sources"][0]["block_refs"],
                             ["BLOCK:p:s3:000000"])

    def test_inventory_preserves_uneven_multiblock_tail(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=1)
            spec = read_json(run / "run.json")
            spec["campaigns"][0].update({"analysis": "JJP", "target_mixed_events": 5,
                "max_processing_nodes": 1, "merge_events": 5, "detector_variants": ["test"],
                "resolved_detector_variants": [{"profile_id": "test"}]})
            spec["campaigns"][0]["sources"] = [
                {"pool": "p", "mode": "normal", "lhe_budget": 13}]
            atomic_json(run / "run.json", spec)
            identity, _, outcome = self.add_plan_records(run, 0, accepted=True)
            outcome["outputs"] = [dict(artifact_id=f"BLOCK:p:s0:{index:06d}",
                url=f"root://stage.invalid//block{index}.lhe.gz", kind="lhe-block",
                size_bytes=20, events=events) for index, events in enumerate((5, 5, 2))]
            atomic_json(shards._receipt_path(run, identity), outcome)
            state = read_json(run / "state.json")
            state["accepted"][identity] = {"stage": "plan"}
            state["planner"]["inventory_segments"]["pools"]["p"] = [identity]
            atomic_json(run / "state.json", state)
            blocks, _, exhausted = shards._inventory(run, spec, read_json(run / "state.json"))
            self.assertEqual([block.events for block in blocks["p"]], [5, 5, 2])
            self.assertEqual([block.index for block in blocks["p"]], [0, 1, 2])
            allocation = allocate_shard(spec["campaigns"][0], blocks,
                build_streams(spec), 0, 1, exhausted)
            self.assertEqual(allocation["nodes"][0]["sources"][0]["block_refs"],
                ["BLOCK:p:s0:000000", "BLOCK:p:s0:000001", "BLOCK:p:s0:000002"])
            self.assertEqual(allocation["shortfalls"][0]["missing_events"], 1)

    def test_inventory_allocates_the_final_partial_ten_node_range(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=1)
            spec = read_json(run / "run.json")
            spec["limits"] = {"shard_nodes": 10}
            spec["campaigns"][0].update({"analysis": "JJP", "target_mixed_events": 1,
                "max_processing_nodes": 11, "merge_events": 1, "detector_variants": ["test"],
                "resolved_detector_variants": [{"profile_id": "test"}]})
            spec["campaigns"][0]["sources"] = [
                {"pool": "p", "mode": "normal", "lhe_budget": 1}]
            atomic_json(run / "run.json", spec)
            identity, _, outcome = self.add_plan_records(run, 0, accepted=True)
            outcome["outputs"] = [dict(artifact_id=f"BLOCK:p:s0:{index:06d}",
                url=f"root://stage.invalid//block{index}.lhe.gz", kind="lhe-block",
                size_bytes=20, events=1) for index in range(11)]
            atomic_json(shards._receipt_path(run, identity), outcome)
            state = read_json(run / "state.json")
            state["accepted"][identity] = {"stage": "plan"}
            state["planner"]["inventory_segments"]["pools"]["p"] = [identity]
            atomic_json(run / "state.json", state)
            self.assertEqual(shard_ranges(spec), [("C", 0, 0, 10), ("C", 1, 10, 1)])
            blocks, _, exhausted = shards._inventory(run, spec, read_json(run / "state.json"))
            streams = build_streams(spec)
            streams["C::p"]["allocation_cursor"] = 10
            allocation = allocate_shard(spec["campaigns"][0], blocks, streams,
                                         10, 1, exhausted)
            self.assertEqual(len(allocation["nodes"]), 1)
            self.assertEqual(allocation["nodes"][0]["job_index"], 10)
            self.assertEqual(allocation["nodes"][0]["sources"][0]["block_refs"],
                             ["BLOCK:p:s0:000010"])

    def test_segments_must_match_accepted_plan_markers(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory))
            state = read_json(run / "state.json")
            state["planner"]["inventory_segments"]["pools"]["p"] = ["PLAN_000000"]
            atomic_json(run / "state.json", state)
            with self.assertRaisesRegex(ValueError, "accepted PLAN result"):
                with staging.transaction(run):
                    self.fail("invalid segment must not yield")

    def test_invalid_source_or_pool_segment_aborts_before_transaction_commit(self):
        for identity, pool in (("PLAN_999999", "p"), ("PLAN_000000", "wrong-pool")):
            with self.subTest(identity=identity, pool=pool):
                with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
                    run = self.run_fixture(Path(directory), sources=1)
                    state = read_json(run / "state.json")
                    state["accepted"][identity] = {"stage": "plan"}
                    state["planner"]["inventory_segments"]["pools"] = {pool: [identity]}
                    atomic_json(run / "state.json", state)
                    before = (run / "state.json").read_bytes()
                    with self.assertRaises(RuntimeError):
                        with staging.transaction(run) as locked_state:
                            shards._inventory(run, read_json(run / "run.json"), locked_state)
                    self.assertEqual((run / "state.json").read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
