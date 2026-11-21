"""Contracts for the opt-in acceptance-ordered PLAN inventory schema."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from copy import deepcopy
from io import StringIO
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import shards, staging
from full_mc.cli import main as mcprod_main
from full_mc.domain.streams import allocate_shard, build_streams
from full_mc.io import atomic_json, read_json
from full_mc.local_paths import record_path
from full_mc.planning.shards import shard_ranges
from tests import test_core_production as legacy_tests
from tests import test_core_shards


class InventorySegmentTest(unittest.TestCase):
    def run_fixture(self, root, sources=13, *, v2=False):
        run = root / "run"
        run.mkdir()
        source_specs = [{"pool": "p", "source_id": f"s{i}", "seed": i + 1}
                        for i in range(sources)]
        atomic_json(run / "run.json", {"run_id": "segments", "runtime_release": "test",
                    "sources": source_specs,
                    "campaigns": [{"name": "C", "sources": [
                        {"pool": "p", "mode": "normal", "lhe_budget": 5}]}]})
        planner = {"sources": {str(i): "admitted" for i in range(sources)}}
        state = staging.initialize(run, 10_000, planner)
        if v2:
            state["state_schema"] = staging.INVENTORY_STATE_SCHEMA
            state["planner"]["inventory_segments"] = {
                "schema": staging.INVENTORY_SEGMENT_SCHEMA,
                "pools": {"p": []},
            }
            atomic_json(run / "state.json", state)
        return run

    def add_plan_records(self, run, index, *, accepted=False, resident=False):
        identity = f"PLAN_{index:06d}"
        ref = {"artifact_id": f"BLOCK:p:s{index}:000000",
               "url": f"root://stage.invalid//{index}.lhe.gz",
               "kind": "lhe-block", "size_bytes": 20, "events": 5}
        task = {"task_id": identity, "stage": "plan", "parameters": {}}
        runtime_release = read_json(run / "run.json").get("runtime_release", "test")
        outcome = {"task_id": identity, "stage": "plan", "status": "ok",
                   "runtime_release": runtime_release, "outputs": [ref]}
        for kind, payload in (("tasks", task), ("results", outcome)):
            path = record_path(run, kind, identity)
            path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(path, payload)
        if accepted:
            path = shards._receipt_path(run, identity)
            path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(path, outcome)
        if resident:
            state = read_json(run / "state.json")
            state["staging"]["objects"][ref["url"]] = {
                key: ref[key] for key in ("artifact_id", "url", "kind", "size_bytes")}
            state["staging"]["objects"][ref["url"]]["resident"] = True
            state["staging"]["resident_bytes"] += ref["size_bytes"]
            atomic_json(run / "state.json", state)
        return identity, ref, outcome

    def accept(self, run, identity):
        with patch.object(shards.legacy, "_validate_planner_result"):
            return shards.accept_result(run, identity)

    def two_source_shard_run(self, root):
        spec_path, run = legacy_tests.ProductionLifecycle().fixture(root)
        spec = read_json(spec_path)
        spec["orchestration"] = "campaign_shards"
        spec["limits"]["shard_nodes"] = 2
        spec["sources"][0]["actual_events"] = 40
        spec["sources"].append(dict(spec["sources"][0], source_id="s2", seed=20))
        spec["campaigns"][0]["max_processing_nodes"] = 2
        atomic_json(spec_path, spec)
        test_core_shards.plan_legacy_fixture(spec_path, run)
        return run

    def test_post_acceptance_order_does_not_renumber_existing_blocks(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), v2=True)
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
            run = self.run_fixture(Path(directory), v2=True)
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

            with patch.object(shards.legacy, "_validate_planner_result"):
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
            run = self.run_fixture(Path(directory), v2=True)
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

    def test_migration_keeps_visible_prefix_then_appends_hidden_segments(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory))
            state = read_json(run / "state.json")
            state["planner"]["sources"].update({str(i): "skipped" for i in range(10)})
            state["planner"]["sources"]["10"] = "admitted"
            state["planner"]["sources"]["11"] = "admitted"
            state["planner"]["sources"]["12"] = "admitted"
            state["planner"]["streams"] = {"C:p": {
                "stride": 1, "offset": 0, "allocation_cursor": 1,
                "safe_cursor": 0, "closed": False}}
            for index in (12, 10):
                identity, ref, _ = self.add_plan_records(run, index, accepted=True)
                state["accepted"][identity] = {"stage": "plan"}
                state["staging"]["objects"][ref["url"]] = {
                    key: ref[key] for key in ("artifact_id", "url", "kind", "size_bytes")}
                state["staging"]["objects"][ref["url"]]["resident"] = True
                state["staging"]["resident_bytes"] += ref["size_bytes"]
            state["allocations"]["SHARD_C_000000"] = {
                "campaign": "C", "start_job_index": 0, "node_count": 1, "completed": False}
            manifest = shards._manifest_path(run, "SHARD_C_000000")
            manifest.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(manifest, {"allocation": {
                "campaign": "C", "start_job_index": 0, "nodes": [{"sources": [{"slot": 0,
                    "pool": "p", "block_refs": ["BLOCK:p:s10:000000"]}]}]}})
            manifest_before = manifest.read_bytes()
            atomic_json(run / "state.json", state)
            original_v1 = deepcopy(read_json(run / "state.json"))

            with patch.object(shards.legacy, "_validate_planner_result"):
                result = shards.migrate_inventory_v2(run)
            self.assertEqual(result, {"status": "migrated", "segments": 2, "hidden_segments": 1})
            first_v2 = deepcopy(read_json(run / "state.json"))
            self.assertEqual(shards.rollback_inventory_v2(run, original_v1_state=original_v1,
                                                          expected_v2_state=first_v2),
                             {"status": "rolled back"})
            self.assertEqual(read_json(run / "state.json"), original_v1)
            with patch.object(shards.legacy, "_validate_planner_result"):
                shards.migrate_inventory_v2(run)
            state = read_json(run / "state.json")
            self.assertEqual(state["allocations"]["SHARD_C_000000"], {
                "campaign": "C", "start_job_index": 0, "node_count": 1, "completed": False})
            self.assertEqual(state["planner"]["streams"]["C:p"], {
                "stride": 1, "offset": 0, "allocation_cursor": 1,
                "safe_cursor": 0, "closed": False})
            self.assertEqual(manifest.read_bytes(), manifest_before)
            blocks, _, _ = shards._inventory(run, read_json(run / "run.json"), state)
            self.assertEqual([(block.source_id, block.index) for block in blocks["p"]],
                             [("s10", 0), ("s12", 1)])

            identity, _, _ = self.add_plan_records(run, 11)
            self.assertEqual(self.accept(run, identity)["exit_code"], 0)
            blocks, _, _ = shards._inventory(run, read_json(run / "run.json"),
                                             read_json(run / "state.json"))
            self.assertEqual([(block.source_id, block.index) for block in blocks["p"]],
                             [("s10", 0), ("s12", 1), ("s11", 2)])
            changed = deepcopy(read_json(run / "state.json"))
            with self.assertRaisesRegex(RuntimeError, "ledger changed after migration"):
                shards.rollback_inventory_v2(run, original_v1_state=original_v1,
                                              expected_v2_state=first_v2)
            self.assertEqual(read_json(run / "state.json"), changed)

    def test_operator_checkpoint_migration_and_guarded_rollback(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=2)
            identity, _, _ = self.add_plan_records(run, 1, accepted=True, resident=True)
            state = read_json(run / "state.json")
            state["accepted"][identity] = {"stage": "plan"}
            atomic_json(run / "state.json", state)
            original = deepcopy(read_json(run / "state.json"))
            (run / ".controller-switch").mkdir()
            checkpoint = run / ".controller-switch" / "inventory-v2-checkpoint.json"

            with patch.object(shards.legacy, "_validate_planner_result"):
                result = shards.migrate_inventory_v2(run, checkpoint_path=checkpoint)
            self.assertEqual(result["status"], "migrated")
            saved = read_json(checkpoint)
            self.assertEqual(saved["original_v1_state"], original)
            self.assertEqual(saved["inventory_segments"]["pools"]["p"], [identity])
            expected = deepcopy(original)
            expected["state_schema"] = staging.INVENTORY_STATE_SCHEMA
            expected["planner"]["inventory_segments"] = {
                "schema": staging.INVENTORY_SEGMENT_SCHEMA,
                "pools": {"p": [identity]},
            }
            self.assertEqual(read_json(run / "state.json"), expected)

            rollback = shards.rollback_inventory_v2_from_checkpoint(run, checkpoint)
            self.assertEqual(rollback["status"], "rolled back")
            self.assertEqual(rollback["run_id"], "segments")
            self.assertEqual(read_json(run / "state.json"), original)
            with patch.object(shards.legacy, "_validate_planner_result"):
                self.assertEqual(shards.migrate_inventory_v2(
                    run, checkpoint_path=checkpoint)["status"], "migrated")
            identity0, _, _ = self.add_plan_records(run, 0)
            self.assertEqual(self.accept(run, identity0)["exit_code"], 0)
            with self.assertRaisesRegex(RuntimeError, "ledger changed after migration"):
                shards.rollback_inventory_v2_from_checkpoint(run, checkpoint)

    def test_operator_checkpoint_rejects_path_outside_run(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=1)
            with self.assertRaisesRegex(ValueError, "inside its run directory"):
                shards.migrate_inventory_v2(
                    run, checkpoint_path=Path(directory) / "outside-checkpoint.json")

    def test_operator_cli_migrates_and_rolls_back_checkpointed_ledger(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=1)
            (run / ".controller-switch").mkdir()
            checkpoint = run / ".controller-switch" / "inventory-v2-checkpoint.json"
            with redirect_stdout(StringIO()):
                self.assertEqual(mcprod_main([
                    "inventory", "migrate-v2", "--run-dir", str(run),
                    "--checkpoint", str(checkpoint)]), 0)
            self.assertEqual(read_json(run / "state.json")["state_schema"],
                             staging.INVENTORY_STATE_SCHEMA)
            with redirect_stdout(StringIO()):
                self.assertEqual(mcprod_main([
                    "inventory", "rollback-v2", "--run-dir", str(run),
                    "--checkpoint", str(checkpoint)]), 0)
            self.assertEqual(read_json(run / "state.json")["state_schema"],
                             staging.STATE_SCHEMA)

    def test_migration_contiguous_prefix_is_per_pool(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=4)
            spec = read_json(run / "run.json")
            spec["sources"][3]["pool"] = "q"
            atomic_json(run / "run.json", spec)
            state = read_json(run / "state.json")
            state["planner"]["sources"].update({"0": "admitted", "1": "admitted",
                                                  "2": "admitted", "3": "admitted"})
            for index in (0, 2, 3):
                identity, ref, _ = self.add_plan_records(run, index, accepted=True)
                state["accepted"][identity] = {"stage": "plan"}
                if index == 2:
                    state["staging"]["objects"][ref["url"]] = {
                        key: ref[key] for key in ("artifact_id", "url", "kind", "size_bytes")}
                    state["staging"]["objects"][ref["url"]]["resident"] = True
                    state["staging"]["resident_bytes"] += ref["size_bytes"]
            atomic_json(run / "state.json", state)
            with patch.object(shards.legacy, "_validate_planner_result"):
                shards.migrate_inventory_v2(run)
            segments = read_json(run / "state.json")["planner"]["inventory_segments"]["pools"]
            self.assertEqual(segments, {"p": ["PLAN_000000", "PLAN_000002"],
                                        "q": ["PLAN_000003"]})

    def test_v2_inventory_ordinals_feed_repeated_strided_pool_slots(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=5, v2=True)
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

    def test_v2_inventory_preserves_uneven_multiblock_tail(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=1, v2=True)
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

    def test_v2_shard_waits_for_second_node_without_advancing_cursors(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.two_source_shard_run(Path(directory))
            helper = test_core_shards.ShardFixture("runTest")
            shards.prepare_source(run, 0)
            helper.source_plan(run, count=2)
            self.assertEqual(shards.migrate_inventory_v2(run)["status"], "migrated")
            before = read_json(run / "state.json")
            result = shards.prepare_shard(run, "C", 0)
            self.assertEqual(result["exit_code"], 75)
            after = read_json(run / "state.json")
            self.assertEqual(after["planner"]["streams"], before["planner"]["streams"])
            self.assertEqual(after["planner"]["assigned_progress"], {"C": 0})
            self.assertNotIn("SHARD_C_000000", after["allocations"])

            identity, _, outcome = self.add_plan_records(run, 1)
            outcome["outputs"] = [dict(artifact_id=f"BLOCK:pool:s2:{index:06d}",
                url=f"root://stage.invalid//s2-block{index}.lhe.gz", kind="lhe-block",
                size_bytes=20, events=5) for index in range(2)]
            atomic_json(record_path(run, "results", identity), outcome)
            self.assertEqual(self.accept(run, identity)["exit_code"], 0)
            completed = shards.prepare_shard(run, "C", 0)
            self.assertEqual(completed["exit_code"], 0)
            allocation = read_json(shards._manifest_path(run, "SHARD_C_000000"))["allocation"]
            self.assertEqual(len(allocation["nodes"]), 2)
            self.assertEqual(read_json(run / "state.json")["planner"]["assigned_progress"], {"C": 2})

    def test_out_of_order_v2_shard_completion_holds_safe_cursor_at_prefix(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=1, v2=True)
            spec = read_json(run / "run.json")
            spec["campaigns"][0]["max_processing_nodes"] = 2
            atomic_json(run / "run.json", spec)
            identity, _, _ = self.add_plan_records(run, 0, accepted=True)
            state = read_json(run / "state.json")
            state["accepted"][identity] = {"stage": "plan"}
            state["planner"]["inventory_segments"]["pools"]["p"] = [identity]
            state["planner"]["streams"] = {"C::p": {
                "stride": 1, "offset": 0, "allocation_cursor": 2,
                "safe_cursor": 0, "closed": False}}
            for shard_index, end_cursor in ((0, 1), (1, 2)):
                name = f"SHARD_C_{shard_index:06d}"
                archive_id = f"ARCHIVE_{shard_index}"
                state["accepted"][archive_id] = {"stage": "archive"}
                receipt = shards._receipt_path(run, archive_id)
                receipt.parent.mkdir(parents=True, exist_ok=True)
                atomic_json(receipt, {"task_id": archive_id})
                state["allocations"][name] = {"campaign": "C", "start_job_index": shard_index,
                    "node_count": 1, "completed": False}
                manifest = shards._manifest_path(run, name)
                manifest.parent.mkdir(parents=True, exist_ok=True)
                atomic_json(manifest, {"tasks": [{"task_id": archive_id, "stage": "archive"}],
                    "allocation": {"end_cursors": {"C::p": end_cursor}}})
            atomic_json(run / "state.json", state)

            self.assertEqual(shards.complete_shard(run, "C", 1)["status"], "completed")
            state = read_json(run / "state.json")
            self.assertEqual(state["planner"]["streams"]["C::p"]["safe_cursor"], 0)
            self.assertFalse(state["planner"]["streams"]["C::p"]["closed"])
            self.assertEqual(shards.complete_shard(run, "C", 0)["status"], "completed")
            state = read_json(run / "state.json")
            self.assertEqual(state["planner"]["streams"]["C::p"]["safe_cursor"], 2)
            self.assertTrue(state["planner"]["streams"]["C::p"]["closed"])

    def test_orphan_shard_manifest_survives_append_and_commit_retry(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.two_source_shard_run(Path(directory))
            helper = test_core_shards.ShardFixture("runTest")
            shards.prepare_source(run, 0)
            helper.source_plan(run, count=8)
            self.assertEqual(shards.migrate_inventory_v2(run)["status"], "migrated")

            original_writer = staging._write_state
            with patch.object(staging, "_write_state", side_effect=OSError("injected state commit failure")):
                with self.assertRaisesRegex(OSError, "injected state commit failure"):
                    shards.prepare_shard(run, "C", 0)
            manifest = shards._manifest_path(run, "SHARD_C_000000")
            orphan_bytes = manifest.read_bytes()
            self.assertNotIn("SHARD_C_000000", read_json(run / "state.json")["allocations"])

            identity, _, outcome = self.add_plan_records(run, 1)
            outcome["outputs"] = [dict(artifact_id="BLOCK:pool:s2:000000",
                url="root://stage.invalid//s2-block0.lhe.gz", kind="lhe-block",
                size_bytes=20, events=5)]
            atomic_json(record_path(run, "results", identity), outcome)
            self.assertEqual(self.accept(run, identity)["exit_code"], 0)
            with patch.object(staging, "_write_state", wraps=original_writer):
                result = shards.prepare_shard(run, "C", 0)
            self.assertEqual(result["exit_code"], 0)
            self.assertEqual(manifest.read_bytes(), orphan_bytes)
            allocation = read_json(manifest)["allocation"]
            self.assertEqual(allocation["start_job_index"], 0)
            self.assertEqual(allocation["start_cursors"], {"C::pool": 0})
            self.assertEqual(len(allocation["nodes"]), 2)
            self.assertEqual(read_json(run / "state.json")["planner"]["streams"]["C::pool"]["allocation_cursor"], 4)

    def test_v2_inventory_allocates_the_final_partial_ten_node_range(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=1, v2=True)
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
            streams["C::p"]["safe_cursor"] = 10
            allocation = allocate_shard(spec["campaigns"][0], blocks, streams,
                                         10, 1, exhausted)
            self.assertEqual(len(allocation["nodes"]), 1)
            self.assertEqual(allocation["nodes"][0]["job_index"], 10)
            self.assertEqual(allocation["nodes"][0]["sources"][0]["block_refs"],
                             ["BLOCK:p:s0:000010"])

    def test_cleanup_waits_for_safe_cursor_of_later_inventory_ordinals(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), v2=True)
            state = read_json(run / "state.json")
            segments = []
            for index in (10, 12, 11):
                identity, ref, outcome = self.add_plan_records(run, index, accepted=True)
                segments.append(identity)
                state["accepted"][identity] = {"stage": "plan"}
                archive_id = f"SOURCE_ARCHIVE_{index:06d}"
                state["accepted"][archive_id] = {"stage": "archive"}
                state["staging"]["objects"][ref["url"]] = {
                    key: ref[key] for key in ("artifact_id", "url", "kind", "size_bytes")}
                state["staging"]["objects"][ref["url"]]["resident"] = True
                state["staging"]["resident_bytes"] += ref["size_bytes"]
            state["planner"]["inventory_segments"]["pools"]["p"] = segments
            state["planner"]["streams"] = {"C::p": {
                "stride": 1, "offset": 0, "allocation_cursor": 3,
                "safe_cursor": 1, "closed": False}}
            atomic_json(run / "state.json", state)
            refs = shards._cleanup_refs(run, read_json(run / "run.json"), state, "p")
            self.assertEqual([ref["artifact_id"] for ref in refs], ["BLOCK:p:s10:000000"])

    def test_v2_inventory_generates_child_dag_from_persisted_segments(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            helper = test_core_shards.ShardFixture("runTest")
            run = helper.fixture(Path(directory), campaigns=1)
            shards.prepare_source(run, 0)
            helper.source_plan(run)
            legacy_blocks, _, _ = shards._inventory(run, read_json(run / "run.json"),
                                                     read_json(run / "state.json"))
            with patch.object(shards.legacy, "_validate_planner_result", wraps=shards.legacy._validate_planner_result):
                self.assertEqual(shards.migrate_inventory_v2(run)["status"], "migrated")
            state = read_json(run / "state.json")
            blocks, _, _ = shards._inventory(run, read_json(run / "run.json"), state)
            self.assertEqual([(b.artifact_id, b.index) for b in blocks["pool"]],
                             [(b.artifact_id, b.index) for b in legacy_blocks["pool"]])
            result = shards.prepare_shard(run, "C", 0)
            self.assertEqual(result["exit_code"], 0)
            manifest = read_json(shards._manifest_path(run, "SHARD_C_000000"))
            used = {artifact_id for node in manifest["allocation"]["nodes"]
                    for slot in node["sources"] for artifact_id in slot["block_refs"]}
            self.assertTrue(used)
            self.assertTrue(used <= {block.artifact_id for block in blocks["pool"]})
            self.assertTrue(Path(result["dag"]).is_file())
            manifest_path = shards._manifest_path(run, "SHARD_C_000000")
            manifest_before = manifest_path.read_bytes()
            Path(result["dag"]).unlink()
            retried = shards.prepare_shard(run, "C", 0)
            self.assertEqual(retried["exit_code"], 0)
            self.assertEqual(manifest_path.read_bytes(), manifest_before)
            self.assertTrue(Path(retried["dag"]).is_file())

    def test_v2_state_is_fenced_from_v1_transaction_writer(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), v2=True)
            before = (run / "state.json").read_bytes()
            baseline_src = Path(__file__).resolve().parents[2] / "v06_00-staging-scheduler" / "src"
            env = dict(os.environ, PYTHONPATH=str(baseline_src))
            code = ("import sys\nfrom full_mc.application.staging import transaction\n"
                    "with transaction(sys.argv[1]):\n    pass\n")
            result = subprocess.run([sys.executable, "-c", code, str(run)], env=env,
                                    text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unsupported staging state schema", result.stderr)
            self.assertEqual((run / "state.json").read_bytes(), before)

    def test_v2_segments_must_match_accepted_plan_markers(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), v2=True)
            state = read_json(run / "state.json")
            state["planner"]["inventory_segments"]["pools"]["p"] = ["PLAN_000000"]
            atomic_json(run / "state.json", state)
            with self.assertRaisesRegex(ValueError, "accepted PLAN result"):
                with staging.transaction(run):
                    self.fail("invalid v2 segment must not yield")

    def test_idempotent_migration_still_validates_frozen_pool_membership(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.run_fixture(Path(directory), sources=1, v2=True)
            identity, _, _ = self.add_plan_records(run, 0, accepted=True)
            state = read_json(run / "state.json")
            state["accepted"][identity] = {"stage": "plan"}
            state["planner"]["inventory_segments"]["pools"] = {"wrong-pool": [identity]}
            atomic_json(run / "state.json", state)
            before = (run / "state.json").read_bytes()
            with self.assertRaisesRegex(RuntimeError, "pools disagree with the frozen source spec"):
                shards.migrate_inventory_v2(run)
            self.assertEqual((run / "state.json").read_bytes(), before)

    def test_invalid_source_or_pool_segment_aborts_before_transaction_commit(self):
        for identity, pool in (("PLAN_999999", "p"), ("PLAN_000000", "wrong-pool")):
            with self.subTest(identity=identity, pool=pool):
                with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
                    run = self.run_fixture(Path(directory), sources=1, v2=True)
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
