"""Offline graph and transaction contracts for finite campaign shards."""
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application import shards, staging
from full_mc.io import atomic_json, read_json
from full_mc.local_paths import record_path
from full_mc.planning.shards import child_path
from full_mc.planning.shards import shard_name, source_name, source_order
from tests.mcprod_fixtures import run_spec_fixture
from tests.mcprod_fixtures import remote_io_blocked


class ShardFixture(unittest.TestCase):
    def fixture(self, directory, *, nodes=2, campaigns=2, disjoint_pools=False):
        spec_path, run = run_spec_fixture(Path(directory))
        spec = read_json(spec_path)
        spec["orchestration"] = "campaign_shards"
        spec["limits"]["shard_nodes"] = 1
        spec["sources"][0]["actual_events"] = 40
        spec["campaigns"][0]["max_processing_nodes"] = nodes
        if campaigns == 2:
            spec["campaigns"].append(dict(deepcopy(spec["campaigns"][0]), name="D"))
        if disjoint_pools:
            spec["sources"].append(dict(spec["sources"][0], pool="q", source_id="q1", seed=20))
            for slot in spec["campaigns"][1]["sources"]:
                slot["pool"] = "q"
        atomic_json(spec_path, spec)
        with patch.object(shards, "resolve_proxy_path", return_value="/afs/test/proxy"):
            shards.plan_run(spec_path, run)
        return run

    def task(self, run, identity):
        return read_json(record_path(run, "tasks", identity))

    def accepted(self, run, identity, **extra):
        task = self.task(run, identity)
        outcome = dict(task_id=identity, stage=task["stage"], status="ok",
                       runtime_release="test-release", outputs=[])
        outcome.update(extra)
        atomic_json(record_path(run, "results", identity), outcome)
        result = shards.accept_result(run, identity)
        self.assertEqual(result["exit_code"], 0)
        return outcome

    def source_plan(self, run, index=0, count=8):
        identity = f"PLAN_{index:06d}"
        task = self.task(run, identity)
        p = task["parameters"]
        original = dict(p["original_output"], events=40, size_bytes=100)
        outputs = [dict(artifact_id=f"BLOCK:{p['pool_name']}:{p['group_id']}:{i:06d}",
                        url=p["block_output_dir"] + f"/block_{p['group_id']}_{i:06d}.lhe.gz", kind="lhe-block",
                        events=5, size_bytes=20) for i in range(count)]
        return self.accepted(run, identity, outputs=outputs, originals=[original],
                             actual_input_events=40, planned_events=count * 5)

    def archive_result(self, run, identity):
        task = self.task(run, identity)
        state = read_json(run / "state.json")
        members = []
        for i, ref in enumerate(task["inputs"]):
            accepted = shards._find_producer(run, state, ref["artifact_id"])
            members.append(dict(accepted, member=f"{i:06d}_member.gz"))
        return self.accepted(run, identity, outputs=[dict(task["outputs"][0], size_bytes=50)], members=members)

    def reclaim_result(self, run, identity):
        task = self.task(run, identity)
        reclaimed = []
        for ref in task["inputs"]:
            objects = [dict(kind="payload", url=ref["url"], disposition="removed")]
            if "sidecar_url" in ref:
                objects.append(dict(kind="sidecar", url=ref["sidecar_url"], disposition="already-missing"))
            reclaimed.append(dict(artifact_id=ref["artifact_id"], objects=objects))
        return self.accepted(run, identity, reclaimed=reclaimed)

    def source_done(self, run, index=0):
        identity = f"SOURCE_ARCHIVE_{index:06d}"
        self.assertEqual(shards.prepare_source_product(run, identity)["exit_code"], 0)
        self.archive_result(run, identity)
        shards.prepare_source_product(run, f"SOURCE_RECLAIM_{index:06d}")
        self.reclaim_result(run, f"SOURCE_RECLAIM_{index:06d}")
        self.assertEqual(shards.complete_source(run, index)["exit_code"], 0)

class ShardGraphTests(ShardFixture):
    def test_root_is_finite_independent_and_explicitly_configured(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            text = (run / "run.dag").read_text()
            self.assertEqual(text.count("SUBDAG EXTERNAL"), 5)
            self.assertNotIn("WAVE_", text)
            for line in text.splitlines():
                if line.startswith("PARENT") and " CHILD SHARD_" in line:
                    self.fail("shard has a root completion dependency")
            self.assertEqual(text.count("JOB CLEANUP_pool_"), 1)
            configured = read_json(run / "run.json")["scheduler"]["maxjobs"]
            for category in ("lhe_planning", "block_processing", "lhe_coordination"):
                self.assertIn(f"MAXJOBS {category} {configured[category]}", text)
            self.assertIn("SCRIPT DEFER 75 60 POST SOURCE_000000", text)
            self.assertIn("--return-code $RETURN", text)
            config = (run / "dagman.config").read_text()
            for option in ("DAGMAN_MAX_PRE_SCRIPTS = 20", "DAGMAN_MAX_POST_SCRIPTS = 20",
                           "DAGMAN_GENERATE_SUBDAG_SUBMITS = TRUE", "DAGMAN_AUTO_RESCUE = TRUE",
                           "DAGMAN_ALWAYS_RUN_POST = TRUE"):
                self.assertIn(option, config)
            self.assertIn("export PATH=/usr/bin:/bin", (run / "controller.sh").read_text())

    def test_plan_post_unlocks_shard_before_source_archive_completion(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            with remote_io_blocked():
                self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)
                self.source_plan(run)
                self.assertEqual(shards.prepare_shard(run, "C", 0)["exit_code"], 0)
            state = read_json(run / "state.json")
            self.assertEqual(state["planner"]["sources"]["0"], "admitted")
            self.assertNotIn("SOURCE_ARCHIVE_000000", state["accepted"])
            source_dag = child_path(run, "SOURCE_000000").read_text()
            self.assertIn("PARENT PLAN_000000 CHILD SOURCE_ARCHIVE_000000", source_dag)
            self.assertIn("PARENT SOURCE_ARCHIVE_000000 CHILD SOURCE_RECLAIM_000000", source_dag)
            child = child_path(run, "SHARD_C_000000").read_text()
            self.assertIn("CONFIG " + str(run / "dagman.config"), child)
            self.assertIn("JOB PROCESS_C_", child)
            self.assertNotIn(" PRE PROCESS_", child)

    def test_assignment_retry_repairs_dag_and_keeps_inputs_and_seeds(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            shards.prepare_source(run, 0)
            self.source_plan(run)
            shards.prepare_shard(run, "C", 0)
            path = child_path(run, "SHARD_C_000000")
            before = path.read_bytes()
            manifest = shards._manifest_path(run, "SHARD_C_000000")
            frozen = manifest.read_bytes()
            path.unlink()
            shards.prepare_shard(run, "C", 0)
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(manifest.read_bytes(), frozen)

    def test_worker_failure_and_failed_child_never_commit_success(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            before = (run / "state.json").read_bytes()
            self.assertEqual(shards.accept_result(run, "unwritten", 42)["exit_code"], 42)
            self.assertEqual(shards.complete_shard(run, "C", 0, 7)["exit_code"], 7)
            self.assertEqual(shards.complete_source(run, 0, 8)["exit_code"], 8)
            for failed in (shards.accept_result(run, "unwritten", 75),
                           shards.complete_shard(run, "C", 0, 75),
                           shards.complete_source(run, 0, 75)):
                self.assertEqual(failed["exit_code"], 1)
                self.assertEqual(failed["worker_return_code"], 75)
            self.assertEqual((run / "state.json").read_bytes(), before)

    def test_late_source_archive_freezes_authoritative_original(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            shards.prepare_source(run, 0)
            self.assertFalse((record_path(run, "tasks", "SOURCE_ARCHIVE_000000")).exists())
            self.source_plan(run)
            self.assertEqual(shards.prepare_source_product(run, "SOURCE_ARCHIVE_000000")["exit_code"], 0)
            task = self.task(run, "SOURCE_ARCHIVE_000000")
            self.assertEqual(task["inputs"][0]["events"], 40)
            self.assertEqual(task["inputs"][0]["size_bytes"], 100)
            self.assertNotIn("staging_claim_bytes", task["parameters"])
            shards.prepare_source_product(run, "SOURCE_ARCHIVE_000000")


class ShardAcceptanceTests(ShardFixture):

    def test_pool_inventory_follows_plan_acceptance_order(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            spec = read_json(run / "run.json")
            spec["sources"].extend([dict(spec["sources"][0], source_id="s2", seed=20),
                                    dict(spec["sources"][0], source_id="s3", pool="q", seed=21)])
            self.assertEqual(source_order(spec), [0, 2, 1])
            state = read_json(run / "state.json")
            segments = state["planner"]["inventory_segments"]["pools"]
            segments["q"] = []
            for index in (1, 2, 0):
                source = spec["sources"][index]
                identity = f"PLAN_{index:06d}"
                ref = dict(artifact_id=f"BLOCK:{source['pool']}:{source['source_id']}:000000",
                           url=f"root://stage.example///{index}.lhe.gz", kind="lhe-block", events=5, size_bytes=20)
                state["accepted"][identity] = {"stage": "plan"}
                segments[source["pool"]].append(identity)
                atomic_json(shards._receipt_path(run, identity), {"outputs": [ref]})
                if index == 2:
                    blocks, _, exhausted = shards._inventory(run, spec, state)
                    self.assertEqual([b.source_id for b in blocks["pool"]], ["s2"])
                    self.assertEqual([b.source_id for b in blocks["q"]], ["s3"])
                    self.assertIn("q", exhausted)
            blocks, _, _ = shards._inventory(run, spec, state)
            self.assertEqual([(b.source_id, b.index) for b in blocks["pool"]], [("s2", 0), ("s1", 1)])

    def test_lock_contention_defers_pre_and_post_without_partial_state(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            before = (run / "state.json").read_bytes()
            with patch.object(staging, "transaction", side_effect=staging.LockBusy()):
                self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 75)
                self.assertEqual(shards.finalize_run(run)["exit_code"], 75)
            self.assertEqual((run / "state.json").read_bytes(), before)

    def test_generated_plan_must_match_the_accepted_lhe_original(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            spec = read_json(run / "run.json")
            # Supply the resolved profile directly to isolate the acceptance
            # contract from catalogue and HELAC execution, which have own tests.
            spec["sources"][0]["resolved_generator_profile"] = {"planning_events_hint": 40}
            atomic_json(run / "run.json", spec)
            shards.prepare_source(run, 0)
            task = self.task(run, "LHE_000000")
            original = dict(task["outputs"][0], events=40, size_bytes=100, sidecar_size_bytes=17)
            physical = [{k: original[k] for k in ("artifact_id", "url", "kind", "size_bytes")},
                        dict(artifact_id="LHE_000000:sidecar", url=original["sidecar_url"], kind="sidecar", size_bytes=17)]
            self.accepted(run, "LHE_000000", outputs=[original], staged_objects=physical)
            plan = self.task(run, "PLAN_000000")
            blocks = [dict(artifact_id=f"BLOCK:pool:s1:{i:06d}", url=plan["parameters"]["block_output_dir"] + f"/block_s1_{i:06d}.lhe.gz",
                           kind="lhe-block", events=5, size_bytes=20) for i in range(8)]
            retained = dict(original, events=41)
            outcome = dict(task_id="PLAN_000000", stage="plan", runtime_release="test-release", status="ok",
                           outputs=blocks, originals=[retained], actual_input_events=41, planned_events=40,
                           staged_objects=[{k: r[k] for k in ("artifact_id", "url", "kind", "size_bytes")} for r in blocks])
            atomic_json(record_path(run, "results", "PLAN_000000"), outcome)
            before = read_json(run / "state.json")
            with self.assertRaisesRegex(RuntimeError, "accepted LHE"):
                shards.accept_result(run, "PLAN_000000")
            self.assertEqual(read_json(run / "state.json"), before)
            outcome.update(originals=[original], actual_input_events=40)
            atomic_json(record_path(run, "results", "PLAN_000000"), outcome)
            self.assertEqual(shards.accept_result(run, "PLAN_000000")["exit_code"], 0)
            self.assertIn("PLAN_000000", read_json(run / "state.json")["accepted"])

    def test_distinct_oversized_blocks_refill_repeated_same_pool_slots(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory, nodes=1, campaigns=1)
            spec = read_json(run / "run.json")
            spec["sources"].append(dict(spec["sources"][0], source_id="s2", seed=20, actual_events=40))
            for slot in spec["campaigns"][0]["sources"]:
                slot["lhe_budget"] = 1
            spec["block_events"] = 100
            atomic_json(run / "run.json", spec)
            shards.prepare_source(run, 0)
            task = self.task(run, "PLAN_000000")
            original = dict(task["parameters"]["original_output"], events=40, size_bytes=100)
            block = dict(artifact_id="BLOCK:pool:s1:000000", url=task["parameters"]["block_output_dir"] + "/block_s1_000000.lhe.gz",
                         kind="lhe-block", events=40, size_bytes=20)
            refs = [original, block]
            self.accepted(run, "PLAN_000000", outputs=[block], originals=[original], actual_input_events=40, planned_events=40,
                          staged_objects=[{k: r[k] for k in ("artifact_id", "url", "kind", "size_bytes")} for r in refs])
            self.source_done(run)
            self.assertEqual(shards.prepare_shard(run, "C", 0)["exit_code"], 75)
            self.assertEqual(shards.prepare_source(run, 1)["exit_code"], 0)


if __name__ == "__main__":
    Path("/tmp/chiw").mkdir(exist_ok=True)
    unittest.main()
