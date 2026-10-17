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
from full_mc.planning.shards import shard_name, source_name, source_order
from tests import test_core_production as legacy_tests
from tests.test_core_production import remote_io_blocked


class ShardFixture(unittest.TestCase):
    def fixture(self, directory, *, nodes=2, campaigns=2, disjoint_pools=False):
        spec_path, run = legacy_tests.ProductionLifecycle().fixture(Path(directory))
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
        with patch("full_mc.application.shards.resolve_proxy_path", return_value="/afs/test/proxy"):
            shards.plan_run(spec_path, run)
        return run

    def task(self, run, identity):
        return read_json(run / "tasks" / (identity + ".json"))

    def accepted(self, run, identity, **extra):
        task = self.task(run, identity)
        outcome = dict(task_id=identity, stage=task["stage"], status="ok",
                       runtime_release="test-release", outputs=[])
        outcome.update(extra)
        atomic_json(run / "results" / (identity + ".json"), outcome)
        result = shards.accept_result(run, identity)
        self.assertEqual(result["exit_code"], 0)
        return outcome

    def source_plan(self, run, index=0, count=8):
        identity = f"PLAN_{index:06d}"
        task = self.task(run, identity)
        p = task["parameters"]
        original = dict(p["original_output"], events=40, size_bytes=100)
        outputs = [dict(artifact_id=f"BLOCK:{p['pool_name']}:{p['group_id']}:{i:06d}",
                        url=p["block_output_dir"] + f"/block{i}.lhe.gz", kind="lhe-block",
                        events=5, size_bytes=20) for i in range(count)]
        objects = [{k: r[k] for k in ("artifact_id", "url", "kind", "size_bytes")}
                   for r in outputs + [original]]
        return self.accepted(run, identity, outputs=outputs, originals=[original],
                             actual_input_events=40, planned_events=count * 5, staged_objects=objects)

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
        self.assertEqual(shards.admit_producer(run, identity)["exit_code"], 0)
        self.archive_result(run, identity)
        self.reclaim_result(run, f"SOURCE_RECLAIM_{index:06d}")
        self.assertEqual(shards.complete_source(run, index)["exit_code"], 0)

    def mix_result(self, run, identity):
        self.assertEqual(shards.admit_producer(run, identity)["exit_code"], 0)
        task = self.task(run, identity)
        output = dict(task["outputs"][0], size_bytes=64, sidecar_size_bytes=17,
                      events=task["parameters"]["target_mixed_events"])
        objects = [{k: output[k] for k in ("artifact_id", "url", "kind", "size_bytes")},
                   dict(artifact_id=output["artifact_id"] + ":sidecar", url=output["sidecar_url"], kind="sidecar", size_bytes=17)]
        return self.accepted(run, identity, outputs=[output], staged_objects=objects)

    def finish_shard(self, run, campaign, index, *, mix=True):
        descriptor = read_json(shards._manifest_path(run, shard_name(campaign, index)))
        for task in descriptor["tasks"]:
            identity = task["task_id"]
            if task["stage"] == "mix":
                if mix:
                    self.mix_result(run, identity)
            elif task["stage"] == "miniaod":
                output = dict(task["outputs"][0], size_bytes=100, events=1, expected_events=1, partial=False, count_source="edmFileUtil")
                self.accepted(run, identity, outputs=[output], partial=False)
            elif task["stage"] == "archive":
                self.archive_result(run, identity)
            elif task["stage"] == "reclaim":
                self.reclaim_result(run, identity)
        return shards.complete_shard(run, campaign, index)


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
            self.assertEqual(text.count("JOB CLEANUP_pool_"), 6)
            self.assertIn("MAXJOBS lhe_planning 2", text)
            self.assertIn("MAXJOBS block_processing 6", text)
            self.assertIn("MAXJOBS lhe_coordination 20", text)
            self.assertIn("SCRIPT DEFER 75 60 POST SOURCE_000000", text)
            self.assertIn("--return-code $RETURN", text)
            config = (run / "dagman.config").read_text()
            for option in ("DAGMAN_MAX_PRE_SCRIPTS = 20", "DAGMAN_MAX_POST_SCRIPTS = 20",
                           "DAGMAN_GENERATE_SUBDAG_SUBMITS = TRUE", "DAGMAN_AUTO_RESCUE = TRUE",
                           "DAGMAN_ALWAYS_RUN_POST = FALSE"):
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
            source_dag = (run / "subdags" / "SOURCE_000000.dag").read_text()
            self.assertIn("PARENT PLAN_000000 CHILD SOURCE_ARCHIVE_000000", source_dag)
            self.assertIn("PARENT SOURCE_ARCHIVE_000000 CHILD SOURCE_RECLAIM_000000", source_dag)
            child = (run / "subdags" / "SHARD_C_000000.dag").read_text()
            self.assertIn("CONFIG " + str(run / "dagman.config"), child)
            self.assertIn("SCRIPT DEFER 75 60 PRE MIX_C_", child)
            self.assertNotIn("SCRIPT DEFER 75 60 PRE MINIAOD_", child)
            self.assertIn("PARENT MINIAOD_", child)

    def test_assignment_retry_repairs_dag_and_keeps_inputs_seeds_and_claims(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            shards.prepare_source(run, 0)
            self.source_plan(run)
            shards.prepare_shard(run, "C", 0)
            path = run / "subdags" / "SHARD_C_000000.dag"
            before = path.read_bytes()
            manifest = shards._manifest_path(run, "SHARD_C_000000")
            frozen = manifest.read_bytes()
            path.unlink()
            shards.prepare_shard(run, "C", 0)
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(manifest.read_bytes(), frozen)
            mix = next(t for t in read_json(manifest)["tasks"] if t["stage"] == "mix")
            shards.admit_producer(run, mix["task_id"])
            state = read_json(run / "state.json")
            shards.admit_producer(run, mix["task_id"])
            self.assertEqual(read_json(run / "state.json")["staging"], state["staging"])

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
            self.assertFalse((run / "tasks" / "SOURCE_ARCHIVE_000000.json").exists())
            self.source_plan(run)
            self.assertEqual(shards.admit_producer(run, "SOURCE_ARCHIVE_000000")["exit_code"], 0)
            task = self.task(run, "SOURCE_ARCHIVE_000000")
            self.assertEqual(task["inputs"][0]["events"], 40)
            self.assertEqual(task["inputs"][0]["size_bytes"], 100)
            self.assertNotIn("staging_claim_bytes", task["parameters"])
            shards.admit_producer(run, "SOURCE_ARCHIVE_000000")

    def test_minimum_capacity_is_a_hard_configuration_error(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            spec_path, run = legacy_tests.ProductionLifecycle().fixture(Path(directory))
            spec = read_json(spec_path)
            spec["storage"].update(staging_bytes=2999, reserve_bytes=0)
            atomic_json(spec_path, spec)
            with self.assertRaisesRegex(ValueError, "complementary"):
                shards.plan_run(spec_path, run)
            self.assertFalse(run.exists())


class ShardAcceptanceTests(ShardFixture):
    def test_independent_pool_campaigns_do_not_reserve_each_others_complements(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory, disjoint_pools=True)
            with staging.transaction(run) as state:
                state["staging"]["usable_bytes"] = 3000
            self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)

    def test_minimum_capacity_includes_policy_headroom_for_every_complement(self):
        spec = {"limits": {"source_lhe_bytes": 1000, "source_block_bytes": 1000,
                           "mixed_hepmc_bytes": 1000, "policy_headroom_bytes": 3000, "shard_nodes": 1},
                "campaigns": [{"sources": [{"pool": "p"}, {"pool": "q"}], "max_processing_nodes": 1}]}
        self.assertEqual(shards._minimum_capacity(spec), 7000)

    def test_pool_prefix_is_frozen_despite_other_pool_and_source_completion_order(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            spec = read_json(run / "run.json")
            spec["sources"].extend([dict(spec["sources"][0], source_id="s2", seed=20),
                                    dict(spec["sources"][0], source_id="s3", pool="q", seed=21)])
            self.assertEqual(source_order(spec), [0, 2, 1])
            state = read_json(run / "state.json")
            for index in (1, 2, 0):
                source = spec["sources"][index]
                identity = f"PLAN_{index:06d}"
                ref = dict(artifact_id=f"BLOCK:{source['pool']}:{source['source_id']}:000000",
                           url=f"root://stage.example///{index}.lhe.gz", kind="lhe-block", events=5, size_bytes=20)
                state["accepted"][identity] = {"stage": "plan"}
                atomic_json(shards._receipt_path(run, identity), {"outputs": [ref]})
                if index == 2:
                    blocks, _, exhausted = shards._inventory(run, spec, state)
                    self.assertEqual(blocks.get("pool", []), [])
                    self.assertEqual([b.source_id for b in blocks["q"]], ["s3"])
                    self.assertIn("q", exhausted)
            blocks, _, _ = shards._inventory(run, spec, state)
            self.assertEqual([(b.source_id, b.index) for b in blocks["pool"]], [("s1", 0), ("s2", 1)])

    def test_lock_contention_defers_pre_and_post_without_partial_state(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            before = (run / "state.json").read_bytes()
            with patch.object(staging, "transaction", side_effect=BlockingIOError()):
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
            blocks = [dict(artifact_id=f"BLOCK:pool:s1:{i:06d}", url=plan["parameters"]["block_output_dir"] + f"/{i}.lhe.gz",
                           kind="lhe-block", events=5, size_bytes=20) for i in range(8)]
            retained = dict(original, events=41)
            outcome = dict(task_id="PLAN_000000", stage="plan", runtime_release="test-release", status="ok",
                           outputs=blocks, originals=[retained], actual_input_events=41, planned_events=40,
                           staged_objects=[{k: r[k] for k in ("artifact_id", "url", "kind", "size_bytes")} for r in blocks])
            atomic_json(run / "results" / "PLAN_000000.json", outcome)
            before = read_json(run / "state.json")["staging"]
            with self.assertRaisesRegex(RuntimeError, "accepted LHE"):
                shards.accept_result(run, "PLAN_000000")
            self.assertEqual(read_json(run / "state.json")["staging"], before)
            outcome.update(originals=[original], actual_input_events=40)
            atomic_json(run / "results" / "PLAN_000000.json", outcome)
            self.assertEqual(shards.accept_result(run, "PLAN_000000")["exit_code"], 0)
            self.assertEqual(read_json(run / "state.json")["staging"]["resident_bytes"], 277)

    def test_relocated_mixed_sidecar_is_rejected(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            shards.prepare_source(run, 0)
            self.source_plan(run)
            shards.prepare_shard(run, "C", 0)
            identity = "MIX_C_JOB000000_BLOCK000000"
            shards.admit_producer(run, identity)
            output = dict(self.task(run, identity)["outputs"][0], size_bytes=64, events=1,
                          sidecar_url="root://unowned.example///other.json", sidecar_size_bytes=17)
            atomic_json(run / "results" / (identity + ".json"), dict(
                task_id=identity, stage="mix", runtime_release="test-release", status="ok", outputs=[output], staged_objects=[]))
            with self.assertRaisesRegex(RuntimeError, "sidecar URL"):
                shards.accept_result(run, identity)
            self.assertNotIn(identity, read_json(run / "state.json")["accepted"])

    def test_miniaod_requires_edm_count_and_frozen_detector_provenance(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            shards.prepare_source(run, 0)
            self.source_plan(run)
            shards.prepare_shard(run, "C", 0)
            self.mix_result(run, "MIX_C_JOB000000_BLOCK000000")
            descriptor = read_json(shards._manifest_path(run, "SHARD_C_000000"))
            task = next(t for t in descriptor["tasks"] if t["stage"] == "miniaod")
            output = dict(task["outputs"][0], size_bytes=100, events=1, expected_events=1, partial=False,
                          count_source="filename")
            outcome = dict(task_id=task["task_id"], stage="miniaod", runtime_release="test-release", status="ok",
                           outputs=[output], partial=False)
            path = run / "results" / (task["task_id"] + ".json")
            atomic_json(path, outcome)
            with self.assertRaisesRegex(RuntimeError, "count or provenance"):
                shards.accept_result(run, task["task_id"])
            output["count_source"] = "edmFileUtil"
            output["detector_profile"] = {"profile_id": "different"}
            atomic_json(path, outcome)
            with self.assertRaisesRegex(RuntimeError, "count or provenance"):
                shards.accept_result(run, task["task_id"])

    def test_distinct_oversized_blocks_refill_repeated_same_pool_slots(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory, nodes=1, campaigns=1)
            spec = read_json(run / "run.json")
            spec["sources"].append(dict(spec["sources"][0], source_id="s2", seed=20))
            for slot in spec["campaigns"][0]["sources"]:
                slot["lhe_budget"] = 1
            spec["block_events"] = 100
            atomic_json(run / "run.json", spec)
            shards.prepare_source(run, 0)
            task = self.task(run, "PLAN_000000")
            original = dict(task["parameters"]["original_output"], events=40, size_bytes=100)
            block = dict(artifact_id="BLOCK:pool:s1:000000", url=task["parameters"]["block_output_dir"] + "/block.lhe.gz",
                         kind="lhe-block", events=40, size_bytes=20)
            refs = [original, block]
            self.accepted(run, "PLAN_000000", outputs=[block], originals=[original], actual_input_events=40, planned_events=40,
                          staged_objects=[{k: r[k] for k in ("artifact_id", "url", "kind", "size_bytes")} for r in refs])
            self.source_done(run)
            self.assertEqual(shards.prepare_shard(run, "C", 0)["exit_code"], 75)
            self.assertEqual(shards.prepare_source(run, 1)["exit_code"], 0)

    def test_retained_inputs_cannot_endlessly_defer_when_nothing_can_free_bytes(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as directory:
            run = self.fixture(directory)
            spec = read_json(run / "run.json")
            spec["sources"].append(dict(spec["sources"][0], source_id="s2", seed=20))
            for campaign in spec["campaigns"]:
                campaign["max_processing_nodes"] = 4
            atomic_json(run / "run.json", spec)
            shards.prepare_source(run, 0)
            self.source_plan(run, count=1)
            self.source_done(run)
            with staging.transaction(run) as state:
                state["staging"]["usable_bytes"] = 3000
            with self.assertRaisesRegex(RuntimeError, "no admitted source, allocated shard or eligible cleanup"):
                shards.prepare_source(run, 1)


if __name__ == "__main__":
    Path("/tmp/chiw").mkdir(exist_ok=True)
    unittest.main()
