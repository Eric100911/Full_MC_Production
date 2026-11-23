"""Offline v3 CERN worker, storage, DAG, acceptance and IHEP handoff contracts."""
from copy import deepcopy
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from full_mc.application import dagman_recovery, shards, staging
from full_mc.application.handoff import export_handoff
from full_mc.application.recovery_records import reconstruct
from full_mc.application.site_validation import REQUIRED_CHECKS, validate_site
from full_mc.domain.streams import allocate_shard
from full_mc.io import atomic_json, read_json
from full_mc.local_paths import record_path
from full_mc.planning.shards import child_path
from full_mc.runtime.bootstrap import unpack_runtime
from full_mc.runtime.packages import prepare_runtime
from full_mc.storage.transfer import TransferClient
from full_mc.workers import merge as merge_worker, processing, reclaim
from tests.mcprod_fixtures import remote_io_blocked, run_spec_fixture


class FusedProduction(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(dir="/tmp/chiw")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.spec_path, self.run = run_spec_fixture(self.root)
        spec = read_json(self.spec_path)
        spec.update(orchestration="campaign_shards", block_events=5)
        spec["storage"].update(staging_bytes=1, reserve_bytes=2)
        spec["limits"].update(shard_nodes=1, miniaod_bytes=1000)
        spec["resources"] = {"processing": {"disk_bytes": 6_000_000_000,
                                           "memory_mb": 7000, "flavour": "tomorrow"}}
        c = spec["campaigns"][0]
        c.update(max_processing_nodes=2, global_job_index_start=15, merge_events=2)
        spec["campaigns"].append(dict(deepcopy(c), name="D", analysis="JUP",
                                      max_processing_nodes=1, merge_events=2))
        spec["sources"][0]["actual_events"] = 40
        runtime_path = Path(spec["runtime_dir"]) / "runtime.json"
        runtime = read_json(runtime_path)
        runtime["cern_state_schema"] = staging.FUSED_STATE_SCHEMA
        atomic_json(runtime_path, runtime)
        atomic_json(self.spec_path, spec)
        with patch.object(shards, "resolve_proxy_path", return_value="/afs/test/proxy"):
            shards.plan_run(self.spec_path, self.run)
        self.spec = read_json(self.run / "run.json")
        # Exercise the multi-variant execution contract without enabling
        # unavailable detector configurations in the production catalogue.
        self.spec["campaigns"][0]["resolved_detector_variants"].append(dict(
            deepcopy(self.spec["campaigns"][0]["resolved_detector_variants"][0]),
            profile_id="second_mock_detector"))
        atomic_json(self.run / "run.json", self.spec)

    def state(self):
        return read_json(self.run / "state.json")

    def task(self, identity):
        return read_json(record_path(self.run, "tasks", identity))

    def accept(self, task, **fields):
        identity = task["task_id"]
        outcome = dict(task_id=identity, stage=task["stage"], status="ok",
                       runtime_release=task["runtime_release"], outputs=[])
        outcome.update(fields)
        atomic_json(record_path(self.run, "results", identity), outcome)
        self.assertEqual(shards.accept_result(self.run, identity)["exit_code"], 0)
        return outcome

    def plan_source(self):
        self.assertEqual(shards.prepare_source(self.run, 0)["exit_code"], 0)
        task = self.task("PLAN_000000")
        p = task["parameters"]
        refs = [dict(artifact_id=f"BLOCK:pool:s1:{i:06d}", kind="lhe-block",
                     url=f"{p['block_output_dir']}/block_s1_{i:06d}.lhe.gz",
                     events=5, size_bytes=20) for i in range(8)]
        original = dict(p["original_output"], events=40, size_bytes=100)
        self.accept(task, outputs=refs, originals=[original], planned_events=40,
                    actual_input_events=40)
        return refs

    def prepare(self, campaign="C", shard=0):
        self.assertEqual(shards.prepare_shard(self.run, campaign, shard)["exit_code"], 0)
        return self.task(f"PROCESS_{campaign}_JOB000000_BLOCK{shard:06d}")

    def finish_source(self):
        self.assertEqual(shards.prepare_source_product(self.run, "SOURCE_RECLAIM_000000")["exit_code"], 75)
        shards.prepare_source_product(self.run, "SOURCE_ARCHIVE_000000")
        archive_task = self.task("SOURCE_ARCHIVE_000000")
        published = dict(archive_task["outputs"][0], size_bytes=300, runtime_release="test-release")
        self.accept(archive_task, outputs=[published],
                    members=[dict(archive_task["inputs"][0], member="000000_source.lhe.gz")])
        shards.prepare_source_product(self.run, "SOURCE_RECLAIM_000000")
        task = self.task("SOURCE_RECLAIM_000000")
        self.assertEqual(task["parameters"]["verified_archive"], published)
        with patch.object(reclaim, "artifact_ready", return_value=False), \
                patch.object(reclaim, "remove_many") as remove:
            with self.assertRaisesRegex(RuntimeError, "archive cannot be verified"):
                reclaim.execute(task, self.root / "reclaim")
            remove.assert_not_called()
        self.finish_reclaim(task)
        shards.complete_source(self.run, 0)

    def finish_reclaim(self, task):
        with patch.object(reclaim, "artifact_ready", return_value=True), \
                patch.object(reclaim, "remove_many",
                             side_effect=lambda urls, **kw: dict.fromkeys(urls, "removed")):
            outcome = reclaim.execute(task, self.root / "reclaim")
        self.accept(task, reclaimed=outcome["reclaimed"])
        return outcome

    def native(self, task, work, *, fail=None):
        calls = []

        def run(stage, runtime, command, directory):
            calls.append((stage["task_id"], runtime))
            if stage["stage"] == "mix":
                # The real adapter owns LHE hydration, event checks, and gzip.
                (directory / "mixed.hepmc").write_text("E 1 0 0\n")
                atomic_json(directory / "mixed_manifest.json", {"actual_mixed_hepmc_events": 1})
                for slot in stage["parameters"]["sources"]:
                    atomic_json(directory / f"shower_{slot['slot']}_manifest.json",
                                {"actual_hepmc_events": 1})
            else:
                self.assertTrue(Path(task["outputs"][0]["sidecar_url"]).is_file())
                self.assertEqual((directory / "mixed.hepmc").read_text(), "E 1 0 0\n")
                self.assertEqual(stage["parameters"]["edm_event_id"], task["mix"]["parameters"]["edm_event_id"])
                if fail == "all" or stage["task_id"] == fail:
                    raise RuntimeError("mock detector error")
                (directory / "output_MINIAOD.root").write_bytes(stage["task_id"].encode())

        def materialize(ref, work_dir):
            from full_mc.storage.transfer import materialize_input
            if ref["kind"] == "hepmc":
                return materialize_input(ref, work_dir)
            import gzip
            path = work_dir / (ref["artifact_id"].replace(":", "_") + ".lhe.gz")
            with gzip.open(path, "wt") as stream:
                stream.write("<event>\n" * ref["events"])
            return path

        with patch("full_mc.workers.chain.run", side_effect=run), \
                patch("full_mc.workers.chain.materialize_input", side_effect=materialize), \
                patch("full_mc.workers.miniaod.count_events", return_value=1), \
                patch("full_mc.workers.chain.native_tree", return_value=Path("run_chain.sh")):
            result = processing.execute(task, work)
        return result, calls

    def test_graph_allocation_cleanup_and_handoff(self):
        state = self.state()
        self.assertEqual(state["state_schema"], staging.FUSED_STATE_SCHEMA)
        self.assertNotIn("staging", state)
        self.assertNotIn("cleanup_claims", state)
        self.assertNotIn("staging_bytes", self.spec["storage"])
        root = (self.run / "run.dag").read_text()
        self.assertEqual(root.count("JOB CLEANUP_pool_"), 1)
        self.assertIn("PARENT SOURCE_000000 SHARD_C_000000 SHARD_C_000001 SHARD_D_000000 CHILD CLEANUP_pool_000000", root)
        self.assertIn("PARENT CLEANUP_pool_000000 CHILD FINALIZE", root)
        submit = (self.run / "submit/processing.sub").read_text()
        self.assertIn("request_cpus = 2", submit)
        self.assertIn("transfer_output_files = result.json,diagnostics.tar.gz", submit)
        self.assertFalse((self.run / "submit/mix.sub").exists())
        with remote_io_blocked():
            refs = self.plan_source()
            source_dag = child_path(self.run, "SOURCE_000000").read_text()
            self.assertNotIn("admit-producer", source_dag)
            self.assertIn("prepare-source-product --task-id SOURCE_RECLAIM_000000", source_dag)
            before = self.state()
            inventories, _, exhausted = shards._inventory(self.run, self.spec, before)
            config = self.spec["campaigns"][0]
            expected = allocate_shard(config, inventories, before["planner"]["streams"], 0, 1,
                                      exhausted, config["global_job_index_start"])
            c0 = self.prepare()
            c1 = self.prepare(shard=1)
            d0 = self.prepare(campaign="D")
            descriptor = read_json(shards._manifest_path(self.run, "SHARD_C_000000"))
            self.assertEqual(descriptor["allocation"], expected)
            node = expected["nodes"][0]
            self.assertEqual(c0["mix"]["parameters"]["mixing_rng_seed"], node["mixing_rng_seed"])
            self.assertEqual(c0["mix"]["parameters"]["sources"][0]["rng_seed"],
                             node["sources"][0]["source_rng_seed"])
            self.assertEqual(c0["variants"][0]["parameters"]["edm_event_id"], node["edm_event_id"])
            self.assertEqual(c0["variants"][0]["parameters"]["global_job_index"], 15)
            self.assertEqual(c0["inputs"], d0["inputs"])
            child = child_path(self.run, "SHARD_C_000000").read_text()
            self.assertIn("CATEGORY PROCESS_C_JOB000000_BLOCK000000 processing", child)
            self.assertIn("MAXJOBS processing 2", child)
            self.assertIn('memory="7000" flavour="tomorrow"', child)
            self.assertNotIn("PRE PROCESS_", child)
            self.assertNotIn("staging_claim_bytes", str(c0))
            self.assertTrue(c0["outputs"][0]["url"].startswith(self.spec["storage"]["ihep_archive"]))
            with self.assertRaisesRegex(RuntimeError, "source slot"):
                shards.prepare_cleanup(self.run, "pool", 0)
            self.finish_source()
            with self.assertRaisesRegex(RuntimeError, "all campaign shards"):
                shards.prepare_cleanup(self.run, "pool", 0)

        for task, campaign, shard in ((c1, "C", 1), (d0, "D", 0), (c0, "C", 0)):
            outcome, calls = self.native(task, self.root / task["task_id"])
            self.assertEqual([r for _, r in calls],
                             [task["mix"]["runtime"]] + [v["runtime"] for v in task["variants"]])
            with remote_io_blocked():
                self.accept(task, outputs=outcome["outputs"])
                shards.complete_shard(self.run, campaign, shard)
        with self.assertRaisesRegex(RuntimeError, "terminal LHE block reclamation"):
            shards.finalize_run(self.run)
        shards.prepare_cleanup(self.run, "pool", 0)
        cleanup = self.task("CLEANUP_pool_000000")
        self.assertEqual([ref["url"] for ref in cleanup["inputs"]], [ref["url"] for ref in refs])
        self.assertGreater(len(refs), len({ref["url"] for task in (c0, c1, d0) for ref in task["inputs"]}))
        self.assertEqual(shards.accept_result(self.run, cleanup["task_id"], 1)["exit_code"], 1)
        with self.assertRaisesRegex(RuntimeError, "terminal LHE block reclamation"):
            shards.finalize_run(self.run)
        self.finish_reclaim(cleanup)
        self.assertEqual(shards.finalize_run(self.run)["merge_groups"], 3)
        self.assertTrue(Path(c0["outputs"][0]["url"]).is_file())
        handoff = export_handoff(self.run)
        self.assertEqual(handoff["schema"], "full-mc-handoff-v1")
        for group in handoff["tasks"]:
            stages = group["subtasks"]
            merge = stages["merge"]
            campaign = merge["parameters"]["campaign"]
            self.assertEqual(set(stages), {"merge", "ntuple", "selection", "efficiency"}
                             if campaign == "C" else {"merge", "ntuple"})
            variant = merge["parameters"]["detector_profile_id"]
            expected = [ref for task in ((c0, c1) if campaign == "C" else (d0,))
                        for ref in task["outputs"] if ref.get("detector_profile_id") == variant]
            self.assertEqual([r["artifact_id"] for r in merge["inputs"]],
                             [r["artifact_id"] for r in expected])
            self.assertEqual([r["url"] for r in merge["inputs"]], [r["url"] for r in expected])
            self.assertEqual(merge["inputs"][0]["detector_profile"], expected[0]["detector_profile"])
            self.assertNotIn("checkpoint_schema", merge["inputs"][0])

        # Use the actual unchanged IHEP merge adapter with a mocked cmsRun
        # command boundary. Verify its ordered input list from the exported task.
        task = handoff["tasks"][0]["subtasks"]["merge"]

        def run_merge(_task, _runtime, argv, work):
            self.assertEqual(argv[0], "edmCopyPickMerge")
            self.assertEqual((work / "inputs.txt").read_text().splitlines(),
                             ["file:" + ref["url"] for ref in task["inputs"]])
            (work / "merged_MINIAOD.root").write_bytes(b"mock merged product")

        with patch.object(merge_worker, "run", side_effect=run_merge), \
                patch.object(merge_worker, "count_events", return_value=2):
            self.assertEqual(merge_worker.execute(task, self.root / "ihep-worker")["status"], "ok")

    def test_retry_reconciles_interrupted_commit_and_only_reexecutes_failed_variant(self):
        self.plan_source()
        task = self.prepare()
        failed_variant = task["variants"][1]["task_id"]
        with self.assertLogs(level="ERROR"):
            result, first = self.native(task, self.root / "first", fail=failed_variant)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failed_variants"], [failed_variant])
        self.assertEqual(len(first), 3)
        self.assertEqual(len(result["outputs"]), 2)
        final_sidecar = Path(task["outputs"][0]["sidecar_url"])
        # Simulate an interruption after the durable payload copy and before
        # its final metadata commit, retaining the pre-payload candidate.
        final_sidecar.unlink()
        result, retry = self.native(task, self.root / "retry")
        self.assertEqual(retry, [(failed_variant, task["variants"][1]["runtime"])])
        self.assertEqual(result["status"], "ok")
        self.assertTrue(final_sidecar.is_file())
        final, calls = self.native(task, self.root / "all-complete")
        self.assertEqual(calls, [])
        self.assertEqual(final, result)

        path = record_path(self.run, "results", task["task_id"])
        atomic_json(path, result)
        with patch.object(staging, "_write_state", side_effect=RuntimeError("interrupted state write")):
            with self.assertRaisesRegex(RuntimeError, "interrupted state write"):
                shards.accept_result(self.run, task["task_id"])
        self.assertNotIn(task["task_id"], self.state()["accepted"])
        path.unlink()
        recovered, proof = reconstruct(task, {}, self.root / "recover")
        dagman_recovery.verify_record(task, recovered, TransferClient())
        self.assertEqual(recovered, result)
        self.assertEqual(len(proof), 3)
        atomic_json(path, recovered)
        self.assertEqual(shards.accept_result(self.run, task["task_id"])["exit_code"], 0)
        before = (self.run / "state.json").read_bytes()
        self.assertEqual(shards.accept_result(self.run, task["task_id"])["exit_code"], 0)
        self.assertEqual((self.run / "state.json").read_bytes(), before)

    def test_upload_failure_blocks_detectors_and_mismatched_checkpoint_fails_closed(self):
        self.plan_source()
        task = self.prepare()
        client = TransferClient()
        original = client.copy_out_exclusive

        def upload(path, destination, checksum):
            if destination == task["outputs"][0]["url"]:
                raise RuntimeError("mock failed payload copy")
            return original(path, destination, checksum)

        with patch.object(processing, "TransferClient", return_value=client), \
                patch.object(client, "copy_out_exclusive", side_effect=upload), \
                patch.object(processing.miniaod, "produce") as mini:
            with self.assertRaisesRegex(RuntimeError, "failed payload copy"):
                self.native(task, self.root / "fail")
            mini.assert_not_called()
        self.assertFalse(Path(task["outputs"][0]["sidecar_url"]).exists())
        self.native(task, self.root / "retry")
        payload = Path(task["outputs"][0]["url"])
        corrupted = bytearray(payload.read_bytes())
        corrupted[-1] ^= 1
        payload.write_bytes(corrupted)
        with patch.object(processing.mix, "produce") as mix:
            with self.assertRaisesRegex(ValueError, "differs from metadata"):
                self.native(task, self.root / "mismatch")
            mix.assert_not_called()
        self.assertEqual(payload.read_bytes(), corrupted)

    def test_checkpoint_first_writer_wins_and_missing_variants_retry(self):
        self.plan_source()
        task = self.prepare()
        with self.assertLogs(level="ERROR"):
            result, _ = self.native(task, self.root / "mix-only", fail="all")
        self.assertEqual(result["failed_variants"], [v["task_id"] for v in task["variants"]])
        accepted = result["outputs"][0]
        fallback = self.root / "other-attempt.hepmc.gz"
        fallback.write_bytes(b"another attempt")
        # A second worker may reach publish after a race on its initial lookup.
        client = TransferClient()
        winner, used_local = processing._publish(
            task, task["mix"], client, fallback, 1, {"source_counts": accepted["source_counts"]})
        self.assertEqual(winner, accepted)
        self.assertFalse(used_local)
        self.assertNotEqual(Path(winner["url"]).read_bytes(), fallback.read_bytes())
        resumed, calls = self.native(task, self.root / "variants")
        self.assertEqual(resumed["status"], "ok")
        self.assertEqual([name for name, _ in calls], [v["task_id"] for v in task["variants"]])

    def test_receipt_rejects_missing_detector_or_changed_edm_identity(self):
        self.plan_source()
        task = self.prepare()
        outcome, _ = self.native(task, self.root / "production")
        outputs = outcome["outputs"]
        with self.assertRaisesRegex(ValueError, "every MiniAOD"):
            self.accept(task, outputs=outputs[:-1])
        wrong = deepcopy(outputs)
        wrong[1]["edm_event_id"]["first_event"] += 1
        with self.assertRaisesRegex(ValueError, "EDM identity"):
            self.accept(task, outputs=wrong)
        self.assertNotIn(task["task_id"], self.state()["accepted"])

    def test_schema_and_runtime_packaging(self):
        release = self.root / "real-runtime"
        profiles = ["utilities-el9", "pythia-el8", "cmssw12-sim-el8"]
        core = Path(__file__).resolve().parents[1]
        descriptor = prepare_runtime(core, release, "offline", "cern", profiles=profiles)
        self.assertEqual(descriptor["cern_state_schema"], staging.FUSED_STATE_SCHEMA)
        unpacked = unpack_runtime(release, self.root / "unpacked", profiles[0], "offline",
                                   stage_profiles=profiles[1:], revision=descriptor["git_revision"])
        self.assertTrue((unpacked / "src/full_mc/workers/processing.py").is_file())
        self.plan_source()
        task = self.prepare()
        task.update(core_revision=descriptor["git_revision"], runtime_release="offline")
        for stage in [task["mix"], *task["variants"]]:
            stage["runtime_release"] = "offline"
        cached, _ = self.native(task, self.root / "prime-runtime-test")
        atomic_json(self.root / "task.json", task)
        subprocess.run(["python3", str(unpacked / "src/full_mc/runtime/bootstrap.py"),
                        "--task", str(self.root / "task.json"), "--release-dir", str(release),
                        "--work-dir", str(self.root / "bootstrap-work"),
                        "--result", str(self.root / "bootstrap-result.json"),
                        "--diagnostics", str(self.root / "bootstrap-logs.tar.gz")],
                       check=True, capture_output=True, text=True)
        self.assertEqual(read_json(self.root / "bootstrap-result.json"), cached)
        self.assertTrue((self.root / "bootstrap-logs.tar.gz").is_file())
        with self.assertRaisesRegex(ValueError, "core revision"):
            unpack_runtime(release, self.root / "bad", profiles[0], "offline",
                           stage_profiles=profiles[1:], revision="wrong")
        evidence = self.root / "proof.json"
        atomic_json(evidence, {"schema": "cern-afs-lock-proof-v1", "schedds": ["a", "b"],
                               "checks": dict.fromkeys(REQUIRED_CHECKS, True),
                               "evidence": [str(evidence)]})
        validate_site(dict(self.spec, site_validation={"proof_path": str(evidence)}))
        with self.assertRaisesRegex(ValueError, "requires mcprod-shard-state-v3"):
            staging.require_schema({"state_schema": "mcprod-shard-state-v2"})
        with self.assertRaisesRegex(RuntimeError, "completed run"):
            export_handoff(self.run)

    def test_remote_publication_uses_posc_no_replace_and_explicit_ihep_url(self):
        commands = []

        def runner(command, **kwargs):
            commands.append(command)
            stdout = "adler32 123abc\n" if "checksum" in command else ""
            return subprocess.CompletedProcess(command, 0, stdout, "")

        client = TransferClient(runner)
        payload = self.root / "payload"
        payload.write_bytes(b"payload")
        url = "root://cceos.ihep.ac.cn:1094///store/run/product"
        self.assertTrue(client.copy_out_exclusive(payload, url, "00123abc"))
        self.assertEqual(client.checksum(url), "00123abc")
        self.assertEqual(commands[1], ["xrdcp", "--nopbar", "--posc", "--retry", "0",
                                      "--rm-bad-cksum", "--cksum", "adler32:00123abc", str(payload), url])
        self.assertEqual(commands[2], ["xrdfs", "root://cceos.ihep.ac.cn:1094/", "query",
                                      "checksum", "/store/run/product"])
