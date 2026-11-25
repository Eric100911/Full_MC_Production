"""Offline SOURCE, static PLAN, atomic PROC and IHEP handoff contracts."""
from copy import deepcopy
import gzip
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch

from full_mc.application.handoff import export_handoff, transfer_handoff
from full_mc.application.merge_groups import merge_groups, sample_key
from full_mc.application.production import plan_run
from full_mc.backends.hepjob import prepare_batch
from full_mc.io import atomic_json, read_json
from full_mc.planning.graph import PLAN_RECORD, PROC_DAG, RESULTS_BUNDLE, node_stem
from full_mc.runtime.bootstrap import decode_task_payload
from full_mc.runtime.packages import CMSSW15_RUNTIME
from full_mc.runtime.reclaim_bundle import pack
from full_mc.storage.transfer import TransferClient, artifact_ready, file_checksum
from full_mc.workers import chain, execute_task, processing, reclaim, source
from tests.generate_synthetic_lhe import generate
from tests.mcprod_fixtures import run_spec_fixture


class StaticProduction(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.spec_path, self.run = run_spec_fixture(self.root)

    def prepare(self, spec):
        atomic_json(self.spec_path, spec)
        with patch("full_mc.application.production.resolve_proxy_path",
                   return_value=str(self.root / "proxy")), \
                patch("full_mc.application.production.ensure_valid_proxy") as check:
            plan_run(self.spec_path, self.run)
            check.assert_called_once_with(self.root / "proxy")

    def task(self, identity):
        return read_json(self.run / "tasks" / f"{node_stem(identity)}.json")

    def proc_task(self, identity):
        dag = (self.run / "proc" / PROC_DAG).read_text()
        line = next(line for line in dag.splitlines() if line.startswith(f"VARS {identity} "))
        variables = dict(field.split("=", 1) for field in shlex.split(line)[2:])
        return decode_task_payload(variables["payload"])

    def setup_pair(self, include_second=True):
        spec = read_json(self.spec_path)
        cs = dict(spec["sources"][0], pool="pool_2jpsi_cs")
        gg = dict(cs, pool="pool_gg", source_id="g2", seed=20, actual_events=19)
        spec["sources"] = [cs, gg] if include_second else [cs]
        base = dict(target_mixed_events=5, max_processing_nodes=10, merge_events=10,
                    detector_variants=["run3_2022_legacy_premix_v1"])
        spec["campaigns"] = [dict(base, name="JJP_SPS_CS", source_lhe_budgets=[5])]
        if include_second:
            spec["campaigns"].append(dict(base, name="JJP_DPS2_CS", source_lhe_budgets=[5, 5]))
        spec["resources"] = {"processing": {"cpus": 1, "disk_bytes": 3_000_000_000}}
        spec["sources"][0]["actual_events"] = 12
        self.prepare(spec)
        return spec

    @unittest.skipUnless(shutil.which("g++"), "native LHE splitter compiler unavailable")
    def test_source_splits_generated_lhe_above_unwevt_and_retains_tail(self):
        self.setup_pair(False)
        task = self.task("SOURCE_000000")
        fake_core = self.root / "core"
        package = fake_core / "common/packages/helac_package.tar.gz"
        package.parent.mkdir(parents=True)
        package.touch()
        raw = self.root / "generated.lhe"
        generate(str(raw), 12)
        task["inputs"] = []
        task["parameters"]["block_output_dir"] = str(self.root / "staging/blocks")
        task["parameters"]["manifest_url"] = str(self.root / "staging/source.json")
        task["generation"] = dict(task_id="LHE_000000", stage="lhe", runtime="helac-el7",
                                  parameters=dict(pool_name="pool_2jpsi_cs", seed=19,
                                                  generator_profile={"nmc": 500000, "unwevt": 1},
                                                  retain_helac_diagnostics=True))
        core = Path(__file__).resolve().parents[1]

        def generate_native(_task, _profile, argv, work, env):
            self.assertEqual(argv[argv.index("--unwevt") + 1], "1")
            path = Path(env["LOCAL_OUTPUT_BASE"]) / "lhe/sample_pool_2jpsi_cs_19.lhe.gz"
            path.parent.mkdir(parents=True)
            with gzip.open(path, "wb") as stream:
                stream.write(raw.read_bytes())

        def run_native(_task, _profile, argv, work, extra_env=None):
            completed = subprocess.run(argv, cwd=work, env=dict(os.environ, **(extra_env or {})),
                                       capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

        with patch.object(source.lhe, "core_path", side_effect=lambda _, name: fake_core / name), \
                patch.object(source.lhe, "run", side_effect=generate_native), \
                patch.object(source, "core_path", side_effect=lambda _, name: core / name), \
                patch.object(source, "run", side_effect=run_native):
            outcome = execute_task(task, self.root / "source-worker")
            self.assertEqual(outcome["block_count"], 3)
        manifest = read_json(self.root / "staging/source.json")
        self.assertEqual(manifest["actual_input_events"], 12)
        self.assertEqual([block["events"] for block in manifest["blocks"]], [5, 5, 2])
        self.assertTrue(artifact_ready(outcome["outputs"][0]))
        self.assertEqual([file_checksum(ref["url"]) for ref in manifest["blocks"]],
                         [ref["adler32"] for ref in manifest["blocks"]])

    def build_plan(self):
        plan = self.task("PLAN")
        p = plan["parameters"]
        manifests = {}
        for path, spec_source in zip(p["source_manifests"], p["spec"]["sources"]):
            pool, source_id = spec_source["pool"], spec_source["source_id"]
            count = spec_source["actual_events"]
            blocks = [dict(artifact_id=f"BLOCK:{pool}:{source_id}:{i:06d}", kind="lhe-block",
                           url=f"{path.rsplit('/', 1)[0]}/blocks/block_{source_id}_{i:06d}.lhe.gz",
                           events=min(5, count - start), size_bytes=16)
                      for i, start in enumerate(range(0, count, 5))]
            manifests[path] = dict(pool=pool, source_id=source_id, actual_input_events=count,
                                   planned_events=count, runtime_release=plan["runtime_release"],
                                   blocks=blocks)
        p["products_path"] = str(self.run / "proc")
        client = Mock(spec=TransferClient)
        client.read_json.side_effect = manifests.__getitem__
        with patch("full_mc.workers.plan.TransferClient", return_value=client):
            execute_task(plan, self.root / "plan-worker")
        self.assertEqual([call.args[0] for call in client.read_json.call_args_list], list(manifests))
        client.copy_in.assert_not_called()
        return read_json(self.run / "proc" / PLAN_RECORD)

    def test_plan_flattens_sps_and_dps_pairs_with_leftovers_and_resources(self):
        self.setup_pair()
        root = (self.run / "run.dag").read_text()
        self.assertEqual(root.count("SUBDAG EXTERNAL"), 1)
        for i in range(2):
            self.assertIn(f"PARENT SOURCE_{i:06d} CHILD PLAN", root)
        self.assertIn("PARENT PLAN CHILD PROC_SUBDAG", root)
        self.assertIn("PARENT PROC_SUBDAG CHILD RECLAIM", root)
        scripts = [line for line in root.splitlines() if line.startswith("SCRIPT ")]
        self.assertEqual(len(scripts), 1)
        self.assertIn("SCRIPT PRE RECLAIM", scripts[0])
        self.assertIn(" -m full_mc.runtime.reclaim_bundle ", scripts[0])
        reclaim_submit = (self.run / "submit/reclaim.sub").read_text()
        inputs = next(line for line in reclaim_submit.splitlines() if line.startswith("transfer_input_files"))
        inputs = inputs.split(" = ", 1)[1].split(",")
        self.assertIn(str(self.run / "proc" / PLAN_RECORD), inputs)
        self.assertIn(str(self.run / "proc" / RESULTS_BUNDLE), inputs)
        self.assertNotIn(str(self.run / "proc/results"), inputs)
        self.assertFalse((self.run / "proc").exists())
        submit = (self.run / "submit/plan.sub").read_text()
        self.assertIn("universe = vanilla", submit)
        self.assertIn("transfer_output_files = result.json,diagnostics.tar.gz,proc", submit)
        plan = self.build_plan()
        jobs = plan["processing"]
        self.assertEqual(len(jobs), 6)
        child = (self.run / "proc" / PROC_DAG).read_text()
        self.assertEqual(child.count("JOB PROC_"), 6)
        processing_dir = self.run / "proc"
        self.assertEqual(list(processing_dir.glob("*.sub")), [processing_dir / "processing.sub"])
        self.assertEqual(list(processing_dir.glob("*.dag")), [processing_dir / PROC_DAG])
        self.assertFalse((processing_dir / "tasks").exists())
        self.assertEqual(child.count(" payload="), 6)
        submit = (processing_dir / "processing.sub").read_text()
        self.assertIn("log = " + str(processing_dir / "logs/events.log"), submit)
        self.assertIn("--task-payload $(payload)", submit)
        self.assertEqual(child.count('cpus="1" disk="2929688"'), 6)
        self.assertIn("MAXJOBS processing 2", child)
        self.assertNotIn("SUBDAG", child)
        self.assertNotIn("PARENT", child)
        self.assertEqual(plan["unused_blocks"]["JJP_DPS2_CS"]["pool_gg"], ["BLOCK:pool_gg:g2:000003"])
        for name in ("JJP_SPS_CS", "JJP_DPS2_CS"):
            by_campaign = [job for job in jobs if job["campaign"] == name]
            self.assertEqual(len(by_campaign), 3)
            tail = self.proc_task(by_campaign[-1]["task_id"])
            self.assertEqual(tail["mix"]["parameters"]["target_mixed_events"], 2)
            self.assertEqual(tail["variants"][0]["parameters"]["processing_cpus"], 1)
            self.assertEqual(tail["inputs"][0]["events"], 2)

    def test_cpu_request_is_passed_to_native_cmssw(self):
        self.setup_pair(False)
        first = self.build_plan()["processing"][0]
        task = self.proc_task(first["task_id"])
        mini = task["variants"][0]
        mixed = self.root / "mixed.hepmc"
        mixed.write_text("E 1 0 0\n")
        mini["inputs"] = [dict(url=str(mixed), events=5)]
        work = self.root / "native"
        work.mkdir()
        with patch.object(chain, "native_tree", return_value=Path("run_chain.sh")), \
                patch.object(chain, "run") as run:
            chain.execute_chain(mini, work)
        self.assertEqual(run.call_args.kwargs["extra_env"], {"PROCESSING_CPUS": "1"})
        repo = Path(__file__).resolve().parents[1]
        script = (repo / "processing/run_chain.sh").read_text()
        self.assertIn('nThreads="${PROCESSING_CPUS:-2}" nStreams="${PROCESSING_CPUS:-2}"', script)
        usage = '--nThreads "${PROCESSING_CPUS:-2}" --nStreams "${PROCESSING_CPUS:-2}"'
        self.assertEqual(script.count(usage), 3)

    def test_lhe_requires_closing_tag_and_positive_count(self):
        path = self.root / "bad.lhe"
        for contents in ("<LesHouchesEvents>\n<event>\n</event>\n",
                         "<LesHouchesEvents>\n</LesHouchesEvents>\n"):
            with self.subTest(contents=contents):
                path.write_text(contents)
                with self.assertRaisesRegex(RuntimeError, "positive events and a closing tag"):
                    source.validate_lhe(path)

    def test_reclaim_requires_proc_results_and_groups_mini_aod_by_actual_events(self):
        self.setup_pair()
        plan = self.build_plan()
        task = self.task("RECLAIM")
        result_dir = self.run / "proc/results"
        task["parameters"].update(plan_path=str(self.run / "proc" / PLAN_RECORD),
                                  results_dir=str(result_dir),
                                  products_path=str(self.run / "handoff.json"))
        with patch.object(reclaim, "remove_many") as remove:
            with self.assertRaises(FileNotFoundError):
                execute_task(task, self.root / "reclaim-early")
            remove.assert_not_called()
        with self.assertRaisesRegex(ValueError, "missing or empty PROC result"):
            pack(result_dir.parent)
        self.assertFalse((result_dir.parent / RESULTS_BUNDLE).exists())
        mini_paths = []
        for job in plan["processing"]:
            count = 2 if job["task_id"].endswith("000002") else 5
            outputs = []
            for ref in job["outputs"]:
                path = Path(ref["url"])
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"product")
                record = dict(ref, events=count, size_bytes=path.stat().st_size,
                              adler32=file_checksum(path), runtime_release=task["runtime_release"],
                              count_source="edmFileUtil", global_job_index=3,
                              edm_event_id={"first_run": 1, "first_luminosity_block": 4})
                atomic_json(ref["sidecar_url"], dict(record, status="ok", complete=True))
                outputs.append(record)
                if ref["kind"] == "miniaod":
                    mini_paths.append(path)
            atomic_json(result_dir / f"{node_stem(job['task_id'])}.json",
                        dict(task_id=job["task_id"], stage="processing", status="ok",
                             runtime_release=task["runtime_release"], outputs=outputs))
        pack(result_dir.parent)
        bundle = result_dir.parent / RESULTS_BUNDLE
        sandbox = self.root / "worker-transfer"
        sandbox.mkdir()
        shutil.copy2(result_dir.parent / PLAN_RECORD, sandbox)
        task["parameters"].update(plan_path=str(sandbox / PLAN_RECORD),
                                  results_dir=str(sandbox / "results"))
        missing = f"results/{node_stem(plan['processing'][0]['task_id'])}.json"
        with tarfile.open(bundle, "r:gz") as archive, \
                tarfile.open(sandbox / RESULTS_BUNDLE, "w:gz") as incomplete:
            for member in archive.getmembers():
                if member.name != missing:
                    incoming = archive.extractfile(member) if member.isfile() else None
                    incomplete.addfile(member, incoming)
        with patch.object(reclaim, "remove_many") as remove:
            with self.assertRaisesRegex(FileNotFoundError, "missing PROC result from"):
                execute_task(task, self.root / "reclaim-incomplete-bundle")
            remove.assert_not_called()
        shutil.copy2(bundle, sandbox / RESULTS_BUNDLE)
        self.assertFalse((sandbox / "results").exists())
        mini_paths[0].write_bytes(b"corrupt")
        with patch.object(reclaim, "remove_many") as remove:
            with self.assertRaisesRegex(RuntimeError, "cannot verify EOS MiniAODs"):
                execute_task(task, self.root / "reclaim-corrupted")
            remove.assert_not_called()
        mini_paths[0].write_bytes(b"product")
        with patch.object(reclaim, "remove_many", side_effect=lambda urls, **_: dict.fromkeys(urls, "removed")) as remove:
            outcome = execute_task(task, self.root / "reclaim-worker")
        unpacked = self.root / "reclaim-worker/results"
        self.assertTrue(all((unpacked / f"{node_stem(job['task_id'])}.json").is_file()
                            for job in plan["processing"]))
        self.assertEqual(outcome["reclaimed_count"], 9)
        self.assertEqual(remove.call_args.args[0], plan["cleanup"])
        atomic_json(self.run / "results/RECLAIM.json", outcome)
        handoff = export_handoff(self.run)
        self.assertEqual(handoff["schema"], "full-mc-handoff-v1")
        self.assertEqual(set(handoff), {"schema", "run_id", "runtime_release", "core_revision",
                                        "tasks", "rejected"})
        self.assertEqual(handoff, read_json(self.run / "handoff.json"))
        first = handoff["tasks"][0]["subtasks"]["merge"]["inputs"][0]
        self.assertEqual(first["sidecar_url"], first["url"] + ".json")
        self.assertEqual(first["count_source"], "edmFileUtil")
        self.assertEqual(first["global_job_index"], 3)
        self.assertNotIn("adler32", first)
        self.assertEqual(len(read_json(self.run / "merge_groups.json")["groups"]), 4)
        counts = [sum(ref["events"] for ref in group["subtasks"]["merge"]["inputs"])
                  for group in handoff["tasks"]]
        self.assertEqual(counts, [10, 2, 10, 2])
        ihep = self.root / "ihep-inbox"
        transferred = transfer_handoff(self.run / "handoff.json", {"destination": str(ihep)})
        self.assertEqual(transferred["handoff"], str(ihep / "handoff.json"))
        self.assertEqual(read_json(ihep / "handoff.json"), handoff)
        receiver = {"git_revision": handoff["core_revision"], "source_dirty": False,
                    "profiles": ["cmssw12-sim-el8", "cmssw15-ntuple-el9", "analyzer-el9"],
                    "bundles": {"cmssw15-ntuple-el9": CMSSW15_RUNTIME}}
        with patch("full_mc.backends.hepjob.validate_runtime_release", return_value=receiver), \
                patch("full_mc.backends.hepjob.ensure_valid_proxy"):
            batch = prepare_batch(ihep / "handoff.json", self.root / "batch",
                                  proxy=str(self.root / "proxy"), runtime_dir=self.root / "ihep-runtime")
        self.assertEqual(len(batch["jobs"]), 4)
        for job, group in zip(batch["jobs"], handoff["tasks"]):
            prepared = read_json(job["task_path"])
            self.assertEqual(prepared["subtasks"]["merge"]["inputs"],
                             group["subtasks"]["merge"]["inputs"])


    def test_native_raw_fails_even_after_writing_a_partial_output(self):
        repo = Path(__file__).resolve().parents[1]
        text = (repo / "processing/run_chain.sh").read_text()
        raw = text[text.index("run_raw() {"):text.index("# Step 5: RECO")]
        stubs = """
msg_step() { :; }
msg_info() { :; }
msg_error() { :; }
setup_cmssw12() { :; }
run_logged() { :; }
run_cmssw12_command() {
    printf 'partial' > "$RAW_OUTPUT"
    return 42
}
"""
        env = dict(os.environ, WORKDIR=str(self.root), PREMIX_INPUT_MODE="dbs")
        outcome = subprocess.run(["bash", "-c", stubs + raw + "\nrun_raw"], env=env)
        self.assertEqual(outcome.returncode, 42)
        self.assertEqual((self.root / "output_RAW.root").read_text(), "partial")

    def test_merge_size_threshold_keeps_last_group(self):
        self.setup_pair(False)
        spec = read_json(self.run / "run.json")
        campaign = spec["campaigns"][0]
        campaign.pop("merge_events")
        campaign["merge_bytes"] = 40
        variant = campaign["resolved_detector_variants"][0]["profile_id"]
        refs = [dict(artifact_id=f"M{i}", events=5, size_bytes=22) for i in range(3)]
        groups = merge_groups(spec, {sample_key(campaign["name"], variant): refs})
        self.assertEqual([group["subtasks"]["merge"]["inputs"] for group in groups],
                         [refs[:2], refs[2:]])

    def test_processing_restarts_mix_after_detector_failure(self):
        self.setup_pair(False)
        first = self.build_plan()["processing"][0]
        task = self.proc_task(first["task_id"])

        def mix_native(_task, work):
            work.mkdir()
            (work / "mixed.hepmc").write_bytes(b"mixed")
            path = work / "mixed.hepmc.gz"
            path.write_bytes(b"compressed")
            return path, 5, [{"slot": 0, "actual_hepmc_events": 5}]

        def detector(variant, work):
            self.assertEqual(Path(variant["inputs"][0]["local_path"]).read_bytes(), b"mixed")
            work.mkdir()
            path = work / "output_MINIAOD.root"
            path.write_bytes(b"MiniAOD")
            return path, 5, {"count_source": "edmFileUtil"}

        with patch.object(processing.mix, "produce", side_effect=mix_native) as mix:
            with patch.object(processing.miniaod, "produce", side_effect=RuntimeError("CMSSW failed")):
                with self.assertRaisesRegex(RuntimeError, "CMSSW failed"):
                    execute_task(deepcopy(task), self.root / "first")
            with patch.object(processing.miniaod, "produce", side_effect=detector):
                result = execute_task(deepcopy(task), self.root / "retry")
            self.assertEqual(mix.call_count, 2)
        self.assertTrue(all(artifact_ready(ref) for ref in result["outputs"]))

    @unittest.skipUnless(shutil.which("condor_submit_dag"), "native DAGMan unavailable")
    def test_native_condor_parses_root_before_plan_and_parses_finished_child(self):
        self.setup_pair()
        command = ["condor_submit_dag", "-no_submit", "-no_recurse"]
        env = dict(os.environ, CONDOR_CONFIG="/dev/null")
        self.assertFalse((self.run / "proc" / PROC_DAG).exists())
        subprocess.run(command + ["run.dag"], env=env, cwd=self.run, check=True, capture_output=True)
        self.build_plan()
        subprocess.run(command + [f"proc/{PROC_DAG}"], env=env, cwd=self.run, check=True, capture_output=True)
        self.assertTrue((self.run / "proc" / f"{PROC_DAG}.condor.sub").is_file())


class AtomicUpload(unittest.TestCase):
    def test_verified_replace_and_failures_clean_up_exact_temporary(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "output"
            path.write_bytes(b"payload")
            target = "root://cceos.ihep.ac.cn:1094///store/test/file.root"
            final_lfn = "/store/test/file.root"
            temp_lfn = final_lfn + ".tmp.15.0"
            for failure in (None, "size", "checksum", "rename", "copy", "remove"):
                with self.subTest(failure=failure):
                    self.verify_flow(path, target, final_lfn, temp_lfn, failure)

    def verify_flow(self, path, target, final_lfn, temp_lfn, failure):
        checksum = file_checksum(path)

        def xrootd(args, **kwargs):
            if args[0] == "xrdcp":
                self.assertEqual(args[-1], target + ".tmp.15.0")
                if failure == "copy":
                    return subprocess.CompletedProcess(args, 1, "", "copy failed")
            else:
                action, lfn = args[2:4]
                if action == "stat":
                    return subprocess.CompletedProcess(args, 0, "Size: 0" if failure == "size" else "Size: 7", "")
                if action == "query":
                    actual = "ffffffff" if failure == "checksum" else checksum
                    return subprocess.CompletedProcess(args, 0, "adler32 " + actual, "")
                if action == "rm" and lfn == final_lfn:
                    return subprocess.CompletedProcess(args, 1, "",
                                                       "[3010] Permission denied" if failure == "remove"
                                                       else "[3011] No such file")
                if action == "mv" and failure == "rename":
                    return subprocess.CompletedProcess(args, 1, "", "rename failed")
            return subprocess.CompletedProcess(args, 0, "", "")

        runner = Mock(side_effect=xrootd)
        client = TransferClient(runner=runner)
        if failure:
            expected = {"size": "actual=0 expected=7",
                        "checksum": f"actual=ffffffff expected={checksum}"}.get(failure, "transfer command failed")
            with self.assertRaisesRegex(RuntimeError, expected):
                client.copy_atomic(path, target, "15.0")
            self.assertEqual(runner.call_args.args[0][-2:], ["rm", temp_lfn])
            if failure in {"copy", "size", "checksum"}:
                self.assertFalse(any(c.args[0][-2:] == ["rm", final_lfn] for c in runner.call_args_list))
        else:
            self.assertEqual(client.copy_atomic(path, target, "15.0"), (7, checksum))
            self.assertEqual(runner.call_args_list[-2].args[0][-2:], ["rm", final_lfn])
            self.assertEqual(runner.call_args.args[0][-3:], ["mv", temp_lfn, final_lfn])
