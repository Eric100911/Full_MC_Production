"""Offline contracts for the refactored runtime and physics-stage boundaries."""
import gzip
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.application.resume import run_combined_task
from full_mc.io import atomic_json
from full_mc.runtime.packages import (_bundle, CORE_PAYLOAD,
                                      validate_cmssw15_source_package)
from full_mc import settings
from full_mc.runtime.bootstrap import unpack_runtime
from full_mc.runtime.profiles import STAGE_RUNTIME, run_profile
from full_mc.storage.transfer import artifact_ready
from full_mc.workers import execute_task
from full_mc.workers.chain import build_command as chain_command, execute_chain
from full_mc.workers.lhe import build_command as lhe_command
from full_mc.workers.edm import parse_event_count
from full_mc.workers.analysis import build_command as analysis_command
from full_mc.workers.mix import execute as mix_execute


def ref(path, identity, kind):
    return {"artifact_id": identity, "url": str(path), "kind": kind}


def publish_fixture(item, events=4):
    path = Path(item["url"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"fixture data")
    item = dict(item, events=events, size_bytes=path.stat().st_size)
    atomic_json(str(path) + ".json", dict(item, status="ok", complete=True))
    return item


class WorkerContracts(unittest.TestCase):
    def test_lhe_uses_real_flags_and_finite_cap(self):
        task = {"log_level": "warning", "parameters": {
            "pool_name": "p", "seed": 5, "process": "g g > cc~",
            "max_events": 11, "min_m_qqp": 3.0969}}
        command = lhe_command(task, Path("/tmp/chiw/task"))
        self.assertIn("--pool", command)
        self.assertIn("--process", command)
        self.assertNotIn("--workdir", command)
        self.assertNotIn("--process-string", command)
        self.assertNotIn("--lhe-shuffle-split", command)
        self.assertTrue(command[command.index("--compress-lhe") + 1].startswith("--"))
        self.assertEqual(command[command.index("--unwevt") + 1], "11")
        self.assertEqual(command[command.index("--min-m-qqp") + 1], "3.0969")
        task["parameters"]["max_events"] = 0
        with self.assertRaises(ValueError):
            lhe_command(task, Path("/tmp/chiw/task"))

    def test_stage_runtime_mismatch_is_rejected_before_execution(self):
        with self.assertRaisesRegex(ValueError, "requires runtime"):
            execute_task({"stage": "lhe", "runtime": "utilities-el9"}, "/tmp/chiw/no-run")

    def test_native_miniaod_command_stops_before_stageout(self):
        task = {"stage": "miniaod", "task_id": "J_JOB000001_BLOCK000002",
                "parameters": {"analysis": "JJP", "campaign": "JJP_DPS1",
                "edm_event_id": {"first_luminosity_block": 42}}}
        command = chain_command(task, Path("/tmp/chiw/w"), Path("/native/run_chain.sh"),
                                Path("/tmp/chiw/w/chain.json"))
        self.assertEqual(command[command.index("--skip-to") + 1], "gensim")
        self.assertEqual(command[command.index("--stop-at") + 1], "miniaod")
        self.assertEqual(command[command.index("--cleanup") + 1], "false")
        self.assertEqual(command[command.index("--first-luminosity-block") + 1], "42")

    def test_source_slots_cannot_reuse_the_same_block(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "block.lhe"
            source.write_text("<LesHouchesEvents/>")
            task = {"stage": "mix", "inputs": [ref(source, "B", "lhe-block")],
                    "parameters": {"sources": [
                        {"inputs": ["B"], "rng_seed": 1},
                        {"inputs": ["B"], "rng_seed": 2}]}}
            with self.assertRaisesRegex(ValueError, "reused"):
                execute_chain(task, root)

    def test_edm_counts_are_not_inferred(self):
        self.assertEqual(parse_event_count([{"events": 12, "file": "x.root"}]), 12)
        for payload in ({"size": 10}, {"events": True}, {"events": "12"},
                        [{"events": 2}, {"events": 3}], {"events": -1}):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                parse_event_count(payload)

    def test_mixer_publishes_actual_count_not_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "mixed.hepmc").write_text("HepMC::Version 2.06.10\nE 1 a\nE 2 b\n")
            atomic_json(root / "mixed_manifest.json", {"events_written": 2})
            atomic_json(root / "shower_0_manifest.json", {"actual_hepmc_events": 2})
            task = {"task_id": "mix", "stage": "mix", "runtime": "pythia-el8",
                    "runtime_release": "r1", "parameters": {
                        "target_mixed_events": 100, "sources": [{"slot": 0}]},
                    "outputs": [ref(root / "published/mixed.hepmc.gz", "mixed", "hepmc")]}
            with patch("full_mc.workers.mix.execute_chain"):
                outcome = mix_execute(task, root)
            self.assertEqual(outcome["outputs"][0]["events"], 2)
            self.assertTrue(artifact_ready(outcome["outputs"][0]))
            with gzip.open(root / "published/mixed.hepmc.gz", "rt") as stream:
                self.assertIn("E 2 b", stream.read())

    def test_raw_ntuple_is_the_only_analyzer_input(self):
        task = {"stage": "efficiency", "parameters": {
            "campaign": "JJP_DPS1", "analysis": "JJP", "merge_group": "g1"}}
        command = analysis_command(task, Path("/tmp/chiw/raw.root"), Path("/tmp/chiw/w"))
        self.assertEqual(command[command.index("--input-files") + 1], "/tmp/chiw/raw.root")
        self.assertNotIn("hadd", command)
        self.assertIn("--skip-plots", command)

    def test_transfer_runtime_binds_job_private_proxy_and_scratch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.dict("os.environ", {"X509_USER_PROXY": str(root / "credentials/proxy")},
                            clear=True), patch("full_mc.runtime.profiles.subprocess.run") as run:
                run_profile("utilities-el9", ["xrdcp", "x", "y"], cwd=root / "worker",
                            bind_paths=[root / "payload"])
            argv = run.call_args.args[0]
            self.assertIn(str(root / "credentials"), argv)
            self.assertIn(str(root / "payload"), argv)
            self.assertIn("--cleanenv", argv)

    def test_bundle_omits_generated_and_provenance(self):
        core = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "core.tar.gz"
            _bundle(core, CORE_PAYLOAD, output)
            with tarfile.open(output) as archive:
                names = archive.getnames()
            self.assertLess(output.stat().st_size, 2_000_000)
            self.assertIn("core/src/full_mc/workers/mix.py", names)
            self.assertFalse(any("/generated/" in name or "/campaign_job_specs/" in name
                                 or "/packages/" in name or "/.git/" in name
                                 or "__pycache__" in name for name in names))


class RuntimeBootstrap(unittest.TestCase):
    def test_transferred_release_is_self_contained_and_profile_checked(self):
        core = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            release = root / "release"
            release.mkdir()
            _bundle(core, CORE_PAYLOAD, release / "core.tar.gz")
            atomic_json(release / "runtime.json", {
                "runtime_release": "r1", "profiles": ["utilities-el9"],
                "bundles": {"core": "core.tar.gz"}})
            with self.assertRaisesRegex(ValueError, "does not match"):
                unpack_runtime(release, root / "wrong", "utilities-el9", "r2")
            self.assertFalse((root / "wrong").exists())
            unpacked = unpack_runtime(release, root / "worker", "utilities-el9", "r1")
            self.assertTrue((unpacked / "mcprod").is_file())
            self.assertTrue((unpacked / "src/full_mc/workers/mix.py").is_file())
            self.assertFalse((unpacked / "common/packages").exists())
            with self.assertRaises(FileExistsError):
                unpack_runtime(release, root / "worker", "utilities-el9", "r1")

    def test_cmssw15_source_fallback_and_installed_resources_are_valid(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.tar.gz"
            member = root / "ConfFile_cfg.py"
            member.write_text("# fixture\n", encoding="utf-8")
            with tarfile.open(source, "w:gz") as archive:
                archive.add(member, arcname="HeavyFlavorAnalysis/TPS-Onia2MuMu/test/ConfFile_cfg.py")
            validate_cmssw15_source_package(source)
            with tarfile.open(root / "invalid.tar.gz", "w:gz") as archive:
                archive.add(member, arcname="wrong.py")
            with self.assertRaisesRegex(ValueError, "source package"):
                validate_cmssw15_source_package(root / "invalid.tar.gz")
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(settings, "CORE_ROOT", Path(temporary)):
                self.assertIn("campaigns", settings.catalog())
                self.assertIn("production_storage", settings.defaults())


class CombinedRecovery(unittest.TestCase):
    def task(self, root):
        stages = {}
        for stage in ("merge", "ntuple", "selection", "efficiency"):
            kind = "raw-ntuple" if stage == "ntuple" else stage
            stages[stage] = {"task_id": "g:" + stage, "stage": stage,
                             "runtime": STAGE_RUNTIME[stage], "runtime_release": "r1",
                             "inputs": [], "outputs": [ref(root / "products" / stage, stage, kind)]}
        return {"task_id": "g", "stage": "combined", "runtime_release": "r1",
                "subtasks": stages}

    def test_branches_resume_independently_from_same_raw_ntuple(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = self.task(root)
            atomic_json(root / "task.json", task)
            calls = []
            def execute(stage, work):
                calls.append((stage["stage"], [x["artifact_id"] for x in stage["inputs"]]))
                return {"task_id": stage["task_id"], "stage": stage["stage"], "status": "ok",
                        "runtime_release": stage["runtime_release"],
                        "outputs": [publish_fixture(stage["outputs"][0])]}
            with patch("full_mc.workers.execute_task", side_effect=execute):
                run_combined_task(root / "task.json", root / "work")
                self.assertEqual(calls, [("merge", []), ("ntuple", ["merge"]),
                                         ("selection", ["ntuple"]), ("efficiency", ["ntuple"])])
                calls.clear()
                run_combined_task(root / "task.json", root / "work")
                self.assertEqual(calls, [])
                (root / "products/efficiency").unlink()
                run_combined_task(root / "task.json", root / "work")
                self.assertEqual(calls, [("efficiency", ["ntuple"])])

    def test_changed_runtime_invalidates_checkpoint(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = self.task(root)
            atomic_json(root / "task.json", task)
            calls = []
            def execute(stage, work):
                calls.append(stage["stage"])
                return dict(task_id=stage["task_id"], stage=stage["stage"], status="ok",
                            runtime_release=stage["runtime_release"],
                            outputs=[publish_fixture(stage["outputs"][0])])
            with patch("full_mc.workers.execute_task", side_effect=execute):
                run_combined_task(root / "task.json", root / "work")
                calls.clear()
                task["subtasks"]["efficiency"]["runtime_release"] = "r2"
                atomic_json(root / "task.json", task)
                run_combined_task(root / "task.json", root / "work")
            self.assertEqual(calls, ["efficiency"])


if __name__ == "__main__":
    unittest.main()
