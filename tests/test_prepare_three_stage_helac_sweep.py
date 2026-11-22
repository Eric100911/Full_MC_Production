"""Offline contracts for the finite stage-one HELAC sweep preparer."""
from contextlib import redirect_stdout
from copy import deepcopy
import gzip
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

CORE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORE / "src"))
sys.path.insert(0, str(CORE))

from full_mc.io import atomic_json, read_json
from full_mc.local_paths import log_path, record_path
from full_mc.runtime.packages import (HELAC_PACKAGE_SHA256, HELAC_RUNTIME_PACKAGE,
                                      HELAC_SOURCE_PACKAGE)
from tools import prepare_three_stage_helac_sweep as sweep


class SweepTest(unittest.TestCase):
    def setUp(self):
        scratch = Path("/tmp/chiw")
        scratch.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="helac-sweep-test-", dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "run"
        self.runtime = self.base / "runtime"
        controller = self.runtime / "controller/core"
        bootstrap = controller / "src/full_mc/runtime/bootstrap.py"
        bootstrap.parent.mkdir(parents=True)
        bootstrap.write_text("# offline fixture\n")
        (controller / "mcprod").touch()
        profiles = { }
        for cell, (pool, nmc, unwevt, _) in sweep.CELLS.items():
            gg = cell == "gg"
            profile = dict(profile_id="sweep_" + cell, availability="ready",
                           compatible_pools=[pool], generator_path="main",
                           process="generate g g > g g" if gg else
                           "generate g g > cc~(3S11) cc~(3S11) g",
                           nmc=nmc, unwevt=unwevt, unwevt_semantics="main-path-stop-target",
                           planning_events_hint=1,
                           cuts=dict(zip(("min_pt_conia", "min_pt_bonia", "min_pt_q",
                                          "min_m_qqp", "min_m_q_beam"),
                                         (0, 0, 4, 0, 0) if gg else (6, 4, 0, 3.0969, 3.0969))))
            profiles[profile["profile_id"]] = profile
        self.catalogue = controller / "common/profiles/generator_profiles.json"
        atomic_json(self.catalogue, {"profiles": profiles})
        self.descriptor = dict(runtime_release="v06_test", git_revision="a" * 40,
                               source_dirty=False, site="cern", profiles=["helac-el7"],
                               bundles={"core": "core.tar.gz", "helac-el7": HELAC_RUNTIME_PACKAGE},
                               helac_package=dict(source=HELAC_SOURCE_PACKAGE,
                                                  runtime_name=HELAC_RUNTIME_PACKAGE,
                                                  sha256=HELAC_PACKAGE_SHA256))
        for name in self.descriptor["bundles"].values():
            (self.runtime / name).write_bytes(b"fixture bundle")
        atomic_json(self.runtime / "runtime.json", self.descriptor)
        self.spec = dict(
            schema="three-stage-helac-sweep-v1", run_id="experiment_test_s1",
            core_revision="a" * 40, runtime_release="v06_test", runtime_dir=str(self.runtime),
            schedd="bigbird11.cern.ch", proxy_path=str(self.base / "credentials/proxy"),
            output_root="root://cceos.ihep.ac.cn:1094///store/user/test/experiment_test/archive/stage1-sources",
            site_proof_path=str(self.base / "proof.json"),
            resources={"lhe": dict(cpus=1, memory_mb=3072, disk_bytes=4_000_000_000, flavour="workday")},
            scheduler=dict(max_jobs_submitted=200, max_jobs_idle=100, max_submits_per_interval=20,
                           submit_delay=0, max_pre_scripts=20, max_post_scripts=20, maxjobs={"lhe": 2}),
            cells=[dict(cell=c, pool=p, profile_id="sweep_" + c, seeds=s)
                   for c, (p, _, _, s) in sweep.CELLS.items()])
        atomic_json(self.base / "proof.json", {
            "schema": "cern-afs-lock-proof-v1",
            "schedds": ["bigbird11.cern.ch", "bigbird22.cern.ch"],
            "checks": {name: True for name in (
                "mutual_exclusion", "read_after_unlock", "crash_recovery",
                "pre_defer", "pre_skip", "dynamic_subdag", "autorescue")}})
        self.spec_path = self.base / "spec.json"
        # Package content verification belongs to runtime tests; all other
        # descriptor, asset and controller validation executes here.
        self.hash_patch = patch("full_mc.runtime.packages._sha256", return_value=HELAC_PACKAGE_SHA256)
        self.hash_patch.start()
        self.addCleanup(self.hash_patch.stop)

    def plan(self):
        atomic_json(self.spec_path, self.spec)
        return sweep.plan(self.spec_path, self.root)

    def entry(self, index=0):
        return read_json(self.root / "sweep-manifest.json")["entries"][index]

    def receipt(self, index=0, **extra):
        entry = self.entry(index)
        ref = dict(artifact_id=entry["task_id"], url=entry["url"], kind="lhe",
                   events=12, size_bytes=345)
        value = dict(task_id=entry["task_id"], stage="lhe", status="ok",
                     runtime_release="v06_test", outputs=[ref])
        value.update(extra)
        atomic_json(record_path(self.root, "results", entry["task_id"]), value)
        return value

    def archive(self, index, members):
        path = log_path(self.root, self.entry(index)["task_id"], ".tar.gz")
        with tarfile.open(path, "w:gz") as archive:
            for name, text in members.items():
                data = text.encode()
                member = tarfile.TarInfo(name)
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))

    def summary(self):
        return sweep.summarize(self.root, self.base / "summary.json")

    def test_plan_cli_emits_all_ten_independent_tasks_and_exact_resources(self):
        atomic_json(self.spec_path, self.spec)
        with redirect_stdout(io.StringIO()) as out:
            self.assertEqual(sweep.main(["plan", "--spec", str(self.spec_path),
                                         "--output-dir", str(self.root)]), 0)
        self.assertFalse(json.loads(out.getvalue())["submitted"])
        dag = (self.root / "run.dag").read_text()
        self.assertEqual(sum(line.startswith("JOB ") for line in dag.splitlines()), 10)
        self.assertEqual(sum(line.startswith("CATEGORY ") and line.endswith(" lhe")
                             for line in dag.splitlines()), 10)
        self.assertEqual(dag.count("RETRY LHE_"), 10)
        self.assertIn("MAXJOBS lhe 2\n", dag)
        self.assertNotIn("SCRIPT PRE", dag)
        self.assertNotIn("SUBDAG", dag)
        self.assertNotIn("PARENT", dag)
        self.assertEqual(list((self.root / "steps").glob("*.dag")), [])
        config = (self.root / "dagman.config").read_text()
        for key, name in sweep.SCHEDULER.items():
            self.assertIn(f"{name} = {self.spec['scheduler'][key]}\n", config)
        submit = (self.root / "submit/lhe.sub").read_text()
        self.assertIn("request_cpus = 1", submit)
        self.assertIn("$(task_path)", submit)
        manifest = read_json(self.root / "sweep-manifest.json")
        self.assertEqual(len({e["source_id"] for e in manifest["entries"]}), 10)
        for entry in manifest["entries"]:
            task = read_json(record_path(self.root, "tasks", entry["task_id"]))
            self.assertEqual(task["stage"], "lhe")
            self.assertEqual(task["inputs"], [])
            self.assertEqual(task["parameters"]["generator_profile"], entry["generator_profile"])
            self.assertTrue(task["parameters"]["retain_helac_diagnostics"])
            self.assertIn(entry["source_id"], task["outputs"][0]["url"])
            self.assertTrue(task["outputs"][0]["url"].startswith("root://cceos.ihep.ac.cn:1094///"))
        self.assertIn('memory="3072"', dag)
        self.assertIn('disk="3906250"', dag)

    def test_runtime_catalogue_is_frozen_and_hint_never_limits_tasks(self):
        self.plan()
        before = (self.root / "sweep-manifest.json").read_bytes()
        atomic_json(self.catalogue, {"profiles": {}})
        self.assertEqual(len(self.summary()["entries"]), 10)
        self.assertEqual(before, (self.root / "sweep-manifest.json").read_bytes())

    def test_invalid_cells_seeds_and_missing_fields_rejected_without_output(self):
        original = deepcopy(self.spec)
        mutations = [lambda s: s["cells"].pop(),
                     lambda s: s["cells"][0].update(seeds=[80000, 93001]),
                     lambda s: s["cells"][0].update(pool="pool_gg"),
                     lambda s: s.pop("site_proof_path"),
                     lambda s: s["resources"]["lhe"].update(cpus=2),
                     lambda s: s.update(output_root=s["output_root"].replace("1094///", "1094//"))]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                self.spec = deepcopy(original)
                mutate(self.spec)
                with self.assertRaises((ValueError, KeyError)):
                    self.plan()
                self.assertFalse(self.root.exists())

    def test_dirty_revision_site_and_release_mismatches_rejected(self):
        for change in (dict(source_dirty=True), dict(git_revision="b" * 40),
                       dict(site="ihep"), dict(runtime_release="other")):
            with self.subTest(change=change):
                atomic_json(self.runtime / "runtime.json", dict(self.descriptor, **change))
                with self.assertRaises(ValueError):
                    self.plan()
                self.assertFalse(self.root.exists())

    def test_bad_profile_values_do_not_create_run(self):
        original = read_json(self.catalogue)
        for change in (dict(unwevt=0), dict(unwevt=-1), dict(nmc=500000),
                       dict(availability="unavailable"), dict(cuts={})):
            with self.subTest(change=change):
                payload = deepcopy(original)
                payload["profiles"]["sweep_A"].update(change)
                atomic_json(self.catalogue, payload)
                with self.assertRaises(ValueError):
                    self.plan()
                self.assertFalse(self.root.exists())

    def test_existing_run_is_never_overwritten(self):
        self.plan()
        before = (self.root / "run.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.plan()
        self.assertEqual(before, (self.root / "run.json").read_bytes())

    def test_summary_uses_exact_worker_count_and_keeps_unknown_metrics_null(self):
        self.plan()
        self.receipt()
        with patch.object(sweep, "count_lhe_events", side_effect=AssertionError("must not recount")):
            payload = self.summary()
        row = payload["entries"][0]
        self.assertEqual(row["actual_events"], 12)
        self.assertEqual(row["gzip_bytes"], 345)
        self.assertFalse(row["reached_cap"])
        self.assertIsNone(row["cpu_seconds"])
        self.assertIsNone(row["stop_reason"])
        self.assertIn("cpu_seconds", row["missing_reasons"])
        self.assertEqual(payload["entries"][1]["status"], "missing")
        self.assertFalse(payload["all_results_present"])

    def test_local_gzip_is_counted_once_and_count_receipt_is_reused(self):
        self.plan()
        path = self.base / "source.lhe.gz"
        with gzip.open(path, "wt") as handle:
            handle.write("<event>\n</event>\n<event >\n</event>\n")
        receipt = self.receipt()
        ref = receipt["outputs"][0]
        ref.pop("events")
        ref.pop("size_bytes")
        ref["local_path"] = str(path)
        atomic_json(record_path(self.root, "results", self.entry()["task_id"]), receipt)
        with patch.object(sweep, "count_lhe_events", wraps=sweep.count_lhe_events) as count:
            for _ in range(2):
                row = self.summary()["entries"][0]
                self.assertEqual(row["actual_events"], 2)
                self.assertEqual(row["gzip_bytes"], path.stat().st_size)
            count.assert_called_once_with(path)
        self.assertTrue(record_path(self.root, "counts", self.entry()["task_id"]).is_file())

    def test_returned_native_diagnostics_supply_sigma_timing_and_warning_evidence(self):
        self.plan()
        self.receipt()
        self.archive(0, {
            "HELAC-Onia-2.7.6/PROC_HO_1/P0_calc/output/RESULT_a.out":
                "total sigma (nb) = 1.1D-01 +/- 1.1D-03\n",
            "command_logs/001_helac_ho_cluster.stdout": "WARNING maximum weight exceeded\n",
            "command_logs/001_helac_ho_cluster.timing.log":
                "wall_seconds=12\ncpu_user_seconds=10\ncpu_system_seconds=2\nexit_code=0\n"})
        row = self.summary()["entries"][0]
        self.assertEqual(row["sigma_nb"], .11)
        self.assertEqual(row["sigma_error_nb"], .0011)
        self.assertEqual(row["cpu_seconds"], 12)
        self.assertEqual(row["wall_seconds"], 12)
        self.assertEqual(row["events_per_cpu_hour"], 3600)
        self.assertEqual(row["max_weight_warnings"], ["WARNING maximum weight exceeded"])
        self.assertTrue(any("RESULT_a.out" in p for p in row["evidence_paths"]))

    def test_failed_and_corrupt_diagnostics_preserve_explanations(self):
        self.plan()
        self.receipt(status="failed", outputs=[])
        self.archive(0, {"command_logs/001_helac_ho_cluster.timing.log":
                         "wall_seconds=2\ncpu_user_seconds=1\ncpu_system_seconds=0\nexit_code=7\n"})
        log_path(self.root, self.entry(1)["task_id"], ".tar.gz").write_bytes(b"broken")
        payload = self.summary()
        self.assertEqual(payload["entries"][0]["stop_reason"], "helac_ho_cluster exit_code=7")
        self.assertIsNone(payload["entries"][0]["actual_events"])
        self.assertIn("diagnostics_error", payload["entries"][1])

    def test_cap_and_all_ten_results_are_reported(self):
        self.plan()
        for index in range(10):
            entry = self.entry(index)
            ref = dict(artifact_id=entry["task_id"], url=entry["url"], kind="lhe",
                       events=entry["generator_profile"]["unwevt"], size_bytes=1000)
            self.receipt(index, outputs=[ref])
        payload = self.summary()
        self.assertTrue(payload["all_results_present"])
        self.assertTrue(payload["all_sources_counted"])
        self.assertTrue(all(row["reached_cap"] for row in payload["entries"]))
        self.assertIn("accepted-event-cap", payload["entries"][0]["stop_reason"])

    def test_wrong_result_identity_is_rejected(self):
        self.plan()
        self.receipt(task_id="LHE_other")
        row = self.summary()["entries"][0]
        self.assertEqual(row["status"], "invalid")
        self.assertIsNone(row["actual_events"])

    def test_summarize_cli_runs_from_outside_checkout(self):
        self.plan()
        output = self.base / "cli-summary.json"
        completed = subprocess.run(
            [sys.executable, str(CORE / "tools/prepare_three_stage_helac_sweep.py"),
             "summarize", "--run", str(self.root), "--output", str(output)],
            cwd=self.base, capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(completed.stdout)["output"], str(output))
        self.assertEqual(len(read_json(output)["entries"]), 10)


if __name__ == "__main__":
    unittest.main()
