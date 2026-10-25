import gzip
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "lhe_kinematics_audit.py"
SPEC = importlib.util.spec_from_file_location("lhe_kinematics_audit", SCRIPT)
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


LHE = '''<LesHouchesEvents version="1.0">
<header></header>
<init>
 2212 2212 6500 6500 0 0 0 0 3 1
 1.0 0.0 1.0 1
</init>
<event>
 2 1 1.0 91.0 0.007 0.118
 443 1 0 0 0 0 3 0 4 5 3.0969 0 9
 443 1 0 0 0 0 -3 0 -4 5 3.0969 0 9
</event>
<event>
 2 1 1.0 91.0 0.007 0.118
 443 1 0 0 0 0 0 4 3 5 3.0969 0 9
 443 1 0 0 0 0 0 -4 -3 5 3.0969 0 9
</event>
</LesHouchesEvents>
'''


class KinematicsAuditTest(unittest.TestCase):
    def test_matching_kinematic_streams_across_files_and_cohorts(self):
        try:
            import awkward, numpy, pylhe  # noqa: F401
        except ImportError:
            self.skipTest("LCG pylhe/numpy/awkward environment required")
        second_event = LHE.index("<event>", LHE.index("</event>"))
        variants = {
            "a.lhe": LHE,
            "b.lhe": LHE.replace("<header></header>", "<header>other metadata</header>"),
            "c.lhe": LHE[:second_event] + "</LesHouchesEvents>\n",
            "d.lhe": LHE.replace("<header></header>", "<header>third metadata</header>"),
            "e.lhe": LHE.replace("443 1 0 0 0 0 0 4 3 5", "443 1 0 0 0 0 1 4 3 5"),
        }
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            for name, content in variants.items():
                (d / name).write_text(content)
            def row(name):
                return {"path": name, "source_id": "source-" + name[0], "status": "ok"}
            inventory = {"pools": {"old": {"files": [row(name) for name in ("a.lhe", "b.lhe", "c.lhe", "e.lhe")]},
                                   "new": {"files": [row("d.lhe")]}}}
            (d / "inventory.json").write_text(json.dumps(inventory))
            config = {"inventory": str(d / "inventory.json"), "base": td,
                      "cohorts": {"old": {"pool": "old"}, "new": {"pool": "new"}}}
            (d / "config.json").write_text(json.dumps(config))
            with mock.patch("builtins.print"):
                self.assertEqual(AUDIT.main(["--config", str(d / "config.json"),
                                             "--output", str(d / "result.json")]), 0)
            report = json.loads((d / "result.json").read_text())
        files = {f["path"]: f for cohort in report["cohorts"].values() for f in cohort["files"]}
        shared = files["a.lhe"]["kinematic_stream_sha256"]
        self.assertEqual(shared, files["b.lhe"]["kinematic_stream_sha256"])
        self.assertEqual(shared, files["d.lhe"]["kinematic_stream_sha256"])
        self.assertNotEqual(shared, files["c.lhe"]["kinematic_stream_sha256"])
        self.assertNotEqual(shared, files["e.lhe"]["kinematic_stream_sha256"])
        self.assertEqual(files["c.lhe"]["counts"]["events"], 1)
        self.assertEqual(files["a.lhe"]["duplicate"]["repeated_event_fingerprints"], 0)
        within = report["cohorts"]["old"]["matching_kinematic_streams"]
        self.assertEqual(len(within), 1)
        self.assertEqual({s["source_id"] for s in within[0]["sources"]}, {"source-a", "source-b"})
        across = report["cross_cohort_kinematic_stream_matches"]
        self.assertEqual(len(across), 1)
        self.assertEqual({(s["cohort"], s["path"]) for s in across[0]["sources"]},
                         {("old", "a.lhe"), ("old", "b.lhe"), ("new", "d.lhe")})

    def test_parser_system_exit_reports_source_and_code(self):
        try:
            import awkward
            import numpy
        except ImportError:
            self.skipTest("LCG numpy/awkward environment required")
        source = "/store/user/example/single.lhe.gz"
        def failing_events():
            yield object()
            raise SystemExit(2)
        parser = mock.Mock(read_lhe=mock.Mock(return_value=failing_events()))
        with mock.patch.object(AUDIT, "imports", return_value=(parser, numpy, awkward)):
            with self.assertRaisesRegex(RuntimeError, r"/store/user/example/single\.lhe\.gz: exit code 2") as caught:
                AUDIT.analyze_file({"path": source}, "")
        self.assertIsInstance(caught.exception.__cause__, SystemExit)

    def test_lfn_endpoint_config_preserves_triple_slash_and_local_paths(self):
        try:
            import awkward, numpy, pylhe  # noqa: F401
        except ImportError:
            self.skipTest("LCG pylhe/numpy/awkward environment required")
        lfn = "/store/user/example/sample.lhe.gz"
        endpoint = "root://cceos.ihep.ac.cn:1094"
        self.assertEqual(AUDIT.resolve_input_path(lfn, "", None), lfn)
        self.assertEqual(AUDIT.resolve_input_path("/tmp/local.lhe", "", endpoint), "/tmp/local.lhe")
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            (d / "sample.lhe").write_text(LHE)
            inventory = {"pools": {"p": {"files": [{"path": lfn, "status": "ok"}]}}}
            (d / "inventory.json").write_text(json.dumps(inventory))
            cfg = {"inventory": str(d / "inventory.json"), "lfn_endpoint": endpoint,
                   "cohorts": {"default": {"pool": "p"},
                               "override": {"pool": "p", "lfn_endpoint": "root://other.example:1094"}}}
            (d / "config.json").write_text(json.dumps(cfg))
            with mock.patch.object(AUDIT, "open_input", return_value=(str(d / "sample.lhe"), False)) as stage:
                self.assertEqual(AUDIT.main(["--config", str(d / "config.json"),
                                             "--output", str(d / "result.json")]), 0)
            self.assertEqual([call.args[0] for call in stage.call_args_list], [
                "root://cceos.ihep.ac.cn:1094///store/user/example/sample.lhe.gz",
                "root://other.example:1094///store/user/example/sample.lhe.gz"])
            self.assertEqual(json.loads((d / "result.json").read_text())["cohorts"]["default"]["events"], 2)

    def test_stratified_sample_has_exact_largest_remainder_allocation(self):
        rows = ([{"path": f"a{i}", "source_namespace": "A"} for i in range(3)] +
                [{"path": f"b{i}", "source_namespace": "B"} for i in range(3)] +
                [{"path": f"c{i}", "source_namespace": "C"} for i in range(3)])
        selected = AUDIT.choose(rows, 4, 9)
        self.assertEqual(len(selected), 4)
        self.assertEqual({ns: sum(r["source_namespace"] == ns for r in selected) for ns in "ABC"},
                         {"A": 2, "B": 1, "C": 1})
        self.assertEqual(selected, AUDIT.choose(rows, 4, 9))

    def test_remote_gzip_staging_preserves_suffix_for_pylhe(self):
        try:
            import pylhe
        except ImportError:
            self.skipTest("LCG pylhe environment required")
        os.makedirs("/tmp/chiw", exist_ok=True)
        def fake_xrdcp(command, check, env):
            self.assertEqual(command[:3], ["/usr/bin/xrdcp", "--nopbar", "--force"])
            self.assertTrue(check)
            self.assertNotIn("LD_LIBRARY_PATH", env)
            self.assertNotIn("PYTHONPATH", env)
            self.assertEqual(env["X509_USER_PROXY"], "/tmp/test-proxy")
            self.assertEqual(env["XRD_NETWORKSTACK"], "IPv4")
            self.assertTrue(command[4].endswith(".lhe.gz"))
            with gzip.open(command[4], "wt") as staged:
                staged.write(LHE)
            return subprocess.CompletedProcess(command, 0)
        with mock.patch.dict(os.environ, {"LD_LIBRARY_PATH": "/lcg/lib", "PYTHONPATH": "/lcg/python",
                                              "X509_USER_PROXY": "/tmp/test-proxy", "XRD_NETWORKSTACK": "IPv4"}):
            with mock.patch.object(subprocess, "run", side_effect=fake_xrdcp) as run:
                staged, cleanup = AUDIT.open_input("root://example:1094///store/sample.lhe.gz")
            self.assertEqual(os.environ["LD_LIBRARY_PATH"], "/lcg/lib")
            self.assertEqual(os.environ["PYTHONPATH"], "/lcg/python")
        try:
            self.assertTrue(cleanup)
            self.assertTrue(staged.endswith(".lhe.gz"))
            self.assertEqual(sum(1 for _ in pylhe.read_lhe(staged)), 2)
            run.assert_called_once()
            self.assertEqual(run.call_args.args[0][3], "root://example:1094///store/sample.lhe.gz")
        finally:
            Path(staged).unlink(missing_ok=True)

    def test_columnar_observables_and_duplicate_fingerprint(self):
        try:
            import awkward  # noqa: F401
            import numpy  # noqa: F401
            import pylhe  # noqa: F401
        except ImportError:
            self.skipTest("LCG pylhe/numpy/awkward environment required")
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            (d / "sample.lhe").write_text(LHE)
            inventory = {"pools": {"pool_x": {"files": [{"path": "sample.lhe", "seed": 7,
                "source_namespace": "/test", "source_id": "a", "event_sha256": "whole-a", "status": "ok"}]}}}
            (d / "inventory.json").write_text(json.dumps(inventory))
            config = {"inventory": str(d / "inventory.json"), "sample_seed": 14,
                "cohorts": {"sample": {"pool": "pool_x", "base": td}}, "comparisons": []}
            (d / "config.json").write_text(json.dumps(config))
            output = d / "result.json"
            env = os.environ.copy()
            proc = subprocess.run(["python3", str(SCRIPT), "--config", str(d / "config.json"), "--output", str(output)],
                                  text=True, capture_output=True, env=env)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            result = json.loads(output.read_text())["cohorts"]["sample"]
            self.assertEqual(result["events"], 2)
            f = result["files"][0]
            self.assertEqual(f["counts"]["events_exactly_two_jpsi"], 2)
            self.assertEqual(f["duplicate"]["repeated_event_fingerprints"], 0)
            self.assertEqual(f["histograms"]["jpsi_pt"]["counts"], [0, 0, 0, 4, 0, 0, 0, 0, 0])
            self.assertEqual(f["moments"]["jpsi_y"]["mean"], 0.0)
            self.assertAlmostEqual(f["moments"]["pair_pt"]["mean"], 0.0)
            self.assertAlmostEqual(f["moments"]["pair_mass"]["mean"], 10.0)
            self.assertAlmostEqual(f["moments"]["delta_y"]["mean"], 1.791759469228055)

    def test_gz_input(self):
        try:
            import awkward, numpy, pylhe  # noqa: F401
        except ImportError:
            self.skipTest("LCG pylhe/numpy/awkward environment required")
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            with gzip.open(d / "sample.lhe.gz", "wt") as f: f.write(LHE)
            inventory = {"pools": {"p": {"files": [{"path": str(d / "sample.lhe.gz"), "status": "ok"}]}}}
            (d / "i.json").write_text(json.dumps(inventory))
            (d / "c.json").write_text(json.dumps({"inventory": str(d / "i.json"), "cohorts": {"c": {"pool": "p"}}}))
            p = subprocess.run(["python3", str(SCRIPT), "--config", str(d / "c.json"), "--output", str(d / "o.json")], capture_output=True, text=True)
            self.assertEqual(p.returncode, 0, p.stderr)
            self.assertEqual(json.loads((d / "o.json").read_text())["cohorts"]["c"]["events"], 2)


if __name__ == "__main__":
    unittest.main()
