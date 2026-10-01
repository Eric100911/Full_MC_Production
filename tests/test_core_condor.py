"""Offline contracts for exact, durable DAGMan submission attempts."""
from pathlib import Path
import json
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.backends.condor import resume_run, status_run, submit_run
from full_mc.io import read_json


def response(code=0, stdout="", stderr=""):
    return SimpleNamespace(returncode=code, stdout=stdout, stderr=stderr)


def run(root, schedd="schedd.example"):
    (root / "run.dag").write_text("JOB A a.sub\n", encoding="utf-8")
    (root / "run.json").write_text('{"condor":{"schedd":"%s"}}\n' % schedd,
                                    encoding="utf-8")
    (root / "submit").mkdir()
    (root / "submit/plan.sub").write_text(
        "x509userproxy = /afs/cern.ch/user/test/proxy\n", encoding="utf-8")


class CondorContracts(unittest.TestCase):
    def setUp(self):
        proxy_check = patch("full_mc.backends.condor.require_initial_proxy_lifetime",
                            return_value=172801)
        proxy_check.start()
        self.addCleanup(proxy_check.stop)

    def test_submit_persists_exact_pair_before_and_after_submit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            def runner(command, **kwargs):
                saved = read_json(root / "submission.json")
                self.assertEqual(saved["state"], "submitting")
                self.assertEqual(saved["scheduler"], "schedd.example")
                self.assertIsNone(saved["root_cluster_id"])
                self.assertEqual(command, ["condor_submit_dag", "-no_recurse", "run.dag"])
                self.assertEqual(kwargs["cwd"], str(root))
                self.assertEqual(kwargs["env"]["_CONDOR_SCHEDD_HOST"], "schedd.example")
                return response(stdout="1 job(s) submitted to cluster 91234.\n")
            result = submit_run(root, runner=runner)
            self.assertEqual((result["scheduler"], result["root_cluster_id"]),
                             ("schedd.example", "91234"))

    def test_ambiguous_or_interrupted_submit_is_never_duplicated(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            submit_run(root, runner=Mock(return_value=response(1, stderr="connection lost")))
            saved = read_json(root / "submission.json")
            self.assertEqual(saved["state"], "unknown")
            with self.assertRaises(RuntimeError):
                submit_run(root, runner=Mock(side_effect=AssertionError("no retry")))
            with self.assertRaises(RuntimeError):
                resume_run(root, runner=Mock(side_effect=AssertionError("no retry")))
            root2 = root / "interrupted"; root2.mkdir(); run(root2)
            with self.assertRaises(KeyboardInterrupt):
                submit_run(root2, runner=Mock(side_effect=KeyboardInterrupt))
            self.assertEqual(read_json(root2 / "submission.json")["state"], "submitting")

    def test_storage_reservation_failure_prevents_scheduler_submission(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            payload = {
                "condor": {"schedd": "schedd.example"},
                "run_id": "reservation-test", "runtime_release": "r1",
                "storage": {"cern_staging": "root://eosuser.cern.ch///eos/user/c/chiw/staging",
                            "ihep_products": str(root / "products"),
                            "ihep_archive": str(root / "archive")},
            }
            (root / "run.json").write_text(json.dumps(payload) + "\n", encoding="utf-8")
            runner = Mock(side_effect=AssertionError("scheduler must not be called"))
            with patch("full_mc.backends.condor.reserve_run_namespaces",
                       side_effect=RuntimeError("namespace is owned")):
                with self.assertRaisesRegex(RuntimeError, "namespace is owned"):
                    submit_run(root, runner=runner)
            runner.assert_not_called()
            self.assertFalse((root / "submission.json").exists())

    def test_status_queries_only_the_recorded_root_on_recorded_schedd(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            submit_run(root, runner=Mock(return_value=response(stdout="submitted to cluster 42")))
            runner = Mock(return_value=response(stdout="42 0 2\n"))
            status = status_run(root, runner=runner)
            self.assertEqual(status["state"], "active")
            self.assertEqual(runner.call_args.args[0][:4], ["condor_q", "-name", "schedd.example", "42"])

    def test_history_query_bounds_the_schedd_side_scan(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            submit_run(root, runner=Mock(return_value=response(stdout="submitted to cluster 42")))
            seen = {}
            def runner(command, **kwargs):
                seen[command[0]] = (command, kwargs)
                return response(stdout="" if command[0] == "condor_q" else "42 0 4\n")
            status = status_run(root, runner=runner)
            self.assertEqual(status["query"], "history")
            self.assertNotIn("-scanlimit", seen["condor_q"][0])
            history, kwargs = seen["condor_history"]
            self.assertEqual(history[history.index("-scanlimit") + 1], "10000")
            self.assertEqual(kwargs["timeout"], 60)

    def test_empty_queue_and_history_use_exact_local_success_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            submit_run(root, runner=Mock(return_value=response(stdout="submitted to cluster 42")))
            (root / "state.json").write_text('{"status":"complete"}\n', encoding="utf-8")
            (root / ".complete").write_text("", encoding="utf-8")
            (root / "run.dag.metrics").write_text(json.dumps({
                "dagman_id": "42", "exitcode": 0, "DagStatus": 0,
                "dag_nodes": 28, "dag_nodes_succeeded": 28,
                "dag_nodes_failed": 0,
            }), encoding="utf-8")
            runner = Mock(return_value=response(stdout=""))

            status = status_run(root, runner=runner)

        self.assertEqual(status["state"], "terminal")
        self.assertTrue(status["terminal"])
        self.assertTrue(status["successful"])
        self.assertEqual(status["query"], "local-evidence")
        self.assertEqual(status["root_cluster_id"], "42")
        self.assertEqual(status["local_evidence"]["dag_nodes_succeeded"], 28)

    def test_local_success_evidence_rejects_metrics_from_another_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            submit_run(root, runner=Mock(return_value=response(stdout="submitted to cluster 42")))
            (root / "state.json").write_text('{"status":"complete"}\n', encoding="utf-8")
            (root / ".complete").write_text("", encoding="utf-8")
            (root / "run.dag.metrics").write_text(json.dumps({
                "dagman_id": "41", "exitcode": 0, "DagStatus": 0,
                "dag_nodes": 28, "dag_nodes_succeeded": 28,
                "dag_nodes_failed": 0,
            }), encoding="utf-8")

            status = status_run(root, runner=Mock(return_value=response(stdout="")))

        self.assertEqual(status["state"], "unknown")
        self.assertFalse(status["terminal"])

    def test_resume_requires_terminal_state_and_a_concrete_rescue_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            submit_run(root, runner=Mock(return_value=response(stdout="submitted to cluster 42")))
            with self.assertRaises(RuntimeError):
                resume_run(root, runner=Mock(return_value=response(stdout="42 0 2\n")))
            rescue = root / "run.dag.rescue002"; rescue.write_text("DONE A\n", encoding="utf-8")
            def runner(command, **_kwargs):
                if command[0] == "condor_q":
                    return response(stdout="")
                if command[0] == "condor_history":
                    return response(stdout="42 0 4\n")
                self.assertEqual(command, ["condor_submit_dag", "-no_recurse", "-AutoRescue", "0",
                                           "-RescueFile", "run.dag.rescue002", "run.dag"])
                self.assertEqual(_kwargs["env"]["_CONDOR_SCHEDD_HOST"], "schedd.example")
                return response(stdout="submitted to cluster 43")
            result = resume_run(root, runner=runner)
            self.assertEqual(result["root_cluster_id"], "43")
            self.assertEqual(result["attempts"][-1]["kind"], "rescue")


if __name__ == "__main__":
    unittest.main()
