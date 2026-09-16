"""Offline contracts for exact, durable DAGMan submission attempts."""
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.backends.condor import resume_run, status_run, submit_run
from full_mc.io import read_json


def response(code=0, stdout="", stderr=""):
    return SimpleNamespace(returncode=code, stdout=stdout, stderr=stderr)


def run(root, schedd="schedd.example"):
    (root / "run.dag").write_text("JOB A a.sub\n", encoding="utf-8")
    (root / "run.json").write_text('{"condor":{"schedd":"%s"}}\n' % schedd,
                                    encoding="utf-8")


class CondorContracts(unittest.TestCase):
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

    def test_status_queries_only_the_recorded_root_on_recorded_schedd(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); run(root)
            submit_run(root, runner=Mock(return_value=response(stdout="submitted to cluster 42")))
            runner = Mock(return_value=response(stdout="42 0 2\n"))
            status = status_run(root, runner=runner)
            self.assertEqual(status["state"], "active")
            self.assertEqual(runner.call_args.args[0][:4], ["condor_q", "-name", "schedd.example", "42"])

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
