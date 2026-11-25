"""Offline IHEP preparation and exact-attempt submission tests."""
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.backends.hepjob import (prepare_batch, resume_batch, SAFE_NTUPLE_NODES,
                                     status_batch, submit_batch)
from full_mc.cli import build_parser, dispatch
from full_mc.io import atomic_json, read_json
from full_mc.runtime.packages import CMSSW15_RUNTIME, CMSSW15_SOURCE_PACKAGE


def handoff(root, count=1):
    tasks = []
    for index in range(count):
        for stage in ("merge", "ntuple", "selection", "efficiency"):
            tasks.append({"task_id": f"g{index}:{stage}", "stage": stage,
                          "runtime_release": "r1",
                          "parameters": {"merge_group": f"g{index}", "analysis": "JJP"},
                          "inputs": [], "outputs": []})
    path = root / "handoff.json"
    atomic_json(path, {"schema": "full-mc-handoff-v1", "run_id": "r", "tasks": tasks})
    return path


def runtime(root, release="r1", revision="core1", *, compiled=True):
    root.mkdir()
    (root / "core.tgz").write_bytes(b"core")
    controller = root / "controller/core/src/full_mc/runtime"
    controller.mkdir(parents=True)
    (root / "controller/core/mcprod").write_text("#!/bin/sh\n", encoding="utf-8")
    (controller / "bootstrap.py").write_text("# bootstrap\n", encoding="utf-8")
    library = "libHeavyFlavorAnalysisTPS-Onia2MuMu.so"
    if compiled:
        asset = CMSSW15_RUNTIME
        members = {
            "CMSSW_15_0_15/src/HeavyFlavorAnalysis/TPS-Onia2MuMu/BuildFile.xml": b"# fixture",
            f"CMSSW_15_0_15/lib/el9_amd64_gcc12/{library}": b"plugin",
            "CMSSW_15_0_15/lib/el9_amd64_gcc12/.edmplugincache":
                f"{library} MultiLepPAT CMS%20EDM%20Framework%20Module\n".encode(),
        }
    else:
        asset = CMSSW15_SOURCE_PACKAGE
        members = {"HeavyFlavorAnalysis/TPS-Onia2MuMu/BuildFile.xml": b"# fixture"}
    with tarfile.open(root / asset, "w:gz") as archive:
        for name, contents in members.items():
            member = tarfile.TarInfo(name)
            member.size = len(contents)
            archive.addfile(member, io.BytesIO(contents))
    atomic_json(root / "runtime.json", {"runtime_release": release, "site": "ihep",
                                         "git_revision": revision,
                                         "profiles": ["cmssw12-sim-el8", "cmssw15-ntuple-el9", "analyzer-el9"],
                                         "bundles": {"core": "core.tgz",
                                                     "cmssw15-ntuple-el9": asset}})
    return root


def hep_q_output(job_id=None, state="I"):
    row = (f"{job_id}.0      wangchi         09/28 21:56     0+00:00:00      "
           f"{state}  0   0.0  0000-persistent.sh \n") if job_id else ""
    count = 1 if job_id else 0
    return ("JOBID\t\tOWNER\t\tSUBMITTED\tRUN_TIME\tST PRI SIZE CMD\n"
            + row + f"{count} jobs; 0 completed, 0 removed, {count} idle, "
            "0 running, 0 held, 0 suspended\n")


class IhepContracts(unittest.TestCase):
    def setUp(self):
        proxy_env = patch.dict(os.environ, {"X509_USER_PROXY": "/tmp/test-proxy"})
        proxy_env.start()
        self.addCleanup(proxy_env.stop)
        proxy_check = patch("full_mc.backends.hepjob.ensure_valid_proxy",
                            return_value=172801)
        proxy_check.start()
        self.addCleanup(proxy_check.stop)

    def test_prepared_wrappers_are_private_and_nodes_round_robin(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root, 12), root / "batch", proxy="/proxy path")
            self.assertEqual(len(batch["jobs"]), 12)
            self.assertEqual(batch["jobs"][0]["node"], SAFE_NTUPLE_NODES[0] + ".ihep.ac.cn")
            self.assertEqual(batch["jobs"][11]["node"], batch["jobs"][0]["node"])
            checkpoints = set()
            for job in batch["jobs"]:
                wrapper = Path(job["wrapper"])
                subprocess.run(["bash", "-n", str(wrapper)], check=True)
                text = wrapper.read_text()
                self.assertIn('mktemp -d "$scratch_base/mcprod-ihep.XXXXXX"', text)
                self.assertIn('${TMPDIR:-/tmp}', text)
                scratch_lines = [line for line in text.splitlines()
                                 if line.startswith(("scratch_base=", "job_scratch="))]
                self.assertEqual(scratch_lines, [
                    'scratch_base="${TMPDIR:-/tmp}/${USER:-$(id -un)}"',
                    'job_scratch=$(mktemp -d "$scratch_base/mcprod-ihep.XXXXXX")',
                ])
                self.assertIn("credentials/x509_user_proxy", text)
                self.assertNotIn("x509up_u", text)
                self.assertIn("proxy_source='/proxy path'", text)
                checkpoints.add(read_json(job["task_path"])["checkpoint_dir"])
            self.assertEqual(len(checkpoints), 12)
            with self.assertRaises(FileExistsError):
                prepare_batch(root / "handoff.json", root / "batch")

    def test_proxy_required_by_default_blocks_preparation_and_submission(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            handoff_path = handoff(root)
            runner = Mock()
            with patch("full_mc.backends.hepjob.ensure_valid_proxy",
                       side_effect=RuntimeError("proxy expired")) as check:
                with self.assertRaisesRegex(RuntimeError, "proxy expired"):
                    prepare_batch(handoff_path, root / "batch", proxy="/test-proxy")
                check.assert_called_once_with("/test-proxy")
            self.assertFalse((root / "batch").exists())

            batch = prepare_batch(handoff_path, root / "batch", proxy="/test-proxy")
            with patch("full_mc.backends.hepjob.ensure_valid_proxy",
                       side_effect=RuntimeError("proxy expired")) as check:
                with self.assertRaisesRegex(RuntimeError, "proxy expired"):
                    submit_batch(batch["manifest_path"], runner=runner)
                check.assert_called_once_with("/test-proxy")
            runner.assert_not_called()
            self.assertEqual(read_json(batch["manifest_path"])["jobs"][0]["state"], "prepared")

    def test_explicit_local_stages_prepare_and_submit_without_proxy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            handoff_path = handoff(root)
            payload = read_json(handoff_path)
            for stage in payload["tasks"]:
                stage["requires_proxy"] = False
            atomic_json(handoff_path, payload)
            with patch.dict(os.environ, {"X509_USER_PROXY": ""}), \
                    patch("full_mc.backends.hepjob.ensure_valid_proxy") as check, \
                    self.assertLogs("full_mc.backends.hepjob", level="INFO") as logs:
                batch = prepare_batch(handoff_path, root / "batch")
                completed = Mock(returncode=0, stdout="submitted to cluster 27", stderr="")
                submitted = submit_batch(batch["manifest_path"], runner=Mock(return_value=completed))
            self.assertFalse(batch["jobs"][0]["requires_proxy"])
            self.assertNotIn("credentials/x509_user_proxy", Path(batch["jobs"][0]["wrapper"]).read_text())
            self.assertEqual(submitted["jobs"][0]["state"], "submitted")
            self.assertIn("Skipping X.509 proxy check for local stage merge", " ".join(logs.output))
            check.assert_not_called()

    def test_worker_default_scratch_follows_tmpdir(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task_path = root / "task.json"
            atomic_json(task_path, {"task_id": "dummy", "stage": "merge"})
            tmpdir = root / "user-scratch"
            tmpdir.mkdir()
            with patch.dict(os.environ, {"TMPDIR": str(tmpdir), "USER": "another-user"}), \
                    patch.object(tempfile, "tempdir", None), \
                    patch("full_mc.workers.execute_task", return_value={
                        "task_id": "dummy", "stage": "merge", "status": "ok"}) as worker:
                dispatch(build_parser().parse_args(["worker", "--task", str(task_path)]))
            scratch = worker.call_args.args[1]
            self.assertEqual(scratch.parent, tmpdir)
            self.assertTrue(scratch.name.startswith("mcprod-worker-"))

    def test_ambiguous_attempt_is_not_resubmitted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root), root / "batch")
            failed = Mock(returncode=1, stdout="", stderr="network cut after submit")
            submit_batch(batch["manifest_path"], runner=Mock(return_value=failed))
            updated = read_json(batch["manifest_path"])
            self.assertEqual(updated["jobs"][0]["state"], "unknown")
            no_submit = Mock(side_effect=AssertionError("must not retry unknown attempt"))
            resume_batch(batch["manifest_path"], runner=no_submit)
            no_submit.assert_not_called()

    def test_worker_archives_ntuple_diagnostics_and_preserves_exit_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            proxy = root / "proxy"
            proxy.write_text("test proxy")
            (root / "mcprod").write_text(
                "from pathlib import Path\n"
                "import os, sys\n"
                "work = Path(sys.argv[-1])\n"
                "print(work.parent)\n"
                "stage = work / 'ntuple'\n"
                "(stage / 'command_logs').mkdir(parents=True)\n"
                "(stage / 'commands.log').write_text('ntuple stage summary')\n"
                "(stage / 'command_logs/001_cmsRun.stderr').write_text('original CMSSW error')\n"
                "(stage / 'output_ntuple.root').touch()\n"
                "sys.exit(int(os.environ['TEST_WORKER_RC']))\n"
            )
            batch = prepare_batch(handoff(root), root / "batch", proxy=proxy, core_root=root)
            submit_batch(batch["manifest_path"], runner=Mock(return_value=Mock(
                returncode=0, stdout="submitted to cluster 27", stderr="")))
            command = read_json(batch["manifest_path"])["jobs"][0]["attempts"][0]["command"]
            logs = Path(command[command.index("-e") + 1]).parent
            attempt = command[command.index("-argu") + 1]

            tmpdir = root / "job-temp"
            scratch_base = tmpdir / "another-user"
            scratch_base.mkdir(parents=True)
            keep = scratch_base / "keep"
            keep.touch()
            job_env = dict(os.environ, TMPDIR=str(tmpdir), USER="another-user")
            for rc in (42, 0):
                with self.subTest(rc=rc):
                    execution = subprocess.run([command[1], attempt], text=True, capture_output=True,
                                               env=dict(job_env, TEST_WORKER_RC=str(rc)))
                    self.assertEqual(execution.returncode, rc, execution.stderr)
                    scratch = Path(execution.stdout.strip())
                    self.assertEqual(scratch.parent, scratch_base)
                    self.assertFalse(scratch.exists())
                    self.assertTrue(keep.is_file())
                    destination = Path(execution.stderr.removeprefix("HepJob diagnostics: ").strip())
                    self.assertEqual(destination.parent, logs)
                    with tarfile.open(destination) as diagnostics:
                        self.assertEqual(set(diagnostics.getnames()), {
                            "execution.txt", "worker/ntuple/commands.log",
                            "worker/ntuple/command_logs/001_cmsRun.stderr",
                        })
                        self.assertIn(f"exit_code={rc}".encode(),
                                      diagnostics.extractfile("execution.txt").read())
                        self.assertEqual(
                            diagnostics.extractfile(
                                "worker/ntuple/command_logs/001_cmsRun.stderr").read(),
                            b"original CMSSW error")
            self.assertEqual(len(list(logs.glob("execution-*.tar.gz"))), 2)
            self.assertEqual(list(logs.glob(".execution-*")), [])

            no_destination = subprocess.run(
                [command[1], "attempt-002"], text=True, capture_output=True,
                env=dict(job_env, TEST_WORKER_RC="42"))
            self.addCleanup(shutil.rmtree, no_destination.stdout.strip(), ignore_errors=True)
            self.assertEqual(no_destination.returncode, 42)
            self.assertIn("HepJob diagnostics could not be archived", no_destination.stderr)

    def test_crash_after_attempt_record_does_not_duplicate_submission(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root), root / "batch")
            with self.assertRaises(KeyboardInterrupt):
                submit_batch(batch["manifest_path"], runner=Mock(side_effect=KeyboardInterrupt))
            self.assertEqual(read_json(batch["manifest_path"])["jobs"][0]["state"], "submitting")
            no_submit = Mock(side_effect=AssertionError("must reconcile crashed attempt"))
            resume_batch(batch["manifest_path"], runner=no_submit)
            no_submit.assert_not_called()

    def test_status_and_explicit_recovery_use_only_the_recorded_job(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root), root / "batch")
            submit_batch(batch["manifest_path"], runner=Mock(return_value=Mock(
                returncode=0, stdout="submitted to cluster 27", stderr="")))
            active = Mock(return_value=Mock(returncode=0, stdout=hep_q_output("27"), stderr=""))
            status_batch(batch["manifest_path"], runner=active)
            self.assertEqual(active.call_args.args[0], ["hep_q", "-i", "27"])
            self.assertEqual(read_json(batch["manifest_path"])["jobs"][0]["state"], "submitted")

            failed = Mock(return_value=Mock(returncode=0, stdout=hep_q_output("27", "F"), stderr=""))
            status_batch(batch["manifest_path"], runner=failed)
            self.assertEqual(read_json(batch["manifest_path"])["jobs"][0]["state"], "failed")
            recovered = resume_batch(batch["manifest_path"], runner=Mock(return_value=Mock(
                returncode=0, stdout="submitted to cluster 28", stderr="")))
            job = recovered["jobs"][0]
            self.assertEqual((job["state"], job["job_id"], job["recovery_of"]),
                             ("submitted", "28", "27"))
            self.assertEqual(len(job["attempts"]), 2)
            for number, attempt in enumerate(job["attempts"], 1):
                command = attempt["command"]
                name = f"attempt-{number:03d}"
                self.assertEqual(command[command.index("-argu") + 1], name)
                self.assertEqual(Path(command[command.index("-e") + 1]),
                                 root / "batch" / "logs" / "0000" / name / "hepjob.err")

    def test_hep_q_summary_and_other_jobs_cannot_authorize_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root), root / "batch")
            submit_batch(batch["manifest_path"], runner=Mock(return_value=Mock(
                returncode=0, stdout="submitted to cluster 27", stderr="")))
            for output, expected in (
                (hep_q_output("27"), "active"),
                (hep_q_output(), "unknown"),
                (hep_q_output("28", "F"), "unknown"),
                (hep_q_output("27") + hep_q_output("27", "F"), "unknown"),
            ):
                with self.subTest(output=output):
                    result = Mock(return_value=Mock(returncode=0, stdout=output, stderr=""))
                    status_batch(batch["manifest_path"], runner=result)
                    job = read_json(batch["manifest_path"])["jobs"][0]
                    self.assertEqual(job["state"], "submitted")
                    self.assertEqual(job["scheduler_state"], expected)
                    self.assertFalse(job.get("recovery_eligible", False))
                    self.assertEqual(job.get("reconciliation_required", False),
                                     expected == "unknown")

            def query_only(command, **_kwargs):
                self.assertEqual(command, ["hep_q", "-i", "27"])
                return Mock(returncode=0, stdout=hep_q_output(), stderr="")

            resume_batch(batch["manifest_path"], runner=query_only)
            self.assertEqual(read_json(batch["manifest_path"])["jobs"][0]["state"],
                             "submitted")

    def test_hep_q_cli_diagnostic_never_marks_a_job_failed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch = prepare_batch(handoff(root), root / "batch")
            submit_batch(batch["manifest_path"], runner=Mock(return_value=Mock(
                returncode=0, stdout="submitted to cluster 27", stderr="")))

            usage_error = (
                "usage: hep_q.py [-h] [-i ID]\n"
                "hep_q.py: error: unrecognized arguments: 27")
            for stdout, stderr in (("", usage_error), (usage_error, "")):
                diagnostic = Mock(return_value=Mock(returncode=0, stdout=stdout,
                                                    stderr=stderr))
                status_batch(batch["manifest_path"], runner=diagnostic)
                job = read_json(batch["manifest_path"])["jobs"][0]
                self.assertEqual(job["state"], "submitted")
                self.assertEqual(job["scheduler_state"], "unknown")
                self.assertTrue(job["reconciliation_required"])
                self.assertEqual(job["scheduler_query"], ["hep_q", "-i", "27"])

    def test_pinned_handoff_requires_matching_ihep_runtime_controller(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = read_json(handoff(root))
            payload.update(runtime_release="r1", core_revision="core1")
            atomic_json(root / "handoff.json", payload)
            with self.assertRaisesRegex(ValueError, "runtime-dir"):
                prepare_batch(root / "handoff.json", root / "missing")
            source_only = runtime(root / "source-runtime", compiled=False)
            with self.assertRaisesRegex(ValueError, "requires a compiled CMSSW15 runtime"):
                prepare_batch(root / "handoff.json", root / "batch", runtime_dir=source_only)
            release = runtime(root / "runtime")
            batch = prepare_batch(root / "handoff.json", root / "batch", runtime_dir=release)
            task = read_json(batch["jobs"][0]["task_path"])
            self.assertEqual(task["runtime_dir"], str(release))
            self.assertEqual(task["subtasks"]["merge"]["parameters"]["core_root"],
                             str(release / "controller/core"))

    def test_cli_routes_batch_status_and_runtime_directory(self):
        parser = build_parser()
        prepared = parser.parse_args(["batch", "prepare", "--handoff", "handoff.json",
                                      "--output-dir", "batch", "--runtime-dir", "runtime"])
        self.assertEqual(prepared.runtime_dir, Path("runtime"))
        status = parser.parse_args(["batch", "status", "--manifest", "batch_manifest.json"])
        with patch("full_mc.backends.hepjob.status_batch", return_value={"checked": True}) as checked:
            self.assertEqual(dispatch(status), {"checked": True})
        checked.assert_called_once_with(Path("batch_manifest.json").resolve())

    def test_cli_forwards_log_level_to_batch_and_aggregation_preparation(self):
        parser = build_parser()
        batch = parser.parse_args(["batch", "prepare", "--handoff", "handoff.json",
                                   "--output-dir", "batch", "--log-level", "warning"])
        with patch("full_mc.backends.hepjob.prepare_batch", return_value={"prepared": True}) as prepared:
            self.assertEqual(dispatch(batch), {"prepared": True})
        self.assertEqual(prepared.call_args.kwargs["log_level"], "warning")

        aggregate = parser.parse_args(["analysis", "aggregate", "--run", "run", "--prepare",
                                       "--handoff", "handoff.json", "--output", "aggregate.json",
                                       "--log-level", "error"])
        with patch("full_mc.application.analysis.prepare_aggregate",
                   return_value={"prepared": True}) as prepared_aggregate:
            self.assertEqual(dispatch(aggregate), {"prepared": True})
        self.assertEqual(prepared_aggregate.call_args.kwargs["log_level"], "error")


if __name__ == "__main__":
    unittest.main()
