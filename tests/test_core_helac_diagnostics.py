"""Offline HELAC evidence retention, using the real shell logging functions."""
import gzip
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

CORE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORE / "src"))
sys.path.insert(0, str(CORE))

from full_mc.runtime import bootstrap
from full_mc.workers import lhe
from tools.helac_inspector import parse_root


def task(retain=True, stage="lhe"):
    return {"task_id": "HELAC_TEST", "stage": stage, "runtime": "helac-el7",
            "runtime_release": "test", "parameters": {
                "retain_helac_diagnostics": retain,
                "pool_name": "pool_2jpsi_g", "seed": 93000,
                "generator_profile": {"nmc": 100, "unwevt": 10}}}


def put(root, name, text):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def shell_helpers():
    """Extract definitions without sourcing the script's top-level operations."""
    lines = (CORE / "lhe_generation/run_helac.sh").read_text().splitlines()
    definitions = []
    for name in ("_slim", "msg_info", "msg_warn", "msg_error",
                 "sanitize_log_label", "ensure_job_log_dir", "show_log_tail",
                 "run_logged", "run_logged_bash"):
        start = next(i for i, line in enumerate(lines)
                     if line.startswith(name + "() {"))
        end = start
        if not lines[start].endswith("}"):
            end = next(i for i in range(start + 1, len(lines)) if lines[i] == "}")
        definitions.extend(lines[start:end + 1])
    return "\n".join(definitions)


def run_native_command(root, retain, exit_code):
    # Exercise the same run_logged_bash -> run_logged boundary as ho_cluster.
    command = ("printf 'maximum weight warning\\nstopping: integration ended\\n'; "
               "printf 'native stderr warning\\n' >&2; "
               + shlex.quote(sys.executable) + " -c 'sum(range(100000))'; "
               + f"exit {exit_code}")
    script = ("set -e\nWORKFLOW_LOG_LEVEL=slim\n"
              'JOB_LOG_DIR="$PWD/command_logs"\nCOMMAND_LOG_INDEX=0\n'
              + shell_helpers() + "\nrun_logged_bash helac_ho_cluster "
              + shlex.quote(command) + "\n")
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-s"], input=script, cwd=root,
        env=dict(os.environ, HELAC_RETAIN_DIAGNOSTICS=str(int(retain))),
        capture_output=True, text=True, check=False)


class HelacDiagnostics(unittest.TestCase):
    def setUp(self):
        scratch = Path("/tmp/chiw")
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="helac-diag-", dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def evidence_tree(self, root):
        helac = "HELAC-Onia-2.7.6/"
        evidence = {
            "commands.log": "wrapper evidence\n",
            "helac_run.log": "stopping: integration ended\n",
            "py8_onia_user.inp": "2\n443 443\n",
            "helac_forbidden_test_93000.txt": "reason: no nonvanishing subprocesses\n",
            helac + "run_config.ho": "generate g g > cc~(3S11) cc~(3S11) g\n",
            helac + "input/user.inp": "nmc 100\nunwevt 10\n",
            helac + "input/default.inp": "default card\n",
            helac + "input/process.inp": "package process card\n",
            helac + "PROC_HO_7/input/process.inp": "process card\n",
            helac + "PROC_HO_7/P0_calc_0/input/user.inp": "nmc 100\n",
            helac + "PROC_HO_7/P0_calc_0/input/process.inp": "actual calculation process\n",
            helac + "PROC_HO_7/P0_calc_0/input/process_RG.inp": "counterterm process\n",
            helac + "PROC_HO_7/P0_calc_0/input/seed.input": "93000\n",
            helac + "PROC_HO_7/P0_calc_0/output/screen_output.txt":
                "maximum weight exceeded\nstopping: integration ended\n",
            helac + "PROC_HO_7/P0_calc_0/output/screen_error.txt": "native error\n",
            helac + "PROC_HO_7/P0_calc_0/output/RESULT_test.out":
                "sigma= 1.1D-01 1.0D-02 20 100 100\n"
                "maximum weight warning\ntotal sigma (nb) = 1.1D-01 +/- 1.1D-03\n",
            helac + "PROC_HO_7/results/results.out": "native stopping evidence\n",
            helac + "PROC_HO_8/P0_calc_1/input/process.inp": "second process\n",
            helac + "PROC_HO_8/P0_calc_1/output/RESULT_second.out":
                "sigma= 2.0D-01 1.0D-02 20 100 100\n",
        }
        excluded = (
            "products/lhe/sample.lhe.gz", "helac_package.tar.gz",
            helac + "PROC_HO_7/P0_calc_0/output/events.lhe",
            helac + "PROC_HO_7/P0_calc_0/output/events.lhe.gz",
            helac + "PROC_HO_7/P0_calc_0/output/full_trace.dat",
            helac + "PROC_HO_7/P0_calc_0/input/vegas.grid",
            helac + "bin/Helac-Onia", "credentials/x509_user_proxy",
        )
        for name, contents in evidence.items():
            put(root, name, contents)
        for name in excluded:
            put(root, name, "must not be archived\n")
        return evidence, excluded

    def archive_contents(self, current_task, root=None):
        root = root or self.root
        destination = self.root / "returned/diagnostics.tar.gz"
        bootstrap.write_diagnostics(current_task, root, destination)
        with tarfile.open(destination) as archive:
            return {member.name: archive.extractfile(member).read().decode()
                    for member in archive.getmembers()}

    def test_worker_enables_timing_only_for_explicit_boolean(self):
        for value in (True, False, None, "true", 1):
            current_task = task(value)
            if value is None:
                current_task["parameters"].pop("retain_helac_diagnostics")
            with self.subTest(value=value), \
                    patch.object(lhe.shutil, "copy2"), \
                    patch.object(lhe, "run", side_effect=RuntimeError("stop before publish")) as run:
                with self.assertRaisesRegex(RuntimeError, "stop before publish"):
                    lhe.execute(current_task, self.root)
            env = run.call_args.args[4]
            self.assertEqual(env["HELAC_RETAIN_DIAGNOSTICS"],
                             "1" if value is True else "0")
            self.assertEqual(env["SKIP_STAGEOUT"], "1")
            command = run.call_args.args[2]
            self.assertEqual(command[command.index("--helac-diagnostics") + 1],
                             "slim" if value is True else "none")

    def test_real_logging_preserves_streams_timing_and_exit_on_success_and_failure(self):
        for exit_code in (0, 7):
            with self.subTest(exit_code=exit_code):
                root = self.root / str(exit_code)
                root.mkdir()
                outcome = run_native_command(root, True, exit_code)
                self.assertEqual(outcome.returncode, exit_code, outcome.stderr)
                contents = self.archive_contents(task(), root)
                prefix = "command_logs/001_helac_ho_cluster"
                self.assertEqual(contents[prefix + ".stdout"],
                                 "maximum weight warning\nstopping: integration ended\n")
                self.assertIn("native stderr warning\n", contents[prefix + ".stderr"])
                self.assertEqual(contents[prefix + ".stderr"],
                                 (root / (prefix + ".stderr")).read_text())
                timing = dict(line.split("=", 1)
                              for line in contents[prefix + ".timing.log"].splitlines())
                self.assertEqual(int(timing.pop("exit_code")), exit_code)
                self.assertEqual(set(timing), {"wall_seconds", "cpu_user_seconds",
                                               "cpu_system_seconds"})
                for value in timing.values():
                    self.assertGreaterEqual(float(value), 0)

    def test_opt_in_keeps_counted_lhe_output_and_physics_arguments(self):
        commands = []
        for retain in (False, True):
            with self.subTest(retain=retain):
                current_task = task(retain)

                def generate(task, runtime, command, work, env):
                    commands.append(command)
                    output = (Path(env["LOCAL_OUTPUT_BASE"]) / "lhe"
                              / "sample_pool_2jpsi_g_93000.lhe.gz")
                    output.parent.mkdir(parents=True, exist_ok=True)
                    with gzip.open(output, "wt") as stream:
                        stream.write("<LesHouchesEvents>\n<event>\n1\n</event>\n"
                                     "<event>\n2\n</event>\n</LesHouchesEvents>\n")

                with patch.object(lhe.shutil, "copy2"), \
                        patch.object(lhe, "run", side_effect=generate), \
                        patch.object(lhe, "publish", return_value={"events": 2}) as publish:
                    outcome = lhe.execute(current_task, self.root)
                self.assertEqual(outcome["status"], "ok")
                self.assertEqual(outcome["outputs"], [{"events": 2}])
                self.assertEqual(publish.call_args.kwargs["events"], 2)
                self.assertEqual(publish.call_args.args[0], self.root /
                                 "products/lhe/sample_pool_2jpsi_g_93000.lhe.gz")
        index = commands[0].index("--helac-diagnostics") + 1
        self.assertEqual(commands[0][index], "none")
        self.assertEqual(commands[1][index], "slim")
        commands[1][index] = "none"
        self.assertEqual(commands[0], commands[1])

    def test_default_logging_does_not_add_timing_or_change_failure(self):
        for exit_code in (0, 7):
            with self.subTest(exit_code=exit_code):
                outcome = run_native_command(self.root, False, exit_code)
                self.assertEqual(outcome.returncode, exit_code)
                self.assertFalse(list(self.root.rglob("*.timing.log")))
                self.assertEqual(self.archive_contents(task(False)), {})

    def test_retained_tree_is_lossless_and_parseable_without_payloads(self):
        evidence, excluded = self.evidence_tree(self.root)
        contents = self.archive_contents(task())
        self.assertEqual(contents, evidence)
        self.assertFalse(set(excluded) & contents.keys())
        extracted = self.root / "extracted"
        for name, text in contents.items():
            put(extracted, name, text)
        summary = parse_root(extracted)
        self.assertEqual(summary["run_count"], 2)
        self.assertEqual(summary["result_count"], 2)
        self.assertEqual(summary["parameters"], {"nmc": "100", "unwevt": "10"})
        result = summary["runs"][0]["calculations"][0]["result_files"][0]
        self.assertEqual(result["final_sigma_nb"], 0.11)
        self.assertIsNone(summary["observed_lhe_events"])

    def test_retention_is_lhe_only_and_disabled_by_default(self):
        self.evidence_tree(self.root)
        for value, stage in ((False, "lhe"), ("true", "lhe"), (1, "lhe"),
                             (True, "mix"), (None, "lhe")):
            current_task = task(value, stage)
            if value is None:
                current_task["parameters"].pop("retain_helac_diagnostics")
            with self.subTest(value=value, stage=stage):
                self.assertEqual(self.archive_contents(current_task), {
                    "commands.log": "wrapper evidence\n",
                    "helac_run.log": "stopping: integration ended\n"})

    def test_does_not_follow_card_or_directory_symlinks(self):
        external = self.root / "private"
        put(external, "secret.inp", "secret")
        stage = self.root / "stage"
        inputs = stage / "HELAC-Onia-2.7.6/input"
        inputs.mkdir(parents=True)
        (inputs / "user.inp").symlink_to(external / "secret.inp")
        proc = stage / "HELAC-Onia-2.7.6/PROC_HO_1"
        proc.mkdir()
        (proc / "input").symlink_to(external, target_is_directory=True)
        self.assertEqual(self.archive_contents(task(), stage), {})

    def test_bootstrap_returns_diagnostics_on_child_failure_and_launch_exception(self):
        for raises in (False, True):
            with self.subTest(raises=raises):
                root = self.root / str(raises)
                root.mkdir()
                task_path = put(root, "task.json", json.dumps(task()))
                work = root / "work"
                result = root / "result.json"
                diagnostics = root / "diagnostics.tar.gz"

                def child(*args, **kwargs):
                    self.evidence_tree(work / "stage")
                    put(work / "stage",
                        "HELAC-Onia-2.7.6/PROC_HO_7/P0_calc_0/output/RESULT_test.out",
                        "partial result before failure\nmaximum weight warning\n")
                    if raises:
                        raise OSError("launch failed")
                    return subprocess.CompletedProcess(args[0], 7)

                argv = ["--task", str(task_path), "--work-dir", str(work),
                        "--result", str(result), "--diagnostics", str(diagnostics)]
                with patch.object(bootstrap, "unpack_runtime", return_value=CORE), \
                        patch.object(bootstrap.subprocess, "run", side_effect=child):
                    if raises:
                        with self.assertRaisesRegex(OSError, "launch failed"):
                            bootstrap.main(argv)
                    else:
                        self.assertEqual(bootstrap.main(argv), 7)
                self.assertEqual(json.loads(result.read_text())["status"], "failed")
                with tarfile.open(diagnostics) as archive:
                    self.assertIn("HELAC-Onia-2.7.6/input/user.inp", archive.getnames())
                    self.assertIn("HELAC-Onia-2.7.6/PROC_HO_7/P0_calc_0/output/RESULT_test.out",
                                  archive.getnames())
                    self.assertEqual(archive.extractfile(
                        "HELAC-Onia-2.7.6/PROC_HO_7/P0_calc_0/output/RESULT_test.out").read(),
                        b"partial result before failure\nmaximum weight warning\n")


if __name__ == "__main__":
    unittest.main()
