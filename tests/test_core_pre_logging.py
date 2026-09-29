"""The generated PRE launcher must leave a per-step log of every attempt."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.planning.graph import (PRE_PROXY_ABORT_EXIT_CODE,
                                    PRE_PROXY_MIN_TIMELEFT_SECONDS,
                                    PRE_SKIP_EXIT_CODE, write_root_dag)


class PreLauncherLogging(unittest.TestCase):
    def fixture(self, root, body="exit 0\n", *, timeleft="7200", proxy_mode="ok"):
        """Write a stub CLI and the root DAG the way plan_run does."""
        core = root / "core"
        core.mkdir()
        cli = core / "mcprod"
        cli.write_text("#!/bin/bash\n" + body, encoding="utf-8")
        cli.chmod(0o755)
        proxy = root / "proxy"
        proxy.write_text("test-proxy\n", encoding="utf-8")
        write_root_dag(root, {"core_root": str(core), "runtime_release": "test-release"},
                       3, str(proxy))
        if proxy_mode == "unavailable":
            path_root = root / "empty-bin"
            path_root.mkdir()
        else:
            path_root = root / "bin"
            path_root.mkdir()
            shim = path_root / "voms-proxy-info"
            if proxy_mode == "fail":
                shim.write_text("#!/bin/bash\necho 'proxy unreadable' >&2\nexit 1\n",
                                encoding="utf-8")
            else:
                shim.write_text(
                    "#!/bin/bash\n"
                    "if [[ \"$1\" != \"--file\" || \"$3\" != \"--timeleft\" ]]; then\n"
                    "  echo 'unexpected voms-proxy-info arguments' >&2\n"
                    "  exit 2\n"
                    "fi\n"
                    "printf '%s\\n' '" + str(timeleft) + "'\n",
                    encoding="utf-8",
                )
            shim.chmod(0o755)
        script = root / "advance.sh"
        generated = script.read_text(encoding="utf-8")
        path_value = (str(path_root) if proxy_mode == "unavailable"
                      else str(path_root) + ":/usr/bin:/bin")
        generated = generated.replace(
            "export PATH=/usr/bin:/bin",
            "export PATH=" + path_value,
        )
        script.write_text(generated, encoding="utf-8")
        return script

    def run_script(self, script, step):
        return subprocess.run([str(script), str(step)], check=False,
                              capture_output=True, text=True)

    def test_failure_exception_lands_in_the_step_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "advance-wave exploded" >&2\nexit 3\n')

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, 3)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("advance-wave exploded", text)

    def test_success_is_preserved_and_the_header_is_written(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran $*"\n')

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, 0)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("advance-wave step=0 runtime=test-release", text)
            self.assertRegex(text, r"\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
            self.assertIn("cli-ran internal advance-wave", text)

    def test_complete_sentinel_skips_before_launching_the_cli(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n', proxy_mode="unavailable")
            (root / "proxy").unlink()
            (root / ".complete").touch()

            outcome = self.run_script(script, 2)

            self.assertEqual(outcome.returncode, PRE_SKIP_EXIT_CODE)
            text = (root / "logs/advance_000002.log").read_text(encoding="utf-8")
            self.assertIn("advance-wave step=2 runtime=test-release", text)
            self.assertNotIn("cli-ran", text)

    def test_missing_proxy_aborts_an_incomplete_wave(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n')
            (root / "proxy").unlink()

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, PRE_PROXY_ABORT_EXIT_CODE)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("proxy-validity", text)
            self.assertIn("pinned proxy is unavailable", text)
            self.assertNotIn("cli-ran", text)

    def test_voms_proxy_info_unavailable_aborts_with_diagnostic(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n', proxy_mode="unavailable")

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, PRE_PROXY_ABORT_EXIT_CODE)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("voms-proxy-info is unavailable", text)

    def test_voms_proxy_info_failure_aborts_with_diagnostic(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n', proxy_mode="fail")

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, PRE_PROXY_ABORT_EXIT_CODE)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("unable to read pinned proxy validity", text)

    def test_timeleft_below_threshold_aborts_and_explains_refresh(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n', timeleft="3599")

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, PRE_PROXY_ABORT_EXIT_CODE)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("3599s remaining", text)
            self.assertIn(f"{PRE_PROXY_MIN_TIMELEFT_SECONDS}s is required", text)
            self.assertIn(str(root / "proxy"), text)
            self.assertIn("Refresh this proxy in place", text)

    def test_timeleft_threshold_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n', timeleft="3600")

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, 0)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("proxy-validity: 3600s remaining", text)
            self.assertIn("cli-ran", text)

    def test_timeleft_above_threshold_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n', timeleft="7201")

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, 0)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("proxy-validity: 7201s remaining", text)
            self.assertIn("cli-ran", text)

    def test_malformed_timeleft_aborts_with_diagnostic(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n', timeleft="not-a-number")

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, PRE_PROXY_ABORT_EXIT_CODE)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("invalid voms-proxy-info --timeleft output", text)

    def test_every_root_wave_has_skip_and_proxy_abort_rules(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            lines = (root / "run.dag").read_text(encoding="utf-8").splitlines()
            waves = [line.split()[2] for line in lines if line.startswith("SUBDAG EXTERNAL")]
            self.assertEqual(len(waves), 3)
            for wave in waves:
                self.assertIn(f"PRE_SKIP {wave} {PRE_SKIP_EXIT_CODE}", lines)
                self.assertIn(
                    f"ABORT-DAG-ON {wave} {PRE_PROXY_ABORT_EXIT_CODE} RETURN 1",
                    lines,
                )

    def test_timeout_returns_124_and_still_writes_the_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("full_mc.planning.graph.PRE_SCRIPT_TIMEOUT_SECONDS", 0.5):
                script = self.fixture(root, "sleep 5\n")

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, 124)
            self.assertTrue((root / "logs/advance_000000.log").is_file())

    def test_the_log_name_carries_the_step_and_creates_its_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, "exit 0\n")
            self.assertFalse((root / "logs").exists())

            self.run_script(script, 1)

            self.assertTrue((root / "logs/advance_000001.log").is_file())
            self.assertFalse((root / "logs/advance_000000.log").exists())


if __name__ == "__main__":
    unittest.main()
