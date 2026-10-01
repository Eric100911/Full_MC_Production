"""The generated PRE launcher must leave a per-step log of every attempt."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.planning.graph import PRE_SKIP_EXIT_CODE, write_root_dag


class PreLauncherLogging(unittest.TestCase):
    def fixture(self, root, body="exit 0\n"):
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
        return root / "advance.sh"

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
            script = self.fixture(root, 'echo "cli-ran"\n')
            (root / "proxy").unlink()
            (root / ".complete").touch()

            outcome = self.run_script(script, 2)

            self.assertEqual(outcome.returncode, PRE_SKIP_EXIT_CODE)
            text = (root / "logs/advance_000002.log").read_text(encoding="utf-8")
            self.assertIn("advance-wave step=2 runtime=test-release", text)
            self.assertNotIn("cli-ran", text)

    def test_missing_proxy_does_not_stop_an_incomplete_wave(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n')
            (root / "proxy").unlink()

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, 0)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("cli-ran", text)

    def test_empty_proxy_does_not_stop_an_incomplete_wave(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n')
            (root / "proxy").write_bytes(b"")

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, 0)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertIn("cli-ran", text)

    def test_pre_uses_no_proxy_validity_tool(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = self.fixture(root, 'echo "cli-ran"\n')

            outcome = self.run_script(script, 0)

            self.assertEqual(outcome.returncode, 0)
            text = (root / "logs/advance_000000.log").read_text(encoding="utf-8")
            self.assertNotIn("proxy-validity:", text)
            self.assertIn("cli-ran", text)
            generated = script.read_text(encoding="utf-8")
            self.assertNotIn("voms-proxy-info", generated)
            self.assertNotIn("proxy_timeleft", generated)

    def test_every_root_wave_has_skip_without_proxy_abort(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            lines = (root / "run.dag").read_text(encoding="utf-8").splitlines()
            waves = [line.split()[2] for line in lines if line.startswith("SUBDAG EXTERNAL")]
            self.assertEqual(len(waves), 3)
            for wave in waves:
                self.assertIn(f"PRE_SKIP {wave} {PRE_SKIP_EXIT_CODE}", lines)
                self.assertFalse(any(line.startswith(f"ABORT-DAG-ON {wave} ")
                                     for line in lines))

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
