"""Reclaim worker and batched staged-deletion contracts."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.storage import reclaim
from full_mc.storage.transfer import TransferClient


STAGING = "root://eosuser.cern.ch///eos/user/c/chiw/staging/run-1"

class ReclaimStorage(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.work = self.root / "work"

    def test_injected_runner_is_called_once_for_many_objects(self):
        runner = Mock(return_value=Mock(returncode=0, stdout="0\tremoved\n1\tremoved\n",
                                        stderr=""))
        first = STAGING + "/a.lhe.gz"
        second = STAGING + "/b.lhe.gz"
        dispositions = reclaim.remove_many([first, second], runner=runner, work_dir=self.work)
        self.assertEqual(runner.call_count, 1)
        self.assertEqual(runner.call_args.args[0][:2], ["bash", "-c"])
        self.assertEqual(dispositions, {first: "removed", second: "removed"})

    def test_already_missing_lines_are_not_failures(self):
        runner = Mock(return_value=Mock(returncode=0, stdout="0\talready-missing\n", stderr=""))
        url = STAGING + "/missing.lhe.gz"
        self.assertEqual(reclaim.remove_many([url], runner=runner, work_dir=self.work),
                         {url: "already-missing"})

    def test_nonzero_exit_without_reported_dispositions_fails(self):
        runner = Mock(return_value=Mock(returncode=1, stdout="", stderr="[3010] denied"))
        with self.assertRaisesRegex(RuntimeError, "reclaim command failed"):
            reclaim.remove_many([STAGING + "/x.lhe.gz"], runner=runner, work_dir=self.work)

    def test_only_not_found_diagnostics_are_treated_as_missing(self):
        url = STAGING + "/3011.lhe.gz"
        missing = "[ERROR] Server responded with an error: [3011] No such file or directory"
        denied = "[ERROR] Server responded with an error: [3010] Permission denied for /store/3011"
        for diagnostic, exception in ((missing, FileNotFoundError), (denied, RuntimeError)):
            with self.subTest(diagnostic=diagnostic):
                runner = Mock(return_value=Mock(returncode=1, stderr=diagnostic))
                with self.assertRaises(exception):
                    TransferClient(runner=runner).remove(url)
                script = 'xrdfs() { printf "%s\\n" "$TEST_DIAGNOSTIC" >&2; return 1; }\n'
                script += reclaim._build_script([(0, "root://example/", "/store/3011")])
                completed = subprocess.run(
                    ["/bin/bash", "-c", script], text=True, capture_output=True,
                    env={"TEST_DIAGNOSTIC": diagnostic, "PATH": "/usr/bin:/bin"}, check=False)
                if exception is FileNotFoundError:
                    self.assertEqual((completed.returncode, completed.stdout),
                                     (0, "0\talready-missing\n"))
                else:
                    self.assertEqual(completed.returncode, 1)
                    self.assertIn(diagnostic, completed.stderr)

    def test_broad_removal_is_refused(self):
        with self.assertRaisesRegex(ValueError, "refusing broad removal"):
            reclaim.remove_many(["root://eosuser.cern.ch//store"], runner=Mock(),
                                work_dir=self.work)

    def test_local_targets_are_unlinked_without_a_container(self):
        present = self.root / "staged.lhe.gz"
        present.write_bytes(b"payload")
        absent = self.root / "gone.lhe.gz"
        with patch.object(reclaim, "run_profile") as run:
            dispositions = reclaim.remove_many([str(present), str(absent), str(present)],
                                               work_dir=self.work)
        run.assert_not_called()
        self.assertEqual(list(dispositions), [str(present), str(absent)])
        self.assertEqual(dispositions[str(present)], "removed")
        self.assertEqual(dispositions[str(absent)], "already-missing")
        self.assertFalse(present.exists())


if __name__ == "__main__":
    unittest.main()
