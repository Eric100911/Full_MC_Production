"""Reclaim uses the shared runtime-profile subprocess contract."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.storage.reclaim import remove_many


class ReclaimRuntimeTests(unittest.TestCase):
    def test_remote_reclaim_calls_profile_without_subprocess_only_options(self):
        url = "root://eosuser.cern.ch///eos/user/chiw/staging/run/block.lhe.gz"
        completed = Mock(returncode=0, stdout="0\tremoved\n", stderr="")
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            with patch("full_mc.storage.reclaim.run_profile",
                       return_value=completed) as run_profile:
                result = remove_many([url], work_dir=work)

        self.assertEqual(result, {url: "removed"})
        self.assertEqual(run_profile.call_args.args[0], "utilities-el9")
        self.assertEqual(run_profile.call_args.kwargs["cwd"], work)
        self.assertFalse(run_profile.call_args.kwargs["check"])
        self.assertNotIn("text", run_profile.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
