import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.credentials import _persistent_afs_path, resolve_proxy_path


class CredentialResolutionTests(unittest.TestCase):
    def test_persistent_afs_requirement_rejects_tmp_and_eos(self):
        self.assertTrue(_persistent_afs_path(Path("/afs/cern.ch/user/test/proxy")))
        self.assertFalse(_persistent_afs_path(Path("/tmp/x509up_u123")))
        self.assertFalse(_persistent_afs_path(Path("/eos/user/test/proxy")))

    def test_environment_proxy_has_precedence_over_voms_proxy(self):
        with patch("full_mc.credentials._validate_proxy_path",
                   side_effect=[(Path("/afs/cern.ch/user/test/env"), None)]) as validate:
            resolved = resolve_proxy_path(
                environ={"X509_USER_PROXY": "/afs/cern.ch/user/test/env"},
                runner=lambda *args, **kwargs: self.fail("voms fallback must not run"))
        self.assertEqual(resolved, "/afs/cern.ch/user/test/env")
        validate.assert_called_once_with("/afs/cern.ch/user/test/env")

    def test_uses_voms_after_unusable_environment_proxy(self):
        voms = SimpleNamespace(returncode=0, stdout="/afs/cern.ch/user/test/voms\n", stderr="")
        with patch("full_mc.credentials._validate_proxy_path",
                   side_effect=[(None, "not a persistent AFS path"),
                                (Path("/afs/cern.ch/user/test/voms"), None)]) as validate:
            resolved = resolve_proxy_path(
                environ={"X509_USER_PROXY": "/tmp/x509up_u123"},
                runner=lambda *args, **kwargs: voms)
        self.assertEqual(resolved, "/afs/cern.ch/user/test/voms")
        self.assertEqual([call.args[0] for call in validate.call_args_list],
                         ["/tmp/x509up_u123", "/afs/cern.ch/user/test/voms"])

    def test_rejects_unusable_tmp_and_eos_candidates(self):
        failed = SimpleNamespace(returncode=1, stdout="", stderr="no proxy")
        for path in ("/tmp/x509up_u123", "/eos/user/test/proxy"):
            with self.subTest(path=path), self.assertRaisesRegex(RuntimeError, "persistent AFS"):
                resolve_proxy_path(environ={"X509_USER_PROXY": path},
                                   runner=lambda *args, **kwargs: failed)


if __name__ == "__main__":
    unittest.main()
