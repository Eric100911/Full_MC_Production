"""Reclaim worker and batched staged-deletion contracts."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.storage import reclaim
from full_mc.storage.transfer import _remote_parts
from full_mc.workers.reclaim import execute as execute_reclaim


STAGING = "root://eosuser.cern.ch///eos/user/c/chiw/staging/run-1"

XRDFS_STUB = """#!/bin/bash
lfn="$3"
if grep -qxF "$lfn" "$RECLAIM_MISSING"; then
    echo "[ERROR] Server responded with an error: [3011] Unable to open file $lfn; No such file or directory" >&2
    exit 1
fi
if grep -qxF "$lfn" "$RECLAIM_FATAL"; then
    echo "[ERROR] Server responded with an error: [3010] Permission denied" >&2
    exit 1
fi
exit 0
"""


def lfn(url):
    return _remote_parts(url)[1]


def script_text(command):
    return command[2] if command[1] == "-c" else Path(command[1]).read_text(encoding="utf-8")


def task(inputs):
    return {"task_id": "RECLAIM_000001", "stage": "reclaim",
            "runtime": "utilities-el9", "runtime_release": "release-1",
            "inputs": inputs, "outputs": [],
            "parameters": {"staging_prefix": STAGING}}


def reference(identity, name, *, sidecar=False):
    ref = {"artifact_id": identity, "kind": "lhe-block",
           "url": STAGING + "/" + name}
    if sidecar:
        ref["sidecar_url"] = ref["url"] + ".json"
    return ref


class ReclaimTestCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.work = self.root / "work"
        binary = self.root / "bin" / "xrdfs"
        binary.parent.mkdir()
        binary.write_text(XRDFS_STUB, encoding="utf-8")
        binary.chmod(0o755)
        self.binary = binary.parent
        self.missing = self.root / "missing"
        self.fatal = self.root / "fatal"

    def tearDown(self):
        self.temporary.cleanup()

    def runner(self, command, *, missing=(), fatal=(), **kwargs):
        """Execute the batched script against a stub xrdfs, never a container."""
        self.missing.write_text("\n".join(missing) + ("\n" if missing else ""), encoding="utf-8")
        self.fatal.write_text("\n".join(fatal) + ("\n" if fatal else ""), encoding="utf-8")
        environment = dict(os.environ,
                           PATH=os.pathsep.join([str(self.binary), os.environ.get("PATH", "")]),
                           RECLAIM_MISSING=str(self.missing), RECLAIM_FATAL=str(self.fatal))
        return subprocess.run(command, env=environment, **kwargs)

    def execute(self, refs, *, missing=(), fatal=()):
        """Run the worker and count how often it launches a utilities container."""
        launches = []

        def fake_run_profile(profile, command, **kwargs):
            self.assertEqual(profile, "utilities-el9")
            launches.append(command)
            return self.runner(command, missing=missing, fatal=fatal, **kwargs)

        with patch.object(reclaim, "run_profile", side_effect=fake_run_profile):
            outcome = execute_reclaim(task(refs), self.work)
        return outcome, launches


class ReclaimWorkerContracts(ReclaimTestCase):
    def test_path_outside_run_staging_is_refused_without_deleting(self):
        outside = {"artifact_id": "A", "kind": "lhe-block",
                   "url": "root://eosuser.cern.ch///eos/user/c/chiw/other/block.lhe.gz"}
        with patch.object(reclaim, "run_profile") as run:
            with self.assertRaisesRegex(ValueError, "refusing to reclaim outside run staging"):
                execute_reclaim(task([outside]), self.work)
        run.assert_not_called()

    def test_missing_payload_and_sidecar_are_idempotent_success(self):
        ref = reference("B1", "block_000000.lhe.gz", sidecar=True)
        outcome, launches = self.execute(
            [ref], missing=[lfn(ref["url"]), lfn(ref["sidecar_url"])])
        self.assertEqual(len(launches), 1)
        self.assertEqual(outcome["status"], "ok")
        entry = outcome["reclaimed"][0]
        self.assertEqual(entry["artifact_id"], "B1")
        self.assertEqual([item["kind"] for item in entry["objects"]], ["payload", "sidecar"])
        self.assertEqual([item["disposition"] for item in entry["objects"]],
                         ["already-missing", "already-missing"])

    def test_absent_sidecar_declaration_never_manufactures_a_url(self):
        ref = reference("C1", "block_000001.lhe.gz")
        outcome, launches = self.execute([ref])
        self.assertEqual(outcome["reclaimed"][0]["objects"],
                         [{"kind": "payload", "url": ref["url"],
                           "disposition": "removed"}])
        script = script_text(launches[0])
        self.assertNotIn(".json", script)
        self.assertEqual(script.count("remove "), 1)

    def test_one_container_deletes_many_objects(self):
        refs = [reference("D%02d" % index, "block_%06d.lhe.gz" % index) for index in range(5)]
        outcome, launches = self.execute(refs)
        self.assertEqual(len(launches), 1)
        self.assertEqual(script_text(launches[0]).count("remove "), 5)
        dispositions = [item["disposition"] for entry in outcome["reclaimed"]
                        for item in entry["objects"]]
        self.assertEqual(dispositions, ["removed"] * 5)

    def test_fatal_remote_error_fails_the_task(self):
        ref = reference("E1", "block_000002.lhe.gz")
        with self.assertRaisesRegex(RuntimeError, "reclaim command failed"):
            self.execute([ref], fatal=[lfn(ref["url"])])


class BatchedDeletionTests(ReclaimTestCase):
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
