"""Synthetic-only tests for immutable controller builds and launcher switching."""
from __future__ import annotations

import json
import fcntl
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from full_mc.runtime import controllers, packages
from full_mc.application import shards, staging
from full_mc.io import atomic_json, read_json


SCRATCH = Path("/tmp/chiw/v06_01_implementation/controllers")


class ControllerReleaseTests(unittest.TestCase):
    def setUp(self):
        SCRATCH.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="case-", dir=SCRATCH)
        self.root = Path(self.temp.name)
        self.core = self.root / "source-core"
        self.core.mkdir()
        self._make_source_core()
        self.build_dir = self.root / "releases" / "ctrl-test-1"
        self.build = controllers.prepare_controller(self.core, self.build_dir, "ctrl-test-1")
        self.run = self.root / "run"
        self.run.mkdir()
        (self.run / "logs").mkdir()
        (self.run / "run.dag").write_text("# synthetic frozen DAG\n")
        self.run_metadata = {
            "orchestration": "campaign_shards",
            "runtime_release": "worker-release-frozen-7",
            "core_revision": self._git("rev-parse", "HEAD"),
            "core_root": str(self.core),
            "run_id": "synthetic-run",
        }
        (self.run / "run.json").write_text(json.dumps(self.run_metadata, sort_keys=True))
        (self.run / "state.json").write_text('{"sentinel": "unchanged"}\n')
        (self.run / "tasks").mkdir()
        (self.run / "tasks" / "task.json").write_text('{"task": "frozen"}\n')
        launcher = controllers._original_launcher(self.run.resolve(), self.run_metadata)
        (self.run / "controller.sh").write_bytes(launcher)
        (self.run / "controller.sh").chmod(0o751)
        self.original_bytes = launcher

    def tearDown(self):
        self.temp.cleanup()

    def _git(self, *args):
        env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
                   GIT_AUTHOR_NAME="Controller Test", GIT_AUTHOR_EMAIL="test@example.invalid",
                   GIT_COMMITTER_NAME="Controller Test",
                   GIT_COMMITTER_EMAIL="test@example.invalid")
        return subprocess.check_output(["git", "-C", str(self.core), *args],
                                       text=True, env=env, stderr=subprocess.PIPE).strip()

    def _make_source_core(self):
        for entry in packages.CORE_PAYLOAD:
            path = self.core / entry
            if entry == "mcprod":
                path.write_text(
                    "#!/usr/bin/env python3\n"
                    "import json, os, sys\n"
                    "print(json.dumps({'build': os.environ.get('MCPROD_CONTROLLER_BUILD'), "
                    "'events': os.environ.get('MCPROD_CONTROL_EVENTS'), "
                    "'hints': os.environ.get('MCPROD_ADMISSION_HINTS'), "
                    "'index': os.environ.get('MCPROD_PRODUCER_INDEX'), "
                    "'plan_post': os.environ.get('MCPROD_PLAN_ACCEPT_POST_SECONDS'), "
                    "'plan_run_dir': os.environ.get('MCPROD_PLAN_ACCEPT_POST_RUN_DIR'), "
                    "'argv': sys.argv[1:]}))\n"
                )
                path.chmod(0o755)
            elif path.suffix:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# synthetic controller payload\n")
            else:
                (path / "stub.py").parent.mkdir(parents=True, exist_ok=True)
                (path / "stub.py").write_text("# synthetic controller payload\n")
        (self.core / "src/full_mc/cli.py").write_text("# synthetic CLI source\n")
        (self.core / "src/full_mc/application").mkdir(parents=True)
        for module in ("admission_hints", "producer_index", "control_events"):
            (self.core / "src/full_mc/application" / f"{module}.py").write_text(
                "# synthetic optional controller module\n"
            )
        self._git("init", "-q")
        self._git("add", "-A")
        self._git("-c", "user.name=Controller Test", "-c",
                  "user.email=test@example.invalid", "commit", "-qm", "synthetic source")

    def _activate(self, events=False, admission_hints=False, producer_index=False,
                  plan_accept_post_seconds=None, maintenance_pause=False):
        return controllers.activate_controller(self.run, self.build_dir,
                                               control_events=events,
                                               admission_hints=admission_hints,
                                               producer_index=producer_index,
                                               plan_accept_post_seconds=plan_accept_post_seconds,
                                               maintenance_pause=maintenance_pause)

    def test_prepare_allowlist_and_immutable_destination(self):
        (self.core / "generated").mkdir()
        (self.core / "generated" / "secret.txt").write_text("exclude")
        (self.core / "credentials").mkdir()
        (self.core / "credentials" / "proxy").write_text("exclude")
        # The payload snapshot is already built; a new build records source dirtiness,
        # while the archived payload remains limited to CORE_PAYLOAD.
        second = self.root / "releases" / "ctrl-test-2"
        info = controllers.prepare_controller(self.core, second, "ctrl-test-2")
        self.assertTrue(info["source_dirty"])
        self.assertEqual(info["payload"], list(packages.CORE_PAYLOAD))
        for module in ("admission_hints", "producer_index", "control_events"):
            self.assertTrue((second / "controller/core/src/full_mc/application"
                             / f"{module}.py").is_file())
        self.assertFalse((second / "controller/core/generated").exists())
        self.assertFalse((second / "controller/core/credentials").exists())
        with self.assertRaises(FileExistsError):
            controllers.prepare_controller(self.core, second, "ctrl-test-2")

    def test_prepare_persistent_parent_fsync_failure_reports_visible_build(self):
        destination = self.root / "releases" / "ctrl-visible"
        original_fsync_dir = controllers._fsync_dir

        def fail_parent(path):
            if Path(path) == destination.parent:
                raise OSError("persistent parent fsync failure")
            return original_fsync_dir(Path(path))

        with mock.patch.object(controllers, "_fsync_dir", side_effect=fail_parent):
            with self.assertRaisesRegex(OSError, "build is visible.*retry the operation"):
                controllers.prepare_controller(self.core, destination, "ctrl-visible")
        self.assertEqual(controllers._read_build(destination)["build_id"], "ctrl-visible")

    def test_bundle_digest_is_verified_by_status_reader(self):
        bundle = self.build_dir / "controller-core.tar.gz"
        record = json.loads((self.build_dir / "controller.json").read_text())
        self.assertEqual(record["payload_sha256"], controllers._sha256_file(bundle))
        with bundle.open("ab") as handle:
            handle.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "payload checksum does not match"):
            controllers._read_build(self.build_dir)

    def test_activate_is_idempotent_and_preserves_frozen_run_identity(self):
        run_json = (self.run / "run.json").read_bytes()
        root_dag = (self.run / "run.dag").read_bytes()
        state = (self.run / "state.json").read_bytes()
        task = (self.run / "tasks/task.json").read_bytes()
        first = self._activate(events=True)
        launcher = (self.run / "controller.sh").read_bytes()
        second = self._activate(events=True)
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(launcher, (self.run / "controller.sh").read_bytes())
        self.assertEqual((self.run / "run.json").read_bytes(), run_json)
        self.assertEqual((self.run / "run.dag").read_bytes(), root_dag)
        self.assertEqual((self.run / "state.json").read_bytes(), state)
        self.assertEqual((self.run / "tasks/task.json").read_bytes(), task)
        self.assertEqual(first["runtime_release"], "worker-release-frozen-7")
        self.assertTrue(first["control_events"])

    def test_rollback_restores_exact_original_bytes_and_mode(self):
        self._activate()
        result = controllers.rollback_controller(self.run, expect_build="ctrl-test-1")
        self.assertEqual((self.run / "controller.sh").read_bytes(), self.original_bytes)
        self.assertEqual((self.run / "controller.sh").stat().st_mode & 0o777, 0o751)
        self.assertFalse(result["managed"])
        self.assertEqual((self.run / "run.json").read_bytes(),
                         json.dumps(self.run_metadata, sort_keys=True).encode())

    def test_unknown_launcher_is_not_overwritten(self):
        path = self.run / "controller.sh"
        path.write_bytes(self.original_bytes + b"# local hand edit\n")
        with self.assertRaisesRegex(ValueError, "unrecognized controller launcher"):
            self._activate()
        with self.assertRaisesRegex(ValueError, "unrecognized controller launcher"):
            controllers.controller_status(self.run)

    def test_rollback_refuses_edited_managed_launcher(self):
        self._activate()
        path = self.run / "controller.sh"
        path.write_bytes(path.read_bytes() + b"# unexpected edit\n")
        with self.assertRaisesRegex(ValueError, "edited after activation"):
            controllers.rollback_controller(self.run)

    def test_replace_failure_before_change_keeps_original(self):
        with mock.patch.object(controllers.os, "replace", side_effect=OSError("before")):
            with self.assertRaisesRegex(OSError, "before"):
                self._activate()
        self.assertEqual((self.run / "controller.sh").read_bytes(), self.original_bytes)

    def test_replace_failure_after_visibility_is_recovered(self):
        replace = os.replace

        def replace_then_raise(source, target):
            replace(source, target)
            raise OSError("after replace")

        with mock.patch.object(controllers.os, "replace", side_effect=replace_then_raise):
            result = self._activate()
        self.assertTrue(result["recovered_after_replace"])
        self.assertTrue(result["managed"])

    def test_persistent_parent_fsync_failure_raises_then_idempotent_retry_syncs(self):
        original_fsync_dir = controllers._fsync_dir
        root_calls = 0

        def fail_root_after_replace(path):
            nonlocal root_calls
            path = Path(path)
            if path == self.run:
                root_calls += 1
                # Backup creation syncs this directory once before replacement.
                if root_calls >= 2:
                    raise OSError("persistent parent fsync failure")
            return original_fsync_dir(path)

        with mock.patch.object(controllers, "_fsync_dir", side_effect=fail_root_after_replace):
            with self.assertRaisesRegex(OSError, "launcher replacement is visible.*fsync failed"):
                self._activate()
        self.assertTrue(controllers.controller_status(self.run)["managed"])
        with mock.patch.object(controllers, "_fsync_dir", wraps=original_fsync_dir) as sync_dir:
            result = self._activate()
        self.assertTrue(result["idempotent"])
        sync_dir.assert_called_once_with(self.run)

    def test_backup_existing_retry_resyncs_file_and_directory(self):
        backup = controllers._write_backup(self.run, self.original_bytes, 0o751)
        original_fsync_dir = controllers._fsync_dir
        with mock.patch.object(controllers.os, "fsync", wraps=os.fsync) as sync_file, \
                mock.patch.object(controllers, "_fsync_dir", wraps=original_fsync_dir) as sync_dir:
            self.assertEqual(controllers._write_backup(self.run, self.original_bytes, 0o751),
                             backup)
        self.assertGreaterEqual(sync_file.call_count, 2)  # backup file and directory
        sync_dir.assert_any_call(backup.parent)

    def test_active_wrapper_executes_fresh_controller_with_original_arguments(self):
        self._activate(events=True, admission_hints=True, producer_index=True)
        subprocess.run([str(self.run / "controller.sh"), "prepare-source",
                        "--source-index", "17"], check=True)
        output = (self.run / "logs/controller.log").read_text().splitlines()[-1]
        observed = json.loads(output)
        self.assertEqual(observed["build"], "ctrl-test-1")
        self.assertEqual(observed["events"], "1")
        self.assertEqual(observed["hints"], "1")
        self.assertEqual(observed["index"], "1")
        self.assertEqual(observed["argv"], [
            "internal", "prepare-source", "--run-dir", str(self.run),
            "--source-index", "17",
        ])

    def test_plan_post_override_is_run_bound_visible_and_rollback_safe(self):
        run_json = (self.run / "run.json").read_bytes()
        state = (self.run / "state.json").read_bytes()
        task = (self.run / "tasks/task.json").read_bytes()
        result = self._activate(plan_accept_post_seconds=10)
        self.assertEqual(result["plan_accept_post_seconds"], 10)
        status = controllers.controller_status(self.run)
        self.assertEqual(status["plan_accept_post_seconds"], 10)
        launcher = (self.run / "controller.sh").read_text()
        self.assertIn("unset MCPROD_PLAN_ACCEPT_POST_SECONDS MCPROD_PLAN_ACCEPT_POST_RUN_DIR", launcher)
        self.assertIn("export MCPROD_PLAN_ACCEPT_POST_SECONDS=10", launcher)
        self.assertIn(f"export MCPROD_PLAN_ACCEPT_POST_RUN_DIR={self.run}", launcher)
        with mock.patch.dict(os.environ, {
                "MCPROD_PLAN_ACCEPT_POST_SECONDS": "999",
                "MCPROD_PLAN_ACCEPT_POST_RUN_DIR": "/another/run"}):
            subprocess.run([str(self.run / "controller.sh"), "prepare-source"], check=True)
        observed = json.loads((self.run / "logs/controller.log").read_text().splitlines()[-1])
        self.assertEqual(observed["plan_post"], "10")
        self.assertEqual(observed["plan_run_dir"], str(self.run))
        self.assertEqual((self.run / "run.json").read_bytes(), run_json)
        self.assertEqual((self.run / "state.json").read_bytes(), state)
        self.assertEqual((self.run / "tasks/task.json").read_bytes(), task)

        controllers.rollback_controller(self.run, expect_build="ctrl-test-1")
        self.assertEqual((self.run / "controller.sh").read_bytes(), self.original_bytes)

    def test_maintenance_pause_returns_75_before_python_and_can_resume(self):
        paused = self._activate(events=True, admission_hints=True, producer_index=True,
                                plan_accept_post_seconds=10, maintenance_pause=True)
        self.assertTrue(paused["maintenance_pause"])
        status = controllers.controller_status(self.run)
        self.assertTrue(status["maintenance_pause"])
        log_path = self.run / "logs/controller.log"
        before = log_path.read_bytes() if log_path.exists() else b""
        result = subprocess.run([str(self.run / "controller.sh"), "prepare-source",
                                 "--source-index", "9"])
        self.assertEqual(result.returncode, 75)
        log = log_path.read_text()
        self.assertIn("controller maintenance pause: phase=prepare-source", log)
        self.assertNotIn('"build": "ctrl-test-1"', log[len(before.decode()):])

        resumed = self._activate(events=True, admission_hints=True, producer_index=True,
                                 plan_accept_post_seconds=10)
        self.assertFalse(resumed["maintenance_pause"])
        self.assertFalse(controllers.controller_status(self.run)["maintenance_pause"])
        subprocess.run([str(self.run / "controller.sh"), "prepare-source",
                        "--source-index", "9"], check=True)
        observed = json.loads((self.run / "logs/controller.log").read_text().splitlines()[-1])
        self.assertEqual(observed["build"], "ctrl-test-1")
        self.assertEqual(observed["events"], "1")
        self.assertEqual(observed["hints"], "1")
        self.assertEqual(observed["index"], "1")
        self.assertEqual(observed["plan_post"], "10")

    def test_maintenance_pause_rollback_restores_previous_launcher(self):
        self._activate(events=True, admission_hints=True, producer_index=True,
                       plan_accept_post_seconds=10, maintenance_pause=True)
        controllers.rollback_controller(self.run, expect_build="ctrl-test-1")
        self.assertEqual((self.run / "controller.sh").read_bytes(), self.original_bytes)

    def test_legacy_managed_launcher_without_policy_remains_valid_and_rollbackable(self):
        backup = controllers._write_backup(self.run, self.original_bytes, 0o751)
        metadata = {
            "schema": "mcprod-controller-switch-v1",
            "run_dir": str(self.run.resolve()),
            "runtime_release": "worker-release-frozen-7",
            "original_core_root": str(self.core),
            "build_id": "ctrl-test-1",
            "build_dir": str(self.build_dir.resolve()),
            "control_events": False,
            "admission_hints": False,
            "producer_index": False,
            "previous_launcher": str(backup),
            "previous_sha256": controllers._sha256(self.original_bytes),
            "previous_mode": 0o751,
        }
        old_launcher = controllers._managed_launcher(self.run.resolve(), metadata)
        expected_old_launcher = (
            "#!/bin/bash\n"
            + controllers._metadata_comment(metadata) + "\n"
            + "set -euo pipefail\n"
            + "export PATH=/usr/bin:/bin\n"
            + "export MCPROD_CONTROLLER_BUILD=ctrl-test-1\n"
            + "unset MCPROD_CONTROL_EVENTS MCPROD_ADMISSION_HINTS MCPROD_PRODUCER_INDEX\n"
            + 'phase="${1:?missing controller phase}"\n'
            + "shift\n"
            + f"exec >>{self.run}/logs/controller.log 2>&1\n"
            + f'exec {self.build_dir}/controller/core/mcprod internal "$phase" --run-dir '
            + f'{self.run} "$@"\n'
        ).encode()
        self.assertEqual(old_launcher, expected_old_launcher)
        (self.run / "controller.sh").write_bytes(old_launcher)
        (self.run / "controller.sh").chmod(0o751)

        status = controllers.controller_status(self.run)
        self.assertIsNone(status["plan_accept_post_seconds"])
        rollback = controllers.rollback_controller(self.run, expect_build="ctrl-test-1")
        self.assertFalse(rollback["managed"])
        self.assertEqual((self.run / "controller.sh").read_bytes(), self.original_bytes)

    def test_status_is_offline_and_reports_no_live_process_proof(self):
        status = controllers.controller_status(self.run)
        self.assertEqual(status["controller_build_dir"], str(self.core))
        self.assertEqual(status["runtime_release"], "worker-release-frozen-7")
        self.assertFalse(status["live_processes_verified"])

    def test_sync_rebuilds_only_index_and_reports_busy_lock(self):
        (self.run / "state.json").unlink()
        state = staging.initialize(self.run, 1000, {})
        task_id = "LHE_000001"
        outcome = {"task_id": task_id, "stage": "lhe", "outputs": [{
            "artifact_id": "artifact-1", "url": "root://host/file.lhe",
            "kind": "lhe", "size_bytes": 123, "events": 10,
        }]}
        (self.run / "ledger/accepted").mkdir(parents=True)
        atomic_json(shards._receipt_path(self.run, task_id), outcome)
        with staging.transaction(self.run) as current:
            current["accepted"][task_id] = {"stage": "lhe"}
        before = read_json(self.run / "state.json")
        result = controllers.sync_producer_index(self.run)
        after = read_json(self.run / "state.json")
        self.assertEqual(result["status"], "rebuilt")
        self.assertEqual(after["producer_index"]["artifacts"], {"artifact-1": task_id})
        self.assertEqual({key: value for key, value in before.items()
                          if key != "producer_index"},
                         {key: value for key, value in after.items()
                          if key != "producer_index"})
        with (self.run / "staging/ledger.lock").open("r+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            busy = controllers.sync_producer_index(self.run)
        self.assertEqual(busy["exit_code"], 75)


if __name__ == "__main__":
    unittest.main()
