"""Contracts for configurable DAGMan SCRIPT DEFER intervals."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.planning.script_defer import (MAX_SECONDS, controller_render_override,
                                           resolve_script_defer)
from full_mc.planning.shards import (_script, child_path, write_child_dag,
                                     write_root_dag)
from full_mc.application import shards
from full_mc.io import atomic_json, read_json
from full_mc.settings import _validate_scheduler_and_resources, load_run_spec


class ScriptDeferSettingsTests(unittest.TestCase):
    def test_absent_settings_keep_legacy_sixty_second_default(self):
        self.assertEqual(resolve_script_defer({}), {
            "default_seconds": 60,
            "plan_accept_post_seconds": 60,
        })
        self.assertEqual(resolve_script_defer({"script_defer": {
            "plan_accept_post_seconds": 15,
        }}), {
            "default_seconds": 60,
            "plan_accept_post_seconds": 15,
        })

    def test_strict_positive_integer_validation_at_settings_boundary(self):
        invalid = (0, -1, 1.5, 10.0, True, False, "10", None, [], {}, MAX_SECONDS + 1)
        for key in ("default_seconds", "plan_accept_post_seconds"):
            for value in invalid:
                with self.subTest(key=key, value=value):
                    spec = {"scheduler": {"script_defer": {key: value}}}
                    with self.assertRaisesRegex(ValueError, "supported range"):
                        _validate_scheduler_and_resources(spec)

    def test_invalid_scheduler_and_script_defer_shapes_are_rejected(self):
        for scheduler in (None, [], "scheduler"):
            with self.subTest(scheduler=scheduler):
                with self.assertRaisesRegex(ValueError, "scheduler must be an object"):
                    resolve_script_defer(scheduler)
        for value in (None, [], "60"):
            with self.subTest(script_defer=value):
                with self.assertRaisesRegex(ValueError, "script_defer must be an object"):
                    resolve_script_defer({"script_defer": value})

    def test_script_renderer_rejects_invalid_intervals_directly(self):
        self.assertIn(f"SCRIPT DEFER 75 {MAX_SECONDS} PRE", _script(
            "/run", "phase", "NODE", "", MAX_SECONDS))
        for interval in (0, -1, 10.5, True, "10", MAX_SECONDS + 1):
            with self.subTest(interval=interval):
                with self.assertRaisesRegex(ValueError, "supported range"):
                    _script("/run", "phase", "NODE", "", interval)

    def test_opt_in_survives_spec_loading_and_is_frozen_into_run_json(self):
        from tests.mcprod_fixtures import run_spec_fixture

        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            spec_path, run = run_spec_fixture(Path(temporary))
            spec = read_json(spec_path)
            spec["scheduler"]["script_defer"] = {
                "default_seconds": 60,
                "plan_accept_post_seconds": 15,
            }
            atomic_json(spec_path, spec)

            from full_mc.application.staging import FUSED_STATE_SCHEMA
            runtime_path = Path(spec["runtime_dir"]) / "runtime.json"
            runtime = read_json(runtime_path)
            runtime["cern_state_schema"] = FUSED_STATE_SCHEMA
            atomic_json(runtime_path, runtime)
            loaded = load_run_spec(spec_path)
            self.assertEqual(loaded["scheduler"]["script_defer"], spec["scheduler"]["script_defer"])
            with patch.object(shards, "resolve_proxy_path", return_value="/afs/test/proxy"):
                shards.plan_run(spec_path, run)
            frozen = read_json(run / "run.json")
            self.assertEqual(frozen["scheduler"]["script_defer"], spec["scheduler"]["script_defer"])


class ScriptDeferDagTests(unittest.TestCase):
    def _root_dag(self, root, scheduler):
        (root / "submit").mkdir()
        spec = {
            "core_root": "/runtime/controller/core",
            "scheduler": scheduler,
            "sources": [{"pool": "pool", "source_id": "source"}],
            "campaigns": [{"name": "C", "max_processing_nodes": 1,
                           "sources": [{"pool": "pool"}]}],
        }
        return write_root_dag(root, spec).read_text()

    def _child_dag(self, root, scheduler):
        tasks = [
            {"task_id": "PLAN_000000", "stage": "plan",
             "parameters": {"source_plan_task": True}},
            {"task_id": "PROCESS_C_000000", "stage": "processing", "parameters": {"scratch_bytes": 1000}},
            {"task_id": "LHE_000000", "stage": "lhe", "parameters": {}},
            {"task_id": "ARCHIVE_C_000000", "stage": "archive", "parameters": {}},
            {"task_id": "RECLAIM_C_000000", "stage": "reclaim", "parameters": {}},
        ]
        dependencies = [(["PLAN_000000"], "PROCESS_C_000000")]
        return write_child_dag(root, "CHILD", tasks, dependencies,
                               {"scheduler": scheduler}).read_text()

    def test_old_specs_keep_all_root_and_child_intervals_at_sixty(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root_text = self._root_dag(root, {})
            child_text = self._child_dag(root, {})

        for name in ("SOURCE_000000", "SHARD_C_000000", "CLEANUP_pool_000000",
                     "FINALIZE"):
            with self.subTest(root_node=name):
                self.assertRegex(root_text, rf"SCRIPT DEFER 75 60 PRE {name} ")
        self.assertRegex(root_text, r"SCRIPT DEFER 75 60 POST SOURCE_000000 ")
        self.assertRegex(root_text, r"SCRIPT DEFER 75 60 POST SHARD_C_000000 ")
        self.assertRegex(root_text, r"SCRIPT DEFER 75 60 POST CLEANUP_pool_000000 ")
        self.assertNotIn("PRE_SKIP ", root_text)
        self.assertIn("--pre-return-code $PRE_SCRIPT_RETURN", root_text)
        self.assertIn("--return-code $RETURN", root_text)
        self.assertIn("CATEGORY SHARD_C_000000 block_processing", root_text)
        self.assertIn("SCRIPT DEFER 75 60 POST PLAN_000000", child_text)
        for identity in ("PROCESS_C_000000", "LHE_000000",
                         "ARCHIVE_C_000000", "RECLAIM_C_000000"):
            with self.subTest(child_node=identity):
                self.assertIn(f"SCRIPT DEFER 75 60 POST {identity}", child_text)

    def test_only_plan_accept_post_uses_opt_in_interval(self):
        for interval in (10, 15):
            with self.subTest(interval=interval), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                settings = {"script_defer": {
                    "default_seconds": 60,
                    "plan_accept_post_seconds": interval,
                }}
                root_text = self._root_dag(root, settings)
                text = self._child_dag(root, settings)

            self.assertIn(f"SCRIPT DEFER 75 {interval} POST PLAN_000000", text)
            self.assertIn("SCRIPT DEFER 75 60 PRE SOURCE_000000", root_text)
            self.assertIn("SCRIPT DEFER 75 60 POST SHARD_C_000000", root_text)
            self.assertIn("SCRIPT DEFER 75 60 POST CLEANUP_pool_000000", root_text)
            self.assertNotIn("SCRIPT DEFER 75 10 POST PROCESS_C_000000", text)
            self.assertNotIn("SCRIPT DEFER 75 15 POST PROCESS_C_000000", text)
            for identity in ("PROCESS_C_000000", "LHE_000000",
                             "ARCHIVE_C_000000", "RECLAIM_C_000000"):
                self.assertIn(f"SCRIPT DEFER 75 60 POST {identity}", text)
            self.assertIn("RETRY PLAN_000000 1", text)
            self.assertIn("CATEGORY PLAN_000000 lhe_planning", text)
            self.assertIn("--return-code $RETURN", text)
            self.assertIn("PARENT PLAN_000000 CHILD PROCESS_C_000000", text)

    def test_custom_default_applies_to_other_scripts_and_plan_can_override(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            text = self._child_dag(root, {"script_defer": {
                "default_seconds": 45,
                "plan_accept_post_seconds": 12,
            }})
        self.assertIn("SCRIPT DEFER 75 12 POST PLAN_000000", text)
        self.assertIn("SCRIPT DEFER 75 45 POST PROCESS_C_000000", text)
        self.assertIn("SCRIPT DEFER 75 45 POST LHE_000000", text)

    def test_controller_override_is_run_bound_and_existing_child_is_preserved(self):
        name = "SOURCE_000000"
        plan = {"task_id": "PLAN_000000", "stage": "plan",
                "parameters": {"source_plan_task": True}}
        mix = {"task_id": "PROCESS_C_000000", "stage": "processing",
               "parameters": {"scratch_bytes": 1000}}
        manifest = {"tasks": [plan, mix],
                    "dependencies": [(["PLAN_000000"], "PROCESS_C_000000")]}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            atomic_json(shards._manifest_path(root, name), manifest)
            path = child_path(root, name)

            with patch.dict("os.environ", {
                    "MCPROD_PLAN_ACCEPT_POST_SECONDS": "10",
                    "MCPROD_PLAN_ACCEPT_POST_RUN_DIR": "/different/run"}, clear=True):
                with self.assertRaisesRegex(ValueError, "different run"):
                    controller_render_override(root)
                with self.assertRaisesRegex(ValueError, "different run"):
                    shards._render(root, name, {"scheduler": {}},
                                   allow_plan_accept_post_override=True)
            self.assertFalse(path.exists())

            with patch.dict("os.environ", {
                    "MCPROD_PLAN_ACCEPT_POST_SECONDS": "10",
                    "MCPROD_PLAN_ACCEPT_POST_RUN_DIR": str(root.resolve())}, clear=True):
                shards._render(root, name, {"scheduler": {}},
                               allow_plan_accept_post_override=True)
            first_bytes = path.read_bytes()
            first_inode = path.stat().st_ino
            self.assertIn("SCRIPT DEFER 75 10 POST PLAN_000000", first_bytes.decode())
            self.assertIn("SCRIPT DEFER 75 60 POST PROCESS_C_000000", first_bytes.decode())

            with patch.dict("os.environ", {
                    "MCPROD_PLAN_ACCEPT_POST_SECONDS": "15",
                    "MCPROD_PLAN_ACCEPT_POST_RUN_DIR": str(root.resolve())}, clear=True):
                result = shards._render(root, name, {"scheduler": {}},
                                        allow_plan_accept_post_override=True)
            self.assertTrue(result["preserved_existing"])
            self.assertEqual(path.read_bytes(), first_bytes)
            self.assertEqual(path.stat().st_ino, first_inode)

            fresh_name = "SOURCE_000001"
            atomic_json(shards._manifest_path(root, fresh_name), manifest)
            with patch.dict("os.environ", {}, clear=True):
                shards._render(root, fresh_name, {"scheduler": {}},
                               allow_plan_accept_post_override=True)
            self.assertIn("SCRIPT DEFER 75 60 POST PLAN_000000",
                          child_path(root, fresh_name).read_text())

    def test_repeated_prepare_source_preserves_existing_plan_post_interval(self):
        from tests.test_core_shards import ShardFixture

        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            fixture = ShardFixture("runTest")
            run = fixture.fixture(Path(temporary))
            path = child_path(run, "SOURCE_000000")
            environment = {
                "MCPROD_PLAN_ACCEPT_POST_SECONDS": "10",
                "MCPROD_PLAN_ACCEPT_POST_RUN_DIR": str(run.resolve()),
            }
            with patch.dict("os.environ", environment):
                first = shards.prepare_source(run, 0)
                first_bytes = path.read_bytes()
                first_inode = path.stat().st_ino
                state_before_retry = (run / "state.json").read_bytes()
                second = shards.prepare_source(run, 0)

            self.assertEqual(first["exit_code"], 0)
            self.assertTrue(second["preserved_existing"])
            self.assertIn("SCRIPT DEFER 75 10 POST PLAN_000000", first_bytes.decode())
            self.assertEqual(path.read_bytes(), first_bytes)
            self.assertEqual(path.stat().st_ino, first_inode)
            self.assertEqual((run / "state.json").read_bytes(), state_before_retry)


if __name__ == "__main__":
    unittest.main()
