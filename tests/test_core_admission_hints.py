"""Negative-cache admission hints never replace authoritative PRE checks."""
from copy import deepcopy
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from full_mc.application import admission_hints, shards, staging
from full_mc.io import atomic_json, read_json
from tests import test_core_production
from tests.test_core_shards import ShardFixture


class AdmissionHintTests(unittest.TestCase):
    def setUp(self):
        self.enabled = patch.dict(os.environ, {"MCPROD_ADMISSION_HINTS": "1"})
        self.enabled.start()
        self.addCleanup(self.enabled.stop)

    def fixture(self, directory):
        root = Path(directory)
        spec_path, _ = test_core_production.ProductionLifecycle().fixture(root)
        spec = read_json(spec_path)
        spec["sources"].extend([
            dict(spec["sources"][0], source_id="s2", seed=20),
            dict(spec["sources"][0], pool="pool_b", source_id="b0", seed=21),
        ])
        atomic_json(root / "run.json", spec)
        return root, spec

    def view(self, spec, *, observed_at=None, source_next=None, shard_next=None):
        return {"schema": admission_hints.HINT_SCHEMA, "run_id": spec["run_id"],
                "observed_at": time.time() if observed_at is None else observed_at,
                "source_next": {"pool": 0, "pool_b": 2} if source_next is None else source_next,
                "shard_next": {"C": 2, "D": 0} if shard_next is None else shard_next}

    def write_view(self, root, value):
        atomic_json(root / "staging" / "admission-hints.json", value)

    def shard_run(self, directory, *, nodes=8, campaigns=1):
        fixture = ShardFixture()
        run = fixture.fixture(Path(directory), nodes=nodes, campaigns=campaigns)
        spec = read_json(run / "run.json")
        spec["limits"]["target_window_nodes"] = nodes
        spec["sources"].append(dict(spec["sources"][0], source_id="s2", seed=20,
                                    url=spec["sources"][0]["url"] + ".s2"))
        atomic_json(run / "run.json", spec)
        return run, spec, fixture

    def test_fresh_hint_only_rejects_known_later_source_or_shard(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            root, spec = self.fixture(temporary)
            spec["campaigns"][0]["max_processing_nodes"] = 4
            atomic_json(root / "run.json", spec)
            self.write_view(root, self.view(spec))
            with patch.object(staging, "transaction", side_effect=AssertionError("ledger read")), \
                    patch.object(shards, "_receipt", side_effect=AssertionError("receipt read")):
                self.assertEqual(admission_hints.reject_source(root, spec, 1),
                                 "waiting for earlier source admission in this pool")
                self.assertIsNone(admission_hints.reject_source(root, spec, 0))
                self.assertIsNone(admission_hints.reject_source(root, spec, 2))
                self.assertEqual(admission_hints.reject_shard(root, spec, "C", 3),
                                 "waiting for earlier assignments in this campaign")
                self.assertIsNone(admission_hints.reject_shard(root, spec, "C", 2))
                self.assertIsNone(admission_hints.reject_shard(root, spec, "D", 0))

    def test_fresh_future_source_pre_returns_hint_without_ledger_transaction(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            run, spec, _ = self.shard_run(temporary)
            self.write_view(run, self.view(spec, source_next={"pool": 0},
                                           shard_next={"C": 0}))
            before = (run / "state.json").read_bytes()
            with patch.object(staging, "transaction", wraps=staging.transaction) as transaction:
                result = shards.prepare_source(run, 1)

            self.assertEqual(result["exit_code"], 75)
            self.assertTrue(result["admission_hint"])
            transaction.assert_not_called()
            self.assertEqual((run / "state.json").read_bytes(), before)
            self.assertFalse(shards._manifest_path(run, "SOURCE_000001").exists())

    def test_stale_hint_runs_authoritative_source_order_guard(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            run, spec, _ = self.shard_run(temporary)
            self.write_view(run, self.view(
                spec, observed_at=time.time() - admission_hints.MAX_AGE_SECONDS - 1,
                source_next={"pool": 0}, shard_next={"C": 0}))
            before = (run / "state.json").read_bytes()
            with patch.object(staging, "transaction", wraps=staging.transaction) as transaction:
                result = shards.prepare_source(run, 1)

            self.assertEqual(result["exit_code"], 75)
            self.assertIn("waiting for earlier source admission", result["reason"])
            self.assertNotIn("admission_hint", result)
            transaction.assert_called_once()
            self.assertEqual((run / "state.json").read_bytes(), before)

    def test_terminal_source_skip_is_not_serialized_by_hint_or_ordering(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            run, spec, _ = self.shard_run(temporary, nodes=2, campaigns=2)
            with staging.transaction(run) as state:
                for campaign in spec["campaigns"]:
                    state["planner"]["assigned_progress"][campaign["name"]] = int(
                        campaign["max_processing_nodes"])
            self.write_view(run, self.view(
                spec, source_next={"pool": 0}, shard_next={"C": 2, "D": 2}))

            result = shards.prepare_source(run, 1)
            state = read_json(run / "state.json")
            self.assertEqual(result["exit_code"], 99)
            self.assertEqual(result["status"], "unneeded")
            self.assertEqual(state["planner"]["sources"]["1"], "skipped")
            self.assertNotIn("0", state["planner"]["sources"])

    def test_existing_source_manifest_repair_bypasses_a_fresh_order_hint(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            run, spec, fixture = self.shard_run(temporary)
            self.assertEqual(shards.prepare_source(run, 0)["exit_code"], 0)
            fixture.source_plan(run)

            with patch.object(staging, "_write_state", side_effect=OSError("before replace")):
                with self.assertRaisesRegex(OSError, "before replace"):
                    shards.prepare_source(run, 1)
            manifest = shards._manifest_path(run, "SOURCE_000001")
            self.assertTrue(manifest.is_file())
            frozen = manifest.read_bytes()
            self.write_view(run, self.view(spec, source_next={"pool": 0},
                                           shard_next={"C": 0}))

            with patch.object(staging, "transaction", wraps=staging.transaction) as transaction:
                result = shards.prepare_source(run, 1)

            self.assertEqual(result["exit_code"], 0)
            self.assertNotIn("admission_hint", result)
            transaction.assert_called_once()
            self.assertEqual(manifest.read_bytes(), frozen)
            self.assertEqual(read_json(run / "state.json")["planner"]["sources"]["1"],
                             "admitted")

    def test_missing_malformed_stale_wrong_run_and_invalid_numbers_fall_back(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            root, spec = self.fixture(temporary)
            hint_path = root / "staging" / "admission-hints.json"
            self.assertIsNone(admission_hints.reject_source(root, spec, 1))
            hint_path.parent.mkdir(exist_ok=True)
            hint_path.write_text("{broken", encoding="utf-8")
            self.assertIsNone(admission_hints.reject_source(root, spec, 1))

            invalid = [
                self.view(spec, observed_at=time.time() - admission_hints.MAX_AGE_SECONDS - 1),
                dict(self.view(spec), run_id="another-run"),
                self.view(spec, observed_at=time.time() + 1),
                self.view(spec, observed_at=float("nan")),
                self.view(spec, observed_at=float("inf")),
                self.view(spec, observed_at=10 ** 400),
            ]
            for value in invalid:
                with self.subTest(observed_at=value.get("observed_at"), run_id=value.get("run_id")):
                    self.write_view(root, value)
                    self.assertIsNone(admission_hints.reject_source(root, spec, 1))
                    self.assertIsNone(admission_hints.reject_shard(root, spec, "C", 3))

    def test_wrong_run_and_future_progress_values_do_not_reject(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            root, spec = self.fixture(temporary)
            self.write_view(root, self.view(spec, source_next={"pool": 999},
                                            shard_next={"C": 10 ** 400}))
            self.assertIsNone(admission_hints.reject_source(root, spec, 1))
            self.assertIsNone(admission_hints.reject_shard(root, spec, "C", 3))

    def test_late_publication_does_not_restart_observation_age(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            root, spec = self.fixture(temporary)
            observed_at = 1000.0
            with patch.object(admission_hints.time, "time", return_value=1000.0 +
                              admission_hints.MAX_AGE_SECONDS + 1), \
                    patch.object(admission_hints, "atomic_json") as write:
                admission_hints.publish(root, {"planner": {"sources": {},
                    "assigned_progress": {"C": 0}}}, observed_at)
            write.assert_not_called()
            self.assertFalse((root / "staging" / "admission-hints.json").exists())

    def test_publication_failure_cannot_modify_committed_state(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            root, spec = self.fixture(temporary)
            state = {"planner": {"sources": {"0": "admitted"},
                                 "assigned_progress": {"C": 1, "D": 0}}}
            before = deepcopy(state)
            with patch.object(admission_hints, "atomic_json",
                              side_effect=OSError("hint write failed")):
                admission_hints.publish(root, state, time.time())
            self.assertEqual(state, before)

    def test_disabled_mode_performs_no_io(self):
        with tempfile.TemporaryDirectory(dir="/tmp/chiw") as temporary:
            root, spec = self.fixture(temporary)
            with patch.dict(os.environ, {"MCPROD_ADMISSION_HINTS": "0"}), \
                    patch.object(admission_hints, "read_json",
                                 side_effect=AssertionError("read")) as read, \
                    patch.object(admission_hints, "atomic_json",
                                 side_effect=AssertionError("write")) as write:
                self.assertIsNone(admission_hints.reject_source(root, spec, 1))
                self.assertIsNone(admission_hints.reject_shard(root, spec, "C", 3))
                admission_hints.publish(root, {}, time.time())
            read.assert_not_called()
            write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
