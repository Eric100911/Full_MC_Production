"""Public configuration and submission boundaries for the optional shard path."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from full_mc.application.handoff import export_handoff
from full_mc.application.site_validation import REQUIRED_CHECKS, validate_site
from full_mc.backends.condor import resume_run, submit_run
from full_mc.cli import build_parser, main
from full_mc.domain.streams import may_still_need
from full_mc.io import atomic_json
from full_mc.settings import load_run_spec


def fixture_spec():
    return {
        "schema_version": 2, "run_id": "isolated-test", "block_events": 50,
        "orchestration": "campaign_shards", "runtime_release": "test-release",
        "storage": {
            "cern_staging": "root://eosuser.cern.ch///eos/user/test/staging",
            "ihep_products": "root://cceos.ihep.ac.cn:1094///store/user/test/products",
            "ihep_archive": "root://cceos.ihep.ac.cn:1094///store/user/test/archives",
            "staging_bytes": 10000, "reserve_bytes": 1000,
        },
        "sources": [{"pool": "pool", "source_id": "s1", "seed": 19,
                     "url": "root://example.org///inputs/source.lhe.gz",
                     "actual_events": 800, "size_bytes": 100}],
        "campaigns": [{"name": "C", "analysis": "JJP", "target_mixed_events": 20,
                       "max_processing_nodes": 2, "merge_events": 40,
                       "detector_variants": ["run3_2022_legacy_premix_v1"],
                       "sources": [{"pool": "pool", "mode": "normal", "lhe_budget": 25}]}],
    }


class ShardInterfaces(unittest.TestCase):
    def test_cleanup_cannot_substitute_allocation_for_missing_safe_cursor(self):
        with self.assertRaisesRegex(ValueError, "safe_cursor"):
            may_still_need(10, {"stride": 1, "offset": 0, "allocation_cursor": 20,
                                "closed": False})

    def test_opt_in_settings_and_stream_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "spec.json"
            raw = fixture_spec()
            atomic_json(path, raw)
            spec = load_run_spec(path)
            self.assertEqual(spec["limits"]["shard_nodes"], 10)
            self.assertEqual(spec["limits"]["target_window_nodes"], 20)
            raw["consumption_streams"] = [{"campaign": "absent", "pool": "pool",
                                           "stride": 5, "offset": 0}]
            atomic_json(path, raw)
            with self.assertRaises(ValueError):
                load_run_spec(path)

    def test_legacy_spec_does_not_acquire_new_requirements(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "spec.json"
            raw = fixture_spec()
            del raw["orchestration"]
            atomic_json(path, raw)
            spec = load_run_spec(path)
            self.assertNotIn("shard_nodes", spec["limits"])
            validate_site(spec)

    def test_invalid_new_window_and_modes_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "spec.json"
            for changes in ({"orchestration": "unknown"},
                            {"limits": {"shard_nodes": 10, "target_window_nodes": 9}},
                            {"normal_shortfall_policy": "ignore"}):
                raw = fixture_spec()
                raw.update(changes)
                atomic_json(path, raw)
                with self.subTest(changes=changes), self.assertRaises(ValueError):
                    load_run_spec(path)

    def test_internal_parser_and_script_exit_codes(self):
        args = build_parser().parse_args([
            "internal", "complete-shard", "--run-dir", "/tmp/chiw/run",
            "--campaign", "C", "--shard-index", "1", "--return-code", "-1"])
        self.assertEqual(args.return_code, -1)
        for code in (0, 75, 99):
            with self.subTest(code=code), patch("full_mc.cli.dispatch", return_value={"exit_code": code}):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(main(["internal", "prepare-source", "--run-dir",
                                           "/tmp/chiw/run", "--source-index", "0"]), code)

    def test_live_entrypoints_hold_before_any_external_call(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            atomic_json(root / "run.json", fixture_spec())
            (root / "run.dag").write_text("# not submitted\n")
            runner = Mock(side_effect=AssertionError("must not contact Condor"))
            with patch("full_mc.backends.condor.reserve_run_namespaces") as reserve:
                for operation in (submit_run, resume_run):
                    with self.subTest(operation=operation.__name__), self.assertRaisesRegex(
                            RuntimeError, "isolated staging budget"):
                        operation(root, runner=runner)
                reserve.assert_not_called()
            runner.assert_not_called()
            self.assertFalse((root / "submission.json").exists())

    def test_proof_requires_two_hosts_all_checks_and_real_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = root / "isolated-test-record.json"
            evidence.write_text("{}\n")
            proof_path = root / "proof.json"
            proof = {"schema": "cern-afs-lock-proof-v1", "schedds": ["test-a", "test-b"],
                     "checks": dict.fromkeys(REQUIRED_CHECKS, True), "evidence": [str(evidence)]}
            spec = fixture_spec()
            spec["site_validation"] = {"isolated_budget_bytes": 9000, "proof_path": str(proof_path)}
            atomic_json(proof_path, proof)
            validate_site(spec)
            proof["checks"]["read_after_unlock"] = False
            atomic_json(proof_path, proof)
            with self.assertRaisesRegex(RuntimeError, "not all passed"):
                validate_site(spec)
            proof["checks"]["read_after_unlock"] = True
            proof["schedds"] = ["test-a", "test-a"]
            atomic_json(proof_path, proof)
            with self.assertRaisesRegex(RuntimeError, "two different"):
                validate_site(spec)

    def test_handoff_accepts_complete_new_state_and_rejects_unfinished(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = fixture_spec()
            spec["campaigns"] = []
            atomic_json(root / "run.json", spec)
            state = {"state_schema": "mcprod-shard-state-v1", "status": "planned",
                     "merge_group_counts": {}}
            atomic_json(root / "state.json", state)
            with self.assertRaisesRegex(RuntimeError, "completed run"):
                export_handoff(root)
            state["status"] = "complete"
            atomic_json(root / "state.json", state)
            result = export_handoff(root)
            self.assertEqual(result["schema"], "full-mc-handoff-v1")
            self.assertEqual(result["tasks"], [])


if __name__ == "__main__":
    unittest.main()
