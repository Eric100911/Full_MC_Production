"""Acceptance contracts for schema-v2 profiles and detector fan-out."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from full_mc.io import atomic_json
from full_mc.profiles import load_catalogues
from full_mc.settings import load_run_spec


class SimulationProfileContracts(unittest.TestCase):
    def test_packaged_generator_catalogue_matches_repository(self):
        root = Path(__file__).resolve().parents[1]
        common = root / "common/profiles/generator_profiles.json"
        packaged = root / "src/full_mc/resources/generator_profiles.json"
        self.assertEqual(json.loads(packaged.read_text()), json.loads(common.read_text()))

    def spec(self, root):
        payload = {
            "schema_version": 2, "orchestration": "static_subdag",
            "run_id": "profile-test",
            "runtime_release": "test-release",
            "block_events": 100,
            "storage": {
                "cern_staging": "root://eosuser.cern.ch///eos/user/c/chiw/staging",
                "ihep_products": str(root / "products"),
                "ihep_archive": str(root / "archive"),
            },
            "sources": [{
                "pool": "pool_2jpsi_g", "source_id": "source-1", "seed": 11,
                "generator_profile": "helac_2jpsi_g_nlostar_v1",
            }],
            "campaigns": [{
                "name": "C", "analysis": "JJP", "target_mixed_events": 10,
                "max_processing_nodes": 1, "merge_events": 10,
                "detector_variants": ["run3_2022_legacy_premix_v1"],
                "sources": [{"pool": "pool_2jpsi_g", "mode": "normal",
                              "lhe_budget": 10}],
            }],
        }
        path = root / "spec.json"
        atomic_json(path, payload)
        return path

    def test_resolution_freezes_full_profiles_and_rejects_v1(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            resolved = load_run_spec(self.spec(root))
            source = resolved["sources"][0]
            detector = resolved["campaigns"][0]["resolved_detector_variants"][0]
            self.assertEqual(source["resolved_generator_profile"]["nmc"], 500000)
            self.assertEqual(source["resolved_generator_profile"]["planning_events_hint"], 597)
            self.assertEqual(detector["mc_global_tag"], "124X_mcRun3_2022_realistic_v12")
            self.assertEqual(detector["pileup"]["secondary_kind"], "premix")
            self.assertNotIn("configs", detector)

            raw = json.loads((root / "spec.json").read_text())
            raw["schema_version"] = 1
            atomic_json(root / "v1.json", raw)
            with self.assertRaisesRegex(ValueError, "schema_version"):
                load_run_spec(root / "v1.json")

    def test_catalogue_blocks_unavailable_and_keeps_pileup_semantics(self):
        catalogues = load_catalogues()
        self.assertEqual(
            catalogues["detector"].get("run3_2022_legacy_premix_v1")["pileup"]["mode"],
            "premix")
        with self.assertRaisesRegex(ValueError, "run3_2022_summer24_mu50_v1.*unavailable"):
            catalogues["detector"].require_ready("run3_2022_summer24_mu50_v1")
        with self.assertRaisesRegex(ValueError, "incompatible"):
            from full_mc.profiles import resolve_source
            resolve_source({"pool": "pool_gg", "source_id": "s", "seed": 11,
                            "generator_profile": "helac_2jpsi_g_nlostar_v1"},
                           catalogues["generator"])

    def test_helac_source_seed_range_is_checked_at_settings_boundary(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = self.spec(root)
            payload = json.loads(path.read_text(encoding="utf-8"))
            for seed in (10, 100000):
                with self.subTest(seed=seed):
                    payload["sources"][0]["seed"] = seed
                    atomic_json(path, payload)
                    with self.assertRaisesRegex(
                        ValueError, "HELAC seed must be between 11 and 99999"
                    ):
                        load_run_spec(path)

    def test_ready_generator_profiles_freeze_measured_controls(self):
        profiles = load_catalogues()["generator"]
        expected = {
            "helac_2jpsi_g_nlostar_v1": ("pool_2jpsi_g", 597),
            "helac_gg_v1": ("pool_gg", 2000),
            "helac_2jpsi_cs_v1": ("pool_2jpsi_cs", 2000),
            "helac_jpsi_csco_g_v1": ("pool_jpsi_CSCO_g", 5316),
        }
        for profile_id, (pool, hint) in expected.items():
            with self.subTest(profile_id=profile_id):
                profile = profiles.require_ready(profile_id)
                self.assertEqual(profile["compatible_pools"], [pool])
                stop_target = 10000000 if pool in {"pool_gg", "pool_2jpsi_cs"} else 2000
                self.assertEqual((profile["nmc"], profile["unwevt"]), (500000, stop_target))
                self.assertEqual(profile["planning_events_hint"], hint)
                self.assertTrue(profile["process"])
                self.assertEqual(set(profile["cuts"]), {
                    "min_pt_conia", "min_pt_bonia", "min_pt_q",
                    "min_m_qqp", "min_m_q_beam"})
        self.assertEqual(profiles.require_ready("helac_jpsi_csco_g_v1")["unwevt_semantics"],
                         "addon-input-not-output-cap")


if __name__ == "__main__":
    unittest.main()
