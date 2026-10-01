"""Acceptance contracts for schema-v2 profiles and detector fan-out."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from full_mc.application.production import _process_phase
from full_mc.io import atomic_json
from full_mc.profiles import load_catalogues
from full_mc.settings import load_run_spec


class SimulationProfileContracts(unittest.TestCase):
    def spec(self, root):
        payload = {
            "schema_version": 2,
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
                self.assertEqual((profile["nmc"], profile["unwevt"]), (500000, 2000))
                self.assertEqual(profile["planning_events_hint"], hint)
                self.assertTrue(profile["process"])
                self.assertEqual(set(profile["cuts"]), {
                    "min_pt_conia", "min_pt_bonia", "min_pt_q",
                    "min_m_qqp", "min_m_q_beam"})
        self.assertEqual(profiles.require_ready("helac_jpsi_csco_g_v1")["unwevt_semantics"],
                         "addon-input-not-output-cap")

    def test_one_shared_mix_fans_out_to_distinct_detector_tasks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "steps").mkdir()
            (root / "tasks").mkdir()
            base = load_run_spec(self.spec(root))
            profile = base["campaigns"][0]["resolved_detector_variants"][0]
            variants = []
            for index in range(5):
                variant = copy.deepcopy(profile)
                variant["profile_id"] = "variant%d" % index
                variants.append(variant)
            spec = dict(base, campaigns=[dict(base["campaigns"][0],
                                              resolved_detector_variants=variants,
                                              detector_variants=[p["profile_id"] for p in variants])],
                        limits={"mixed_hepmc_bytes": 1000})
            allocation = {"campaigns": [{
                "campaign": "C", "nodes": [{
                    "job_id": "JOB000000_BLOCK000000",
                    "target_mixed_events": 10,
                    "edm_event_id": {"first_run": 1},
                    "mixing_rng_seed": 11,
                    "sources": [{"slot": 0, "mode": "normal",
                                  "block_refs": ["BLOCK:pool_2jpsi_g:source-1:000000"],
                                  "source_rng_seed": 12, "lhe_events": 10}],
                }],
            }]}
            state = {"resident": {}, "originals": [], "blocks": {
                "pool_2jpsi_g": [{"artifact_id": "BLOCK:pool_2jpsi_g:source-1:000000",
                                   "events": 10, "url": str(root / "block.lhe.gz")}]
            }}
            result = _process_phase(root, 0, spec, state, allocation)
            tasks = [json.loads(path.read_text()) for path in (root / "tasks").glob("*.json")]
            mix = [task for task in tasks if task["stage"] == "mix"]
            mini = [task for task in tasks if task["stage"] == "miniaod"]
            archive = [task for task in tasks if task["stage"] == "archive"]
            self.assertEqual((len(mix), len(mini), len(archive)), (1, 5, 1))
            self.assertEqual({task["inputs"][0]["artifact_id"] for task in mini},
                             {mix[0]["outputs"][0]["artifact_id"]})
            self.assertEqual(len({task["outputs"][0]["url"] for task in mini}), 5)
            self.assertEqual({item["artifact_id"] for item in archive[0]["inputs"]
                              if item["kind"] == "hepmc"},
                             {mix[0]["outputs"][0]["artifact_id"]})
            dag = (root / "steps" / "000000.dag").read_text()
            self.assertEqual(dag.count("CHILD ARCHIVE_"), 1)
            self.assertEqual(dag.count(" CHILD ARCHIVE_"), 1)
            archive_parent = next(line for line in dag.splitlines()
                                  if line.startswith("PARENT MINIAOD_"))
            self.assertEqual(archive_parent.count("MINIAOD_"), 5)


if __name__ == "__main__":
    unittest.main()
