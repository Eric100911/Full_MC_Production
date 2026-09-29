"""Production-core storage namespace and runtime-package safety contracts."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.io import atomic_json
from full_mc.runtime import packages
from full_mc.runtime.bootstrap import unpack_runtime
from full_mc.settings import load_run_spec
from full_mc.storage.layout import canonical_storage_location, storage_path_is_within


IHEP_POOL = "root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/LHE_pool/SPS-Jpsi"
CUSTOM_SOURCE = "root://cceos.ihep.ac.cn///store/user/chiw/custom/source.lhe.gz"


def spec_payload(root, *, cern_staging, source=None):
    source = source or {
        "pool": "pool_jpsi_CSCO_g",
        "source_id": "generated",
        "seed": 101,
        "generator_profile": "helac_jpsi_csco_g_v1",
    }
    return {
        "schema_version": 2,
        "run_id": "safety-test",
        "runtime_release": "test-release",
        "storage": {
            "cern_staging": cern_staging,
            "ihep_products": str(root / "products"),
            "ihep_archive": str(root / "archive"),
        },
        "sources": [source],
        "campaigns": [{
            "name": "C", "analysis": "JJP", "target_mixed_events": 10,
            "max_processing_nodes": 1, "merge_events": 10,
            "detector_variants": ["run3_2022_legacy_premix_v1"],
            "sources": [{"pool": source["pool"], "mode": "normal", "lhe_budget": 10}],
        }],
    }


class StorageIdentityTests(unittest.TestCase):
    def test_authority_default_port_and_traversal_are_normalized(self):
        self.assertEqual(
            canonical_storage_location("root://cceos.ihep.ac.cn///store/a/../b"),
            canonical_storage_location("root://cceos.ihep.ac.cn:1094///store/b"),
        )
        self.assertTrue(
            storage_path_is_within(
                "root://cceos.ihep.ac.cn///store/b/child",
                "root://cceos.ihep.ac.cn:1094///store/b",
            )
        )

    def test_authorities_are_not_collapsed_and_prefix_sibling_is_safe(self):
        self.assertFalse(
            storage_path_is_within(
                "root://eosuser.cern.ch///store/user/chiw/MC_Production_v3/LHE_pool",
                IHEP_POOL,
            )
        )
        self.assertFalse(
            storage_path_is_within(IHEP_POOL + "_extra", IHEP_POOL)
        )

    def test_local_eos_mount_matches_cern_xrootd_namespace(self):
        self.assertTrue(
            storage_path_is_within(
                "/eos/user/c/chiw/staging/run",
                "root://eosuser.cern.ch///eos/user/c/chiw/staging",
            )
        )


class RunSpecStorageGuardTests(unittest.TestCase):
    def load(self, root, *, cern_staging, source=None):
        path = root / "spec.json"
        atomic_json(path, spec_payload(root, cern_staging=cern_staging, source=source))
        return load_run_spec(path)

    def test_default_cern_staging_is_distinct_from_ihep_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            resolved = self.load(
                root,
                cern_staging="root://eosuser.cern.ch///eos/user/c/chiw/staging",
            )
            self.assertEqual(resolved["run_id"], "safety-test")

    def test_cern_staging_requires_an_explicit_xrootd_url(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for spelling in (
                "/eos/user/c/chiw/staging",
                "file:/eos/user/c/chiw/staging",
                str(root / "staging"),
            ):
                with self.subTest(spelling=spelling), self.assertRaisesRegex(
                    ValueError, "storage.cern_staging must be an XRootD URL"
                ):
                    self.load(root, cern_staging=spelling)

    def test_configured_inventory_and_descendants_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for spelling in (
                IHEP_POOL,
                IHEP_POOL + "/../SPS-Jpsi/child",
            ):
                with self.subTest(spelling=spelling), self.assertRaisesRegex(
                    ValueError, "overlaps immutable"
                ):
                    self.load(root, cern_staging=spelling)

    def test_generated_only_specs_are_checked_against_configured_pools(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "overlaps immutable"):
                self.load(
                    root,
                    cern_staging=(
                        "root://cceos.ihep.ac.cn///store/user/chiw/"
                        "MC_Production_v3/LHE_pool/SPS-Jpsi"
                    ),
                )

    def test_selected_custom_existing_source_parent_is_guarded(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            existing = {
                "pool": "pool_jpsi_CSCO_g", "source_id": "custom", "seed": 101,
                "url": CUSTOM_SOURCE, "actual_events": 10, "size_bytes": 12,
            }
            with self.assertRaisesRegex(ValueError, "overlaps immutable"):
                self.load(
                    root,
                    cern_staging="root://cceos.ihep.ac.cn:1094///store/user/chiw/custom",
                    source=existing,
                )

    def test_same_lfn_on_different_authority_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            existing = {
                "pool": "pool_jpsi_CSCO_g", "source_id": "other", "seed": 101,
                "url": "root://other.example///store/user/chiw/custom/source.lhe.gz",
                "actual_events": 10, "size_bytes": 12,
            }
            resolved = self.load(
                root,
                cern_staging="root://cceos.ihep.ac.cn///store/user/chiw/custom",
                source=existing,
            )
            self.assertEqual(resolved["sources"][0]["url"], existing["url"])


class RuntimePackageTests(unittest.TestCase):
    def test_prepare_and_unpack_use_stable_runtime_basename(self):
        core = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            release = root / "release"
            descriptor = packages.prepare_runtime(
                core, release, "package-test", "cern", profiles=["helac-el7"]
            )
            self.assertEqual(descriptor["bundles"]["helac-el7"], "helac_package.tar.gz")
            self.assertEqual(descriptor["helac_package"]["source"], packages.HELAC_SOURCE_PACKAGE)
            self.assertEqual(
                packages._sha256(release / "helac_package.tar.gz"),
                packages.HELAC_PACKAGE_SHA256,
            )
            unpacked = unpack_runtime(release, root / "worker", "helac-el7", "package-test")
            self.assertTrue((unpacked / "common/packages/helac_package.tar.gz").is_file())

    def test_prepare_rejects_missing_or_modified_source(self):
        core = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(packages, "HELAC_SOURCE_PACKAGE", "does-not-exist.tar.gz"):
                with self.assertRaises(FileNotFoundError):
                    packages.prepare_runtime(core, root / "missing", "missing-test", "cern",
                                             profiles=["helac-el7"])
            with patch.object(packages, "_sha256", return_value="0" * 64):
                with self.assertRaisesRegex(ValueError, "sha256"):
                    packages.prepare_runtime(core, root / "modified", "modified-test", "cern",
                                             profiles=["helac-el7"])


if __name__ == "__main__":
    unittest.main()
