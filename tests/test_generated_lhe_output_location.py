#!/usr/bin/env python3
"""Regression tests for generated-LHE output locations (plan Item 1).

Source plan: ``docs/lhe_output_location_and_crystalball_nmc_2026-09-18.md``,
Item 1 ("separate generated output from immutable inventory").

The confirmed defect: two write-side call sites used the read-side resolver
``existing_lhe_pool_dir()``, so freshly generated LHE was written into the
immutable ``MC_Production_v3/LHE_pool/<subdir>`` inventory and the planner then
read back and accepted the overwritten file. Generated LHE must live under
``<target-base>/lhe_pools/<pool-storage-name>/``.

These tests drive ``dag_generator.py generate-test`` and inspect the *emitted*
node-config JSON, not only helper return values, so they fail if the emitted
destination ever drifts back to the inventory tree. No network access, no EOS
writes: every run is offline (``--no-scan-existing`` or an explicit inventory).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
TMP_ROOT = Path("/tmp/chiw")
sys.path.insert(0, str(BASE_DIR))

import dag_generator  # noqa: E402


POOL = "pool_2jpsi_cs"
CAMPAIGN = "JJP_SPS_CS"  # single-source (JJP_SPS_CS -> pool_2jpsi_cs)
STORAGE = dag_generator.pool_storage_name(POOL)
INVENTORY_DIR = dag_generator.LHE_POOL_DIRECTORY_CONFIG[POOL]["path"].rstrip("/")
DEFAULT_BASE = dag_generator.EOS_BASE.rstrip("/")
V45_TARGET = "root://cceos.ihep.ac.cn:1094///store/user/chiw/v4_5_full"
SAMPLE_RE = re.compile(
    r"^sample_(?P<storage>[A-Za-z0-9_]+)_(?P<seed>\d+)\.lhe(?:\.gz)?$"
)


def run_generate_test(output_dir: Path, extra_args: list[str]) -> subprocess.CompletedProcess:
    """Run an offline ``generate-test`` into ``output_dir`` and return the result."""

    command = [
        sys.executable,
        str(BASE_DIR / "dag_generator.py"),
        "generate-test",
        "--campaign",
        CAMPAIGN,
        "--jobs",
        "1",
        "--max-events",
        "20",
        "--enable-lhe-block-subdags",
        "--no-scan-existing",
        "--lhe-events-per-block",
        "5",
        "--disable-ntuple",
        "--output-dir",
        str(output_dir),
        "--output",
        "regression.dag",
    ]
    command.extend(extra_args)
    return subprocess.run(
        command,
        cwd=BASE_DIR,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def load_node_configs(output_dir: Path, kind: str) -> list[dict]:
    directory = output_dir / "node_configs" / kind
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(directory.glob("*.json"))
    ]


def single_config(output_dir: Path, kind: str) -> dict:
    configs = load_node_configs(output_dir, kind)
    if len(configs) != 1:
        raise AssertionError(
            f"expected exactly one {kind} node config in {output_dir}, got {len(configs)}"
        )
    return configs[0]


class GeneratedLheOutputLocationTest(unittest.TestCase):
    """Item 1 regression coverage: case numbers follow the plan's test list."""

    def setUp(self) -> None:
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self._tmp = tempfile.TemporaryDirectory(
            prefix="generated_lhe_output_location_", dir=TMP_ROOT
        )
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def generate(self, name: str, extra_args: list[str]) -> Path:
        output_dir = self.root / name
        completed = run_generate_test(output_dir, extra_args)
        self.assertEqual(
            completed.returncode,
            0,
            f"generate-test failed for {name}:\n{completed.stdout}\n{completed.stderr}",
        )
        return output_dir

    # ------------------------------------------------------------------
    # (1) Fresh remote generation with a v4_5_full target
    # ------------------------------------------------------------------
    def test_fresh_remote_generation_lands_under_target_lhe_pools(self) -> None:
        output_dir = self.generate(
            "fresh_v45", ["--machine-env", "lxplus_t2_ihep", "--target-base-url", V45_TARGET]
        )

        lhe_config = single_config(output_dir, "lhe_generation")
        generated_dir = lhe_config["output_dir"]
        expected_dir = f"{V45_TARGET}/lhe_pools/{STORAGE}"

        # The emitted generation node config - not just a helper - must point at
        # the target lhe_pools tree.
        self.assertEqual(generated_dir, expected_dir)
        self.assertNotIn("MC_Production_v3", generated_dir)
        self.assertFalse(generated_dir.startswith(INVENTORY_DIR))

        planning = single_config(output_dir, "planning")
        self.assertEqual(len(planning["lhe_paths"]), 1)
        lhe_path = planning["lhe_paths"][0]
        match = SAMPLE_RE.match(os.path.basename(lhe_path))
        self.assertIsNotNone(match, f"unexpected generated LHE name: {lhe_path}")
        self.assertEqual(match.group("storage"), STORAGE)
        self.assertEqual(os.path.dirname(lhe_path), expected_dir)
        self.assertNotIn("MC_Production_v3", lhe_path)
        # No --compress-lhe here, so run_helac.sh stages an uncompressed
        # sample; the planner must be handed that same name.
        self.assertEqual(os.path.basename(lhe_path), f"sample_{STORAGE}_60100.lhe")

        # Planner block output is a write target too and must share the tree.
        self.assertEqual(planning["block_output_dir"], f"{expected_dir}/lhe_blocks")

        # All three destinations resolve under one and the same target tree.
        for destination in (
            generated_dir,
            os.path.dirname(lhe_path),
            planning["block_output_dir"],
        ):
            self.assertTrue(
                destination == expected_dir
                or destination.startswith(expected_dir + "/"),
                f"{destination} is outside the shared target tree {expected_dir}",
            )

    def test_generated_lhe_extension_follows_compress_lhe(self) -> None:
        """The planner is handed the extension the worker actually stages out.

        ``run_helac.sh`` only writes ``.lhe.gz`` when ``--compress-lhe`` is set.
        Hardcoding the compressed name made the planner node look for a file
        that was never written whenever the flag was absent.
        """

        uncompressed = self.generate(
            "ext_uncompressed",
            ["--machine-env", "lxplus_t2_ihep", "--target-base-url", V45_TARGET],
        )
        compressed = self.generate(
            "ext_compressed",
            [
                "--machine-env",
                "lxplus_t2_ihep",
                "--target-base-url",
                V45_TARGET,
                "--compress-lhe",
            ],
        )

        plain = single_config(uncompressed, "planning")["lhe_paths"][0]
        gzipped = single_config(compressed, "planning")["lhe_paths"][0]
        self.assertEqual(os.path.basename(plain), f"sample_{STORAGE}_60100.lhe")
        self.assertEqual(os.path.basename(gzipped), f"sample_{STORAGE}_60100.lhe.gz")
        # Only the extension may differ; the destination tree is identical.
        self.assertEqual(os.path.dirname(plain), os.path.dirname(gzipped))
        self.assertEqual(os.path.dirname(plain), f"{V45_TARGET}/lhe_pools/{STORAGE}")

    # ------------------------------------------------------------------
    # (2) Existing-LHE mode keeps reading the immutable inventory
    # ------------------------------------------------------------------
    def test_existing_lhe_mode_planner_inputs_stay_in_inventory(self) -> None:
        seed = 60100
        exact_inventory_url = (
            f"{INVENTORY_DIR}/sample_{STORAGE}_{seed}.lhe.gz"
        )
        inventory_path = self.root / "existing_inventory.json"
        inventory_path.write_text(
            json.dumps(
                {
                    "pools": {
                        POOL: {
                            "files": [
                                {
                                    "path": exact_inventory_url,
                                    "seed": seed,
                                    "actual_events": 5316,
                                }
                            ]
                        }
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        output_dir = self.generate(
            "existing_mode",
            [
                "--machine-env",
                "lxplus_t2_ihep",
                "--skip-lhe-generation",
                "--existing-lhe-inventory",
                str(inventory_path),
            ],
        )

        planning = single_config(output_dir, "planning")
        self.assertEqual(planning["lhe_paths"], [exact_inventory_url])
        # Guard against a helper accidentally rewriting the inventory path.
        self.assertEqual(
            planning["lhe_paths"][0],
            f"{dag_generator.LHE_POOL_DIRECTORY_CONFIG[POOL]['path']}/"
            f"sample_{STORAGE}_{seed}.lhe.gz",
        )
        self.assertIn("/MC_Production_v3/LHE_pool/", planning["lhe_paths"][0])
        # No generation node is emitted in skip-LHE mode.
        self.assertEqual(load_node_configs(output_dir, "lhe_generation"), [])

    # ------------------------------------------------------------------
    # (3) --existing-lhe-base never redirects a generated destination
    # ------------------------------------------------------------------
    def test_existing_lhe_base_does_not_change_generated_destination(self) -> None:
        without = self.generate(
            "without_elb",
            ["--machine-env", "lxplus_t2_ihep", "--target-base-url", V45_TARGET],
        )
        with_base = self.generate(
            "with_elb",
            [
                "--machine-env",
                "lxplus_t2_ihep",
                "--target-base-url",
                V45_TARGET,
                "--existing-lhe-base",
                DEFAULT_BASE,
            ],
        )

        self.assertEqual(
            single_config(without, "lhe_generation")["output_dir"],
            single_config(with_base, "lhe_generation")["output_dir"],
        )
        self.assertEqual(
            single_config(without, "planning")["lhe_paths"],
            single_config(with_base, "planning")["lhe_paths"],
        )
        self.assertEqual(
            single_config(without, "planning")["block_output_dir"],
            single_config(with_base, "planning")["block_output_dir"],
        )
        # ... and both are still the v4_5_full generated tree.
        self.assertEqual(
            single_config(with_base, "lhe_generation")["output_dir"],
            f"{V45_TARGET}/lhe_pools/{STORAGE}",
        )

    # ------------------------------------------------------------------
    # (4) Default write target: selected policy, not the rejected one
    # ------------------------------------------------------------------
    def test_default_target_base_generation_succeeds_without_approved_target(self) -> None:
        """A remote write workflow with no --target-base-url still generates.

        NOTE (binding user decision, 2026-09-18): the stronger policy
        "require an explicit ``--target-base-url`` for any remote DAG that can
        write LHE" - and "fail when the target resolves to the v3 default" - was
        considered and REJECTED for this branch. The only enforced rejection is
        an overlap with a configured ``lhe_pool_directories.<pool>.path`` (case
        7). So this test asserts the actual decided behaviour: generation
        succeeds with no explicit target and, because it now uses the write-side
        resolver, its destination is ``<default-base>/lhe_pools/<storage>``
        rather than the immutable ``LHE_pool`` inventory directory.
        """

        output_dir = self.generate("default_target", ["--machine-env", "lxplus_t2_ihep"])

        generated_dir = single_config(output_dir, "lhe_generation")["output_dir"]
        self.assertEqual(generated_dir, f"{DEFAULT_BASE}/lhe_pools/{STORAGE}")
        self.assertFalse(generated_dir.startswith(INVENTORY_DIR))
        self.assertNotIn("/LHE_pool/", generated_dir)

        planning = single_config(output_dir, "planning")
        self.assertEqual(
            os.path.dirname(planning["lhe_paths"][0]),
            f"{DEFAULT_BASE}/lhe_pools/{STORAGE}",
        )
        self.assertEqual(
            planning["block_output_dir"],
            f"{DEFAULT_BASE}/lhe_pools/{STORAGE}/lhe_blocks",
        )

    # ------------------------------------------------------------------
    # (5) Local generation and local planner paths agree exactly
    # ------------------------------------------------------------------
    def test_local_generation_and_planner_paths_agree(self) -> None:
        local_base = self.root / "local_base"
        output_dir = self.generate(
            "local",
            [
                "--machine-env",
                "local_condor",
                "--local-output-base",
                str(local_base),
            ],
        )

        lhe_config = single_config(output_dir, "lhe_generation")
        relative_generated_dir = lhe_config["output_dir"]

        # The worker prepends LOCAL_OUTPUT_BASE to OUTPUT_DIR, so the emitted
        # value must stay relative: lhe_pools/<storage>.
        self.assertFalse(os.path.isabs(relative_generated_dir))
        self.assertEqual(relative_generated_dir, os.path.join("lhe_pools", STORAGE))

        worker_visible_dir = os.path.join(str(local_base), relative_generated_dir)
        expected_dir = os.path.join(str(local_base), "lhe_pools", STORAGE)
        self.assertEqual(worker_visible_dir, expected_dir)

        planning = single_config(output_dir, "planning")
        lhe_path = planning["lhe_paths"][0]
        self.assertEqual(
            lhe_path, os.path.join(expected_dir, f"sample_{STORAGE}_60100.lhe")
        )
        # _resolve_lhe_path() local branch agrees with the emitted worker dir.
        self.assertEqual(os.path.dirname(lhe_path), worker_visible_dir)
        # This is the assertion that catches _resolve_block_output_dir()'s local
        # branch previously omitting lhe_pools.
        self.assertEqual(
            planning["block_output_dir"],
            os.path.join(expected_dir, "lhe_blocks"),
        )
        self.assertNotIn(
            os.path.join(str(local_base), STORAGE),
            (os.path.dirname(lhe_path), planning["block_output_dir"]),
        )

    # ------------------------------------------------------------------
    # (7) A destination equal to or below an inventory directory is rejected
    # ------------------------------------------------------------------
    def test_generated_destination_below_inventory_is_rejected(self) -> None:
        guard = dag_generator.assert_generated_dir_not_inventory

        # Exact configured inventory directory for this pool.
        with self.assertRaises(ValueError):
            guard(INVENTORY_DIR)
        # Same path with a trailing separator (normalized before comparison).
        with self.assertRaises(ValueError):
            guard(INVENTORY_DIR + "/")
        # Anything below the inventory directory.
        with self.assertRaises(ValueError):
            guard(f"{INVENTORY_DIR}/lhe_blocks")
        with self.assertRaises(ValueError):
            guard(f"{INVENTORY_DIR}/block_000123.lhe.gz/")

        # Equivalent spellings of the same inventory directory must be rejected
        # too. run_helac.sh maps all of these onto the same storage location, so
        # a purely textual comparison would let the overlap through.
        inventory_lfn = "/" + INVENTORY_DIR.split("///", 1)[1]
        for spelling in (
            inventory_lfn,  # bare /store/... LFN
            INVENTORY_DIR.replace(":1094", ""),  # port-less authority
            f"{V45_TARGET}/../MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO",  # traversal
        ):
            with self.assertRaises(ValueError, msg=f"not rejected: {spelling}"):
                guard(spelling)

        # A legitimate v4_5_full generated destination is accepted.
        guard(f"{V45_TARGET}/lhe_pools/{STORAGE}")
        guard(f"{V45_TARGET}/lhe_pools/{STORAGE}/lhe_blocks")
        guard(f"{DEFAULT_BASE}/lhe_pools/{STORAGE}")
        # The v3 default is a sibling of LHE_pool, not an overlap, and the
        # binding decision keeps it working in either spelling.
        guard(f"{dag_generator.EOS_LFN_BASE}/lhe_pools/{STORAGE}")

        # A sibling that merely shares a string prefix is NOT below the
        # inventory directory and must not be rejected.
        prefix_sibling = (
            "root://cceos.ihep.ac.cn:1094///store/user/chiw/"
            "MC_Production_v3_LHE_extra/SPS-JpsiJpsi-LO"
        )
        guard(prefix_sibling)
        guard(f"{INVENTORY_DIR}_extra")
        # Sanity: the sibling really does share a textual prefix.
        self.assertTrue(prefix_sibling.startswith("root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3"))


if __name__ == "__main__":
    unittest.main()
