#!/usr/bin/env python3
"""Regression tests for the CrystalBall HELAC seed fix (offline, no tarball).

Two artifacts must stay in lockstep:

1. ``tools/repack_helac_crystalball_seed.py`` inserts the stock upstream
   ``src/RANDA_init.inc`` include immediately after the *unique*
   ``INTEGER::beam1_pdf,beam2_pdf`` declaration of the shipped CrystalBall
   addon source ``addon/pp_psiX_CrystalBall/src/pp_psiX_cb.f90``, then repacks
   the delivery tarball.
2. ``lhe_generation/run_helac.sh`` writes
   ``addon/pp_psiX_CrystalBall/input/seed.input`` from ``MY_SEED`` inside
   ``prepare_crystalball_addon_input()``; that is exactly the file the include
   reads (``OPEN(777,FILE=TRIM(input_dir)//'seed.input',status='OLD')``).

The tests are fast and fully offline. They never open the 57 MB delivery
tarball (``common/packages/helac_package.tar.gz``); instead the fixture below
reproduces the shipped declaration block verbatim. Frozen identity of the
shipped source this fixture mirrors:

    file:   addon/pp_psiX_CrystalBall/src/pp_psiX_cb.f90
    sha256: 83cbf58f919dd0360802c73790f5d2ebb36bf62a1e306a4e4e68b96f35093fd9
    anchor: exactly 1 occurrence, line 37
    src/RANDA_init.inc sha256:
            820898dea874531aaf19ae2294749a07b30b7113359029ff47a31e21dfb61ade
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
TMP_ROOT = Path("/tmp/chiw")

sys.path.insert(0, str(BASE_DIR))

import tools.repack_helac_crystalball_seed as repack  # noqa: E402


# --------------------------------------------------------------------------
# Fixtures: verbatim shipped source text (no tarball access)
# --------------------------------------------------------------------------

# Declaration block of the shipped pp_psiX_cb.f90 (shipped lines 28-38).
SHIPPED_SRC_PREFIX = [
    "      SUBROUTINE calc_pp_psiX_cb",
    "      IMPLICIT NONE",
    '      INCLUDE "../../../src/ho_common.inc"',
]

SHIPPED_DECLARATION_BLOCK = [
    "    INTEGER::nh,innum,outnum",
    "    REAL(KIND(1d0))::w0,ptp,pqp,wme",
    "    INTEGER::ioerror,icase=0",
    "    LOGICAL::lunwei=.FALSE.,lhewgt=.FALSE.",
    "    REAL(KIND(1d0)),DIMENSION(3)::rslt",
    "    INTEGER::itmxn,ncalln",
    "    REAL(KIND(1d0)),DIMENSION(4)::pmomtemp",
    "    LOGICAL::lexist",
    "    SAVE lunwei,lhewgt,icase",
    "    INTEGER::beam1_pdf,beam2_pdf",  # the anchor
    "    WRITE(*,*)'                     THE BEGINNING OF HELAC-Onia'",
]

SHIPPED_SRC_SUFFIX = [
    "    CALL ReadElem_integer('colpar',Coll_Type)",
    "    CALL ReadElem_logic('unwgt',lunwei)",
    "      END",
]

SHIPPED_SRC_LINES = (
    SHIPPED_SRC_PREFIX + SHIPPED_DECLARATION_BLOCK + SHIPPED_SRC_SUFFIX
)

# Index of the anchor line inside SHIPPED_SRC_LINES (0-based).
SHIPPED_ANCHOR_INDEX = len(SHIPPED_SRC_PREFIX) + SHIPPED_DECLARATION_BLOCK.index(
    "    INTEGER::beam1_pdf,beam2_pdf"
)

# Verbatim shipped src/RANDA_init.inc (the file the include pulls in).  The
# reader hard-codes 'seed.input', which the run_helac.sh bridge must produce.
SHIPPED_RANDA_INIT = (
    "INTEGER::iseed,ililil\n"
    "REAL(KIND(1d0)),DIMENSION(1)::ranrrr\n"
    "! read the random seed\n"
    "OPEN(777,FILE=TRIM(input_dir)//'seed.input',status='OLD')\n"
    "READ(777,*)iseed\n"
    "WRITE(*,*)'Random number seed',iseed\n"
    "CLOSE(777)\n"
    "! randomly RANDA\n"
    "IF(iseed.GT.0)THEN\n"
    "   DO ililil=1,iseed\n"
    "      CALL RANDA(1,ranrrr)\n"
    "   ENDDO\n"
    "ENDIF"
)

RUN_HELAC_SH = BASE_DIR / "lhe_generation" / "run_helac.sh"
SEED_INPUT_WRITE = (
    "printf '%s\\n' \"${MY_SEED}\" > \"${addon_dir}/input/seed.input\""
)
SEED_INPUT_MSG = 'msg_info "CrystalBall RANDA skip seed: ${MY_SEED}"'


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_tree(
    tmp: Path,
    src_lines=None,
    include_text: str = SHIPPED_RANDA_INIT,
    include_after_anchor_offset=None,
) -> Path:
    """Build a minimal HELAC tree holding only the files the tool touches."""
    tree_root = tmp / "HELAC-Onia-2.7.6"
    src_path = tree_root / repack.CRYSTALBALL_SRC_REL
    src_path.parent.mkdir(parents=True, exist_ok=True)

    lines = list(SHIPPED_SRC_LINES if src_lines is None else src_lines)
    if include_after_anchor_offset is not None:
        lines.insert(
            SHIPPED_ANCHOR_INDEX + include_after_anchor_offset,
            repack.INCLUDE_LINE,
        )
    src_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    randa_path = tree_root / repack.RANDA_INIT_REL
    randa_path.parent.mkdir(parents=True, exist_ok=True)
    randa_path.write_text(include_text, encoding="utf-8")
    return tree_root


def function_body(script: str, name: str) -> str:
    """Return the body of a top-level shell function ``name() { ... }``."""
    match = re.search(rf"^{re.escape(name)}\(\) \{{\n", script, re.MULTILINE)
    if match is None:
        raise AssertionError(f"function {name}() not found in {RUN_HELAC_SH}")
    start = match.end()
    end = script.find("\n}\n", start)
    if end == -1:
        raise AssertionError(f"unterminated function {name}() in {RUN_HELAC_SH}")
    return script[start:end]


# --------------------------------------------------------------------------
# 1. Anchor contract on the shipped declaration block
# --------------------------------------------------------------------------


class ShippedAnchorTest(unittest.TestCase):
    def test_shipped_anchor_matches_exactly_once(self):
        anchors = repack.find_anchor_indices(SHIPPED_SRC_LINES)
        self.assertEqual(
            anchors,
            [SHIPPED_ANCHOR_INDEX],
            "the shipped CrystalBall declaration anchor must occur exactly once",
        )
        self.assertEqual(
            SHIPPED_SRC_LINES[anchors[0]].strip(), "INTEGER::beam1_pdf,beam2_pdf"
        )
        # The declaration itself is unique in the shipped block, so a patch can
        # never be applied at an unintended site.
        self.assertEqual(
            "\n".join(SHIPPED_SRC_LINES).count("INTEGER::beam1_pdf,beam2_pdf"), 1
        )

    def test_anchor_regex_is_line_anchored(self):
        for lookalike in (
            "! INTEGER::beam1_pdf,beam2_pdf",
            "    INTEGER::beam1_pdf,beam2_pdf ! trailing comment",
            "    !INTEGER::beam1_pdf,beam2_pdf",
            "    INTEGER::beam1_pdf",
        ):
            self.assertIsNone(
                repack.ANCHOR_RE.match(lookalike),
                f"anchor regex must not match {lookalike!r}",
            )

    def test_anchor_regex_tolerates_spacing_and_case(self):
        self.assertIsNotNone(repack.ANCHOR_RE.match("INTEGER :: beam1_pdf ,beam2_pdf"))
        self.assertIsNotNone(repack.ANCHOR_RE.match("integer::beam1_pdf,beam2_pdf"))

    def test_include_line_present_is_unique(self):
        self.assertEqual(
            sum(
                1
                for line in SHIPPED_SRC_LINES
                if line.strip() == repack.INCLUDE_LINE.strip()
            ),
            0,
            "the shipped source must not already carry the RANDA_init include",
        )


# --------------------------------------------------------------------------
# 2. Insert / verify helpers
# --------------------------------------------------------------------------


class PatchSourceTest(unittest.TestCase):
    def setUp(self):
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.assert_dir = tempfile.TemporaryDirectory(
            prefix="cb_repack_test_", dir=str(TMP_ROOT)
        )
        self.tmp = Path(self.assert_dir.name)

    def tearDown(self):
        self.assert_dir.cleanup()

    def test_insert_places_include_immediately_after_anchor(self):
        tree_root = make_tree(self.tmp)
        src_path = tree_root / repack.CRYSTALBALL_SRC_REL

        info = repack.patch_source(str(tree_root))

        self.assertTrue(info["inserted"])
        self.assertEqual(info["anchor_line"], SHIPPED_ANCHOR_INDEX + 1)
        self.assertEqual(info["include_line"], SHIPPED_ANCHOR_INDEX + 2)

        patched = src_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(
            patched[SHIPPED_ANCHOR_INDEX], "    INTEGER::beam1_pdf,beam2_pdf"
        )
        self.assertEqual(patched[SHIPPED_ANCHOR_INDEX + 1], repack.INCLUDE_LINE)
        # Nothing else moved: the block before and after is byte-identical.
        self.assertEqual(patched[: SHIPPED_ANCHOR_INDEX + 1], SHIPPED_SRC_LINES[: SHIPPED_ANCHOR_INDEX + 1])
        self.assertEqual(
            patched[SHIPPED_ANCHOR_INDEX + 2 :], SHIPPED_SRC_LINES[SHIPPED_ANCHOR_INDEX + 1 :]
        )

    def test_include_line_text_and_path_resolution(self):
        self.assertEqual(
            repack.INCLUDE_LINE.strip(),
            'INCLUDE "../../../src/RANDA_init.inc"',
        )
        rel = re.search(r'"([^"]+)"', repack.INCLUDE_LINE).group(1)
        resolved = os.path.normpath(
            os.path.join(os.path.dirname(repack.CRYSTALBALL_SRC_REL), rel)
        )
        self.assertEqual(resolved, os.path.normpath(repack.RANDA_INIT_REL))

    def test_patch_is_idempotent(self):
        tree_root = make_tree(self.tmp)
        src_path = tree_root / repack.CRYSTALBALL_SRC_REL

        first = repack.patch_source(str(tree_root))
        digest_after_first = sha256_path(src_path)
        second = repack.patch_source(str(tree_root))

        self.assertTrue(first["inserted"])
        self.assertFalse(second["inserted"])
        self.assertEqual(second["anchor_line"], first["anchor_line"])
        self.assertEqual(second["include_line"], first["include_line"])
        self.assertEqual(sha256_path(src_path), digest_after_first)
        self.assertEqual(
            src_path.read_text(encoding="utf-8").count(repack.INCLUDE_LINE), 1
        )

    def test_refuses_double_insert_when_include_is_displaced(self):
        # Include present but not immediately after the anchor -> refuse.
        tree_root = make_tree(self.tmp, include_after_anchor_offset=3)
        with self.assertRaises(ValueError) as ctx:
            repack.patch_source(str(tree_root))
        self.assertIn("refusing to double-insert", str(ctx.exception))

    def test_refuses_missing_or_duplicated_anchor(self):
        for lines, expected in (
            (
                [l for l in SHIPPED_SRC_LINES if "beam1_pdf,beam2_pdf" not in l],
                0,
            ),
            (SHIPPED_SRC_LINES + ["    INTEGER::beam1_pdf,beam2_pdf"], 2),
        ):
            tree_root = make_tree(self.tmp, src_lines=lines)
            with self.assertRaises(ValueError) as ctx:
                repack.patch_source(str(tree_root))
            self.assertIn(f"found {expected}", str(ctx.exception))

    def test_verify_source_reports_anchor_and_include(self):
        tree_root = make_tree(self.tmp)
        src_path = tree_root / repack.CRYSTALBALL_SRC_REL

        before = repack.verify_source(str(tree_root))
        self.assertEqual(before["anchor_count"], 1)
        self.assertFalse(before["include_present"])
        self.assertFalse(before["include_immediately_after_anchor"])
        self.assertEqual(
            before["randa_init_sha256"],
            sha256_text(SHIPPED_RANDA_INIT),
        )

        repack.patch_source(str(tree_root))
        after = repack.verify_source(str(tree_root))
        self.assertEqual(after["anchor_count"], 1)
        self.assertTrue(after["include_present"])
        self.assertTrue(after["include_immediately_after_anchor"])
        self.assertEqual(after["include_line"], after["anchor_line"] + 1)
        self.assertEqual(
            after["crystalball_src_sha256"], sha256_path(src_path)
        )
        self.assertNotEqual(
            after["crystalball_src_sha256"], before["crystalball_src_sha256"]
        )

    def test_randa_init_include_reads_seed_input(self):
        # The bridge artifact and the include's expected input must agree.
        self.assertIn("seed.input", SHIPPED_RANDA_INIT)


# --------------------------------------------------------------------------
# 3. run_helac.sh seed-file bridge
# --------------------------------------------------------------------------


class RunHelacSeedBridgeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script = RUN_HELAC_SH.read_text(encoding="utf-8")
        cls.body = function_body(cls.script, "prepare_crystalball_addon_input")

    def test_bridge_function_is_defined_once(self):
        self.assertEqual(
            len(re.findall(r"^prepare_crystalball_addon_input\(\) \{", self.script, re.M)),
            1,
        )

    def test_bridge_writes_seed_input_from_my_seed(self):
        self.assertIn(SEED_INPUT_WRITE, self.body)
        self.assertEqual(
            self.script.count(SEED_INPUT_WRITE),
            1,
            "the seed.input bridge must live only inside "
            "prepare_crystalball_addon_input()",
        )

    def test_bridge_reports_the_skip_seed(self):
        self.assertIn(SEED_INPUT_MSG, self.body)
        self.assertLess(
            self.body.index(SEED_INPUT_WRITE),
            self.body.index(SEED_INPUT_MSG),
            "the write must precede its status message",
        )

    def test_bridge_targets_the_addon_input_directory(self):
        self.assertIn('local addon_dir="${helac_dir}/addon/pp_psiX_CrystalBall"', self.body)
        # The bridge writes into the same addon tree whose src/ the repack tool
        # patches: <addon_dir>/input/seed.input next to src/pp_psiX_cb.f90.
        addon_dir = Path(repack.CRYSTALBALL_SRC_REL).parents[1]
        self.assertEqual(addon_dir.as_posix(), "addon/pp_psiX_CrystalBall")
        self.assertEqual(
            addon_dir.joinpath("input", "seed.input").as_posix(),
            "addon/pp_psiX_CrystalBall/input/seed.input",
        )

    def test_seed_value_is_positive_before_the_bridge(self):
        # RANDA_init.inc only advances the stream when iseed > 0.
        self.assertIn("IF(iseed.GT.0)THEN", SHIPPED_RANDA_INIT)
        default = re.search(r"^MY_SEED=(\d+)$", self.script, re.M)
        self.assertIsNotNone(default)
        self.assertGreater(int(default.group(1)), 0)
        self.assertIn('if [ "$MY_SEED" -le 10 ]', self.script)
        self.assertIn('if ! [[ "$MY_SEED" =~ ^[0-9]+$ ]]', self.script)


if __name__ == "__main__":
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    unittest.main()
