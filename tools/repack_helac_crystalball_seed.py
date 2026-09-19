#!/usr/bin/env python3
"""
Patch the CrystalBall addon inside the HELAC delivery package and repack it.

The shipped ``addon/pp_psiX_CrystalBall/src/pp_psiX_cb.f90`` never initializes
the HELAC RNG, so every CrystalBall LHE sample starts from the hard-coded
``DSEED`` in ``MC_VEGAS.f90`` and ignores the workflow seed.  The stock
``src/RANDA_init.inc`` is the upstream fix: it reads ``seed.input`` and advances
the Park-Miller stream.  This tool inserts that include at the unique
``INTEGER::beam1_pdf,beam2_pdf`` anchor and repacks the delivery tarball.

The input package is never modified in place.  Build mode writes
``<output-dir>/helac_package.tar.gz`` plus a manifest recording the sha256 of
the unmodified upstream ``src/RANDA_init.inc`` and of every produced artifact.

Usage:
  python3 tools/repack_helac_crystalball_seed.py
  python3 tools/repack_helac_crystalball_seed.py --output-dir /tmp/chiw/helac_cbseed_v1
  python3 tools/repack_helac_crystalball_seed.py --check --package common/packages/helac_package.tar.gz
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

DEFAULT_PACKAGE = "common/packages/helac_package.tar.gz"
DEFAULT_OUTPUT_DIR = "/tmp/chiw/helac_cbseed_v1"
SCRATCH_ROOT = "/tmp/chiw"

OUTER_HELAC_MEMBER = "HELAC-Onia-2.7.6.tar.gz"
OUTER_HEPMC_MEMBER = "hepmc2.06.11.tgz"
HELAC_SOURCE_DIR = "HELAC-Onia-2.7.6"
HELAC_SOURCE_TREE = HELAC_SOURCE_DIR

ANCHOR_RE = re.compile(
    r"^\s*INTEGER\s*::\s*beam1_pdf\s*,\s*beam2_pdf\s*$",
    re.IGNORECASE,
)
INCLUDE_LINE = '    INCLUDE "../../../src/RANDA_init.inc"'

CRYSTALBALL_SRC_REL = os.path.join(
    "addon", "pp_psiX_CrystalBall", "src", "pp_psiX_cb.f90"
)
RANDA_INIT_REL = os.path.join("src", "RANDA_init.inc")

OUTPUT_PACKAGE_NAME = "helac_package.tar.gz"
OUTPUT_MANIFEST_NAME = "helac_package_cbseed.manifest.json"
TOOL_NAME = "tools/repack_helac_crystalball_seed.py"
TOOL_VERSION = 1


class RepackError(Exception):
    """Fatal package-layout or verification error."""


# --------------------------------------------------------------------------
# Hashing helpers
# --------------------------------------------------------------------------


def sha256_file(path: str) -> str:
    """Return the hex sha256 of *path*."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------------------------------
# Tarball helpers
# --------------------------------------------------------------------------


def extract_tar(tar_path: str, dest_dir: str) -> None:
    """Extract *tar_path* under *dest_dir*.

    The package is a trusted local artifact, so member metadata is preserved
    verbatim rather than sanitized.
    """
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            try:
                tar.extractall(dest_dir, filter="fully_trusted")
            except TypeError:  # Python < 3.12 has no extraction filter
                tar.extractall(dest_dir)
    except tarfile.TarError as exc:
        raise RepackError(f"cannot extract {tar_path}: {exc}") from exc


def write_tarball(members: Sequence[Tuple[str, str]], out_path: str) -> None:
    """Write a gzipped tarball from ``(source_path, arcname)`` pairs."""
    tmp_path = out_path + ".partial"
    try:
        with tarfile.open(tmp_path, "w:gz") as tar:
            for source_path, arcname in members:
                if not os.path.exists(source_path):
                    raise RepackError(f"missing tarball member: {source_path}")
                tar.add(source_path, arcname=arcname, recursive=True)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    os.replace(tmp_path, out_path)


def unpack_package(package_path: str, scratch_dir: str) -> Dict[str, str]:
    """Extract the outer package and its nested HELAC tarball.

    Returns a dict with the paths of the outer members and the HELAC source
    tree root.  No source file is modified here.
    """
    outer_dir = os.path.join(scratch_dir, "outer")
    source_dir = os.path.join(scratch_dir, "source")
    os.makedirs(outer_dir)
    os.makedirs(source_dir)
    extract_tar(package_path, outer_dir)

    nested_tar = os.path.join(outer_dir, OUTER_HELAC_MEMBER)
    hepmc_tar = os.path.join(outer_dir, OUTER_HEPMC_MEMBER)
    if not os.path.isfile(nested_tar):
        raise RepackError(
            f"{package_path} has no top-level member {OUTER_HELAC_MEMBER}"
        )
    if not os.path.isfile(hepmc_tar):
        raise RepackError(
            f"{package_path} has no top-level member {OUTER_HEPMC_MEMBER}"
        )
    extract_tar(nested_tar, source_dir)

    tree_root = os.path.join(source_dir, HELAC_SOURCE_TREE)
    if not os.path.isdir(tree_root):
        raise RepackError(
            f"{OUTER_HELAC_MEMBER} has no top-level directory {HELAC_SOURCE_TREE}"
        )
    return {
        "outer_dir": outer_dir,
        "nested_tar": nested_tar,
        "hepmc_tar": hepmc_tar,
        "tree_root": tree_root,
    }


# --------------------------------------------------------------------------
# Patch helpers
# --------------------------------------------------------------------------


def read_lines(path: str) -> List[str]:
    """Read *path* as text, preserving original line endings."""
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return handle.readlines()


def find_anchor_indices(lines: Sequence[str]) -> List[int]:
    """Return the indices of lines matching the CrystalBall declaration anchor."""
    return [i for i, line in enumerate(lines) if ANCHOR_RE.match(_strip_eol(line))]


def _strip_eol(line: str) -> str:
    return line.rstrip("\r\n")


def patch_source(tree_root: str) -> Dict[str, object]:
    """Insert the RANDA_init include into the CrystalBall addon source.

    Idempotent: if the include already sits immediately after the anchor the
    file is left untouched.
    """
    src_path = os.path.join(tree_root, CRYSTALBALL_SRC_REL)
    if not os.path.isfile(src_path):
        raise RepackError(f"CrystalBall source not found: {src_path}")

    lines = read_lines(src_path)
    anchors = find_anchor_indices(lines)
    if len(anchors) != 1:
        raise ValueError(
            f"expected exactly 1 anchor line matching "
            f"'INTEGER::beam1_pdf,beam2_pdf' in {src_path}, found {len(anchors)}"
        )

    anchor_idx = anchors[0]
    is_immediately_after = (
        anchor_idx + 1 < len(lines)
        and _strip_eol(lines[anchor_idx + 1]).strip() == INCLUDE_LINE.strip()
    )
    if is_immediately_after:
        return {
            "inserted": False,
            "anchor_line": anchor_idx + 1,
            "include_line": anchor_idx + 2,
        }

    elsewhere = [
        i
        for i, line in enumerate(lines)
        if _strip_eol(line).strip() == INCLUDE_LINE.strip()
    ]
    if elsewhere:
        raise ValueError(
            f"RANDA_init include already present in {src_path} at line(s) "
            f"{[i + 1 for i in elsewhere]} but not immediately after the anchor "
            f"line {anchor_idx + 1}; refusing to double-insert"
        )

    eol = "\r\n" if lines[anchor_idx].endswith("\r\n") else "\n"
    lines.insert(anchor_idx + 1, INCLUDE_LINE + eol)
    with open(src_path, "w", encoding="utf-8", newline="") as handle:
        handle.writelines(lines)
    return {
        "inserted": True,
        "anchor_line": anchor_idx + 1,
        "include_line": anchor_idx + 2,
    }


def verify_source(tree_root: str) -> Dict[str, object]:
    """Check the anchor count and the include placement without modifying files."""
    src_path = os.path.join(tree_root, CRYSTALBALL_SRC_REL)
    if not os.path.isfile(src_path):
        raise RepackError(f"CrystalBall source not found: {src_path}")

    lines = read_lines(src_path)
    anchors = find_anchor_indices(lines)
    include_present = False
    include_after_anchor = False
    include_line_no: Optional[int] = None

    for i, line in enumerate(lines):
        if _strip_eol(line).strip() == INCLUDE_LINE.strip():
            include_present = True
            include_line_no = i + 1
            break

    if len(anchors) == 1:
        anchor_idx = anchors[0]
        include_after_anchor = (
            anchor_idx + 1 < len(lines)
            and _strip_eol(lines[anchor_idx + 1]).strip() == INCLUDE_LINE.strip()
        )

    return {
        "anchor_count": len(anchors),
        "anchor_line": anchors[0] + 1 if len(anchors) == 1 else None,
        "include_present": include_present,
        "include_immediately_after_anchor": include_after_anchor,
        "include_line": include_line_no,
        "randa_init_sha256": sha256_file(os.path.join(tree_root, RANDA_INIT_REL)),
        "crystalball_src_sha256": sha256_file(src_path),
    }


# --------------------------------------------------------------------------
# Manifest helpers
# --------------------------------------------------------------------------


def manifest_path_for_package(package_path: str) -> str:
    """Locate the manifest recorded alongside *package_path*, if any."""
    candidate = os.path.join(
        os.path.dirname(os.path.abspath(package_path)), OUTPUT_MANIFEST_NAME
    )
    return candidate if os.path.isfile(candidate) else ""


def write_manifest(path: str, manifest: Dict[str, object]) -> None:
    tmp_path = path + ".partial"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp_path, path)


def load_manifest(path: str) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


# --------------------------------------------------------------------------
# Scratch management
# --------------------------------------------------------------------------


def make_scratch_dir() -> str:
    os.makedirs(SCRATCH_ROOT, exist_ok=True)
    return tempfile.mkdtemp(prefix="helac_cbseed_", dir=SCRATCH_ROOT)


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------


def run_build(package_path: str, output_dir: str) -> int:
    """Patch the package source and write the repacked tarball plus manifest."""
    if not os.path.isfile(package_path):
        print(f"ERROR: package not found: {package_path}", file=sys.stderr)
        return 1

    output_package = os.path.join(output_dir, OUTPUT_PACKAGE_NAME)
    if os.path.exists(output_package) and os.path.samefile(
        package_path, output_package
    ):
        print(
            "ERROR: refusing to overwrite the input package in place: "
            f"{package_path}",
            file=sys.stderr,
        )
        return 1

    os.makedirs(output_dir, exist_ok=True)
    scratch_dir = make_scratch_dir()
    try:
        layout = unpack_package(package_path, scratch_dir)
        tree_root = layout["tree_root"]

        # Capture the unmodified upstream checksum before any write.
        upstream_randa_sha = sha256_file(os.path.join(tree_root, RANDA_INIT_REL))

        patch_info = patch_source(tree_root)
        if patch_info["inserted"]:
            print(
                f"patched {CRYSTALBALL_SRC_REL}: inserted RANDA_init include "
                f"after anchor line {patch_info['anchor_line']}"
            )
        else:
            print(
                f"{CRYSTALBALL_SRC_REL}: RANDA_init include already present after "
                f"anchor line {patch_info['anchor_line']}, no insertion needed"
            )

        patched_src_sha = sha256_file(os.path.join(tree_root, CRYSTALBALL_SRC_REL))

        nested_tar = os.path.join(scratch_dir, OUTER_HELAC_MEMBER)
        write_tarball([(tree_root, HELAC_SOURCE_TREE)], nested_tar)
        nested_tar_sha = sha256_file(nested_tar)

        write_tarball(
            [
                (nested_tar, OUTER_HELAC_MEMBER),
                (layout["hepmc_tar"], OUTER_HEPMC_MEMBER),
            ],
            output_package,
        )
        output_package_sha = sha256_file(output_package)

        manifest = {
            "tool": TOOL_NAME,
            "tool_version": TOOL_VERSION,
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "input_package": os.path.abspath(package_path),
            "input_package_sha256": sha256_file(package_path),
            "output_package": os.path.abspath(output_package),
            "patch_anchor": "INTEGER::beam1_pdf,beam2_pdf",
            "patch_include": INCLUDE_LINE.strip(),
            "patch_inserted": patch_info["inserted"],
            "upstream_randa_init_inc_sha256": upstream_randa_sha,
            "patched_pp_psiX_cb_f90_sha256": patched_src_sha,
            "nested_tarball_sha256": nested_tar_sha,
            "outer_package_sha256": output_package_sha,
        }
        manifest_path = os.path.join(output_dir, OUTPUT_MANIFEST_NAME)
        write_manifest(manifest_path, manifest)

        print(f"wrote {output_package}")
        print(f"wrote {manifest_path}")
        print(f"  upstream src/RANDA_init.inc sha256: {upstream_randa_sha}")
        print(f"  patched  {CRYSTALBALL_SRC_REL} sha256: {patched_src_sha}")
        print(f"  nested   {OUTER_HELAC_MEMBER} sha256: {nested_tar_sha}")
        print(f"  outer    {OUTPUT_PACKAGE_NAME} sha256: {output_package_sha}")
    except (RepackError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(scratch_dir, ignore_errors=True)
    return 0


def run_check(package_path: str, manifest_path: str = "") -> int:
    """Verify the packaged CrystalBall source and the recorded checksums."""
    if not os.path.isfile(package_path):
        print(f"ERROR: package not found: {package_path}", file=sys.stderr)
        return 1

    scratch_dir = make_scratch_dir()
    failures: List[str] = []
    try:
        try:
            layout = unpack_package(package_path, scratch_dir)
            info = verify_source(layout["tree_root"])
        except (RepackError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

        if info["anchor_count"] != 1:
            failures.append(
                "anchor line 'INTEGER::beam1_pdf,beam2_pdf' occurs "
                f"{info['anchor_count']} times (expected exactly 1)"
            )
        if not info["include_immediately_after_anchor"]:
            failures.append(
                f"RANDA_init include is not immediately after the anchor line "
                f"(anchor_line={info['anchor_line']}, include_line={info['include_line']})"
            )

        resolved_manifest = manifest_path or manifest_path_for_package(package_path)
        manifest: Optional[Dict[str, object]] = None
        if resolved_manifest:
            try:
                manifest = load_manifest(resolved_manifest)
            except (OSError, ValueError) as exc:
                failures.append(f"cannot read manifest {resolved_manifest}: {exc}")
        else:
            print(
                "WARNING: no sibling manifest found; skipping the recorded "
                "RANDA_init.inc checksum comparison"
            )

        if manifest is not None:
            recorded_randa = manifest.get("upstream_randa_init_inc_sha256")
            if not recorded_randa:
                failures.append(
                    f"manifest {resolved_manifest} has no "
                    "upstream_randa_init_inc_sha256 entry"
                )
            elif recorded_randa != info["randa_init_sha256"]:
                failures.append(
                    "upstream src/RANDA_init.inc sha256 mismatch: recorded "
                    f"{recorded_randa}, found {info['randa_init_sha256']}"
                )
            recorded_src = manifest.get("patched_pp_psiX_cb_f90_sha256")
            if recorded_src and recorded_src != info["crystalball_src_sha256"]:
                failures.append(
                    f"{CRYSTALBALL_SRC_REL} sha256 mismatch: recorded "
                    f"{recorded_src}, found {info['crystalball_src_sha256']}"
                )
            recorded_outer = manifest.get("outer_package_sha256")
            actual_outer = sha256_file(package_path)
            if recorded_outer and recorded_outer != actual_outer:
                failures.append(
                    f"outer package sha256 mismatch: recorded {recorded_outer}, "
                    f"found {actual_outer}"
                )
            recorded_nested = manifest.get("nested_tarball_sha256")
            if recorded_nested:
                actual_nested = sha256_file(layout["nested_tar"])
                if recorded_nested != actual_nested:
                    failures.append(
                        f"nested tarball sha256 mismatch: recorded "
                        f"{recorded_nested}, found {actual_nested}"
                    )

        if failures:
            print(f"CHECK FAILED for {package_path}", file=sys.stderr)
            for failure in failures:
                print(f"  - {failure}", file=sys.stderr)
            return 1

        print(f"CHECK PASSED for {package_path}")
        print(f"  anchor count: {info['anchor_count']} (line {info['anchor_line']})")
        print(f"  include line: {info['include_line']}")
        print(f"  upstream src/RANDA_init.inc sha256: {info['randa_init_sha256']}")
        print(f"  {CRYSTALBALL_SRC_REL} sha256: {info['crystalball_src_sha256']}")
        if manifest is not None:
            print(f"  manifest: {resolved_manifest}")
    finally:
        shutil.rmtree(scratch_dir, ignore_errors=True)
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Insert the stock RANDA_init.inc include into the HELAC-Onia "
            "CrystalBall addon and repack the HELAC delivery package. "
            "The input package is never modified in place."
        )
    )
    parser.add_argument(
        "--package",
        default=DEFAULT_PACKAGE,
        help=f"HELAC delivery tarball (default: {DEFAULT_PACKAGE})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "directory for the repacked package and manifest "
            f"(default: {DEFAULT_OUTPUT_DIR})"
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "verification mode: extract the package and verify the anchor, the "
            "include placement, and the recorded RANDA_init.inc checksum"
        ),
    )
    parser.add_argument(
        "--manifest",
        default="",
        help=(
            "manifest to compare against in --check mode "
            "(default: sibling helac_package_cbseed.manifest.json, if present)"
        ),
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.check:
        return run_check(args.package, args.manifest)
    return run_build(args.package, args.output_dir)


if __name__ == "__main__":
    sys.exit(main())
