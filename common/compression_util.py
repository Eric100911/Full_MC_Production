#!/usr/bin/env python3
"""
Compression-aware file handling for LHE and HepMC files.

Provides suffix detection, atomic gzip/gunzip, and extension-acceptance
predicates used by dag_generator, hepjob_workflow, and utility scripts.
"""

from __future__ import annotations

import gzip
import os
import shutil
import tempfile
from typing import Optional


# ── Extension predicates ─────────────────────────────────────────────────────

def accepts_lhe_ext(name: str) -> bool:
    """Return True if *name* ends with .lhe or .lhe.gz."""
    return name.endswith((".lhe", ".lhe.gz"))


def accepts_hepmc_ext(name: str) -> bool:
    """Return True if *name* ends with .hepmc or .hepmc.gz."""
    return name.endswith((".hepmc", ".hepmc.gz"))


def is_gzip_file(path: str) -> bool:
    """Check gzip magic bytes (1f 8b)."""
    try:
        with open(path, "rb") as fh:
            return fh.read(2) == b"\x1f\x8b"
    except OSError:
        return False


def strip_gz_suffix(path: str) -> str:
    """Return *path* with a single trailing .gz removed, if present."""
    if path.endswith(".gz"):
        return path[:-3]
    return path


# ── Atomic compress / decompress ─────────────────────────────────────────────

def gzip_file_atomic(
    src: str,
    dst: Optional[str] = None,
    level: int = 1,
    force: bool = False,
) -> Optional[str]:
    """Compress *src* with gzip, writing atomically via temp + rename.

    Args:
        src: Path to the uncompressed input file.
        dst: Destination path (default: ``src + ".gz"``).
        level: gzip compression level (1-9, default 1).
        force: If False and *dst* already exists, skip and return None.

    Returns:
        Path to the compressed file, or None if skipped.
    """
    if dst is None:
        dst = src + ".gz"

    if os.path.exists(dst) and not force:
        return None

    dst_tmp = dst + ".tmp"
    try:
        with open(src, "rb") as f_in:
            with gzip.open(dst_tmp, "wb", compresslevel=level) as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.rename(dst_tmp, dst)
    except Exception:
        if os.path.exists(dst_tmp):
            os.remove(dst_tmp)
        raise

    shutil.copymode(src, dst)
    return dst


def gunzip_file_atomic(src: str, dst: Optional[str] = None) -> str:
    """Decompress a .gz file atomically via temp + rename.

    Args:
        src: Path to the .gz input file.
        dst: Destination path (default: *src* with .gz suffix stripped).

    Returns:
        Path to the decompressed file.
    """
    if dst is None:
        dst = strip_gz_suffix(src)

    dst_tmp = dst + ".tmp"
    try:
        with gzip.open(src, "rb") as f_in:
            with open(dst_tmp, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.rename(dst_tmp, dst)
    except Exception:
        if os.path.exists(dst_tmp):
            os.remove(dst_tmp)
        raise

    return dst


def safe_decompress_to_scratch(src: str, scratch_dir: str, label: str = "input") -> str:
    """Decompress *src* into *scratch_dir* if gzipped; otherwise return *src*.

    Returns the path to the plain (decompressed) file.
    """
    if not src.endswith(".gz"):
        return src

    plain_name = os.path.basename(strip_gz_suffix(src))
    dst = os.path.join(scratch_dir, f"{label}_{plain_name}")
    if os.path.exists(dst):
        return dst

    return gunzip_file_atomic(src, dst)
