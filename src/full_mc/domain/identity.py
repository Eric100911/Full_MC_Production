"""Stable identity helpers copied from the established production algorithms."""
from __future__ import annotations

import hashlib

UINT32_MAX = 2**32 - 1


def deterministic_seed(material: str) -> int:
    """Legacy SHA-256-to-nine-digit seed algorithm; keep it byte-for-byte stable."""
    value = int.from_bytes(hashlib.sha256(material.encode("utf-8")).digest()[:4], "big")
    return value % 900000000 + 1


def shuffle_seed(source_seed: int) -> int:
    """Established LHE shuffler derivation."""
    return int(source_seed) * 1000 + 37


def edm_luminosity_block(job_index: int, block_index: int) -> int:
    """Established Cantor pairing used for unique EDM (run, lumi) identities."""
    if job_index < 0 or block_index < 0:
        raise ValueError("EventID indices must be non-negative")
    diagonal = job_index + block_index
    lumi = diagonal * (diagonal + 1) // 2 + block_index + 1
    if lumi > UINT32_MAX:
        raise OverflowError("Cantor-paired luminosity block exceeds uint32")
    return lumi


def edm_event_id(job_index: int, block_index: int, reserved_events: int) -> dict:
    if reserved_events <= 0:
        raise ValueError("reserved_events must be positive")
    return {"first_run": 1, "first_luminosity_block": edm_luminosity_block(job_index, block_index),
            "first_event": 1, "reserved_events": int(reserved_events),
            "number_events_in_luminosity_block": 0}
