#!/usr/bin/env python3
"""Static verification of GEN-SIM vertex smearing configuration.

Reads common/cmssw_configs/hepmc_to_GENSIM.py as plain text and
asserts that every critical vertex-smearing-related assignment is
present.  Intentionally avoids importing the file as a CMSSW config
so it can run with plain python3 (no CVMFS / scram environment).
"""

from __future__ import annotations

import re
from pathlib import Path

# Relative path from repo root to the config file under test.
_CONFIG_PATH = Path("common", "cmssw_configs", "hepmc_to_GENSIM.py")

# ---------------------------------------------------------------------------
# Pre-compiled regex patterns -- one per vertex-smearing assignment
# ---------------------------------------------------------------------------

# 1. VtxSmearedCommon.src = cms.InputTag("source", "generator")
_RE_SRC = re.compile(
    r"VtxSmearedCommon\.src\s*=\s*cms\.InputTag\([\"']source[\"']\s*,\s*[\"']generator[\"']\)"
)

# 2. process.VtxSmeared = cms.EDProducer("BetafuncEvtVtxGenerator", ...)
#    Spans multiple lines -- DOTALL lets . match newlines.
_RE_EDPRODUCER = re.compile(
    r"process\.VtxSmeared\s*=\s*cms\.EDProducer\([\"']BetafuncEvtVtxGenerator[\"']"
    r"\s*,\s*Realistic25ns13p6TeVEarly2022CollisionVtxSmearingParameters"
    r"\s*,\s*VtxSmearedCommon\s*\)",
    re.DOTALL,
)

# 3a. process.g4SimHits.HepMCProductLabel = cms.InputTag("generatorSmeared")
_RE_G4SIMHITS_A = re.compile(
    r"process\.g4SimHits\.HepMCProductLabel\s*=\s*cms\.InputTag\([\"']generatorSmeared[\"']\)"
)

# 3b. process.g4SimHits.Generator.HepMCProductLabel = cms.InputTag("generatorSmeared")
_RE_G4SIMHITS_B = re.compile(
    r"process\.g4SimHits\.Generator\.HepMCProductLabel\s*=\s*cms\.InputTag\([\"']generatorSmeared[\"']\)"
)

# 4. process.genParticles.src = cms.InputTag("generatorSmeared")
_RE_GENPARTICLES = re.compile(
    r"process\.genParticles\.src\s*=\s*cms\.InputTag\([\"']generatorSmeared[\"']\)"
)


def find_repo_root() -> Path:
    """Return the repository root directory (parent of tools/)."""
    return Path(__file__).resolve().parent.parent


def main() -> int:
    repo_root = find_repo_root()
    config_file = repo_root / _CONFIG_PATH

    if not config_file.is_file():
        print(f"[ERROR] Config file not found: {config_file}")
        return 1

    try:
        content = config_file.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"[ERROR] Cannot read config file {config_file}: {exc}")
        return 1

    checks = [
        (
            'VtxSmearedCommon.src = cms.InputTag("source", "generator")',
            lambda text: bool(_RE_SRC.search(text)),
        ),
        (
            'process.VtxSmeared = cms.EDProducer("BetafuncEvtVtxGenerator", ...)',
            lambda text: bool(_RE_EDPRODUCER.search(text)),
        ),
        (
            "process.g4SimHits.HepMCProductLabel = cms.InputTag('generatorSmeared')",
            lambda text: bool(_RE_G4SIMHITS_A.search(text)),
        ),
        (
            "process.g4SimHits.Generator.HepMCProductLabel = cms.InputTag('generatorSmeared')",
            lambda text: bool(_RE_G4SIMHITS_B.search(text)),
        ),
        (
            "process.genParticles.src = cms.InputTag('generatorSmeared')",
            lambda text: bool(_RE_GENPARTICLES.search(text)),
        ),
    ]

    failures = 0
    for label, func in checks:
        if func(content):
            print(f"[OK] {label}")
        else:
            print(f"[ERROR] {label}")
            failures += 1

    if failures:
        print(f"[INFO] {failures} vertex-smearing check(s) FAILED.")
        return 1

    print("[INFO] All vertex-smearing checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
