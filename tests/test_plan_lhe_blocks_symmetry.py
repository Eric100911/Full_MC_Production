#!/usr/bin/env python3
"""Regression tests for fail-closed LHE longitudinal-symmetry QA."""

import json
import subprocess
import sys
import tempfile
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
TMP_ROOT = Path("/tmp/chiw")
sys.path.insert(0, str(BASE_DIR / "tools"))

import plan_lhe_blocks  # noqa: E402


def write_lhe(path: Path, pdg_id: int, pz_values) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write('<LesHouchesEvents version="1.0">\n<init>\n</init>\n')
        for pz in pz_values:
            energy = abs(pz) + 3.2
            handle.write("<event>\n")
            handle.write("1 1 1.0 1.0 1.0 1.0\n")
            handle.write(
                f"{pdg_id} 1 0 0 0 0 0.0 0.0 {pz:.6f} "
                f"{energy:.6f} 3.1 0.0 9.0\n"
            )
            handle.write("</event>\n")
        handle.write("</LesHouchesEvents>\n")


def main() -> int:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lhe_symmetry_test_", dir=TMP_ROOT) as tmp:
        tmp_path = Path(tmp)
        balanced = tmp_path / "balanced.lhe"
        positive_only = tmp_path / "positive_only.lhe"
        irrelevant = tmp_path / "irrelevant.lhe"
        small = tmp_path / "small.lhe"
        write_lhe(balanced, 443, [1.0, -1.0] * 300)
        write_lhe(positive_only, 9941003, [1.0] * 600)
        write_lhe(irrelevant, 21, [1.0] * 600)
        write_lhe(small, 443, [1.0] * 100)

        balanced_report = plan_lhe_blocks.audit_input_longitudinal_symmetry(
            [str(balanced)], ["balanced-source"], [443, 9941003], "fail", 0.20, 500
        )
        assert balanced_report["result"] == "pass"
        assert balanced_report["files"][0]["absolute_asymmetry"] == 0.0

        positive_report = plan_lhe_blocks.audit_input_longitudinal_symmetry(
            [str(positive_only)], ["positive-source"], [443, 9941003], "fail", 0.20, 500
        )
        assert positive_report["result"] == "fail"
        assert positive_report["files"][0]["absolute_asymmetry"] == 1.0
        assert positive_report["files"][0]["reason"] == "asymmetry_exceeds_limit"

        mixed_report = plan_lhe_blocks.audit_input_longitudinal_symmetry(
            [str(balanced), str(positive_only)],
            ["balanced-source", "positive-source"],
            [443, 9941003],
            "fail",
            0.20,
            500,
        )
        assert mixed_report["result"] == "fail"
        assert [item["result"] for item in mixed_report["files"]] == ["pass", "fail"]

        irrelevant_report = plan_lhe_blocks.audit_input_longitudinal_symmetry(
            [str(irrelevant)], ["irrelevant-source"], [443], "fail", 0.20, 500
        )
        assert irrelevant_report["result"] == "fail"
        assert irrelevant_report["files"][0]["reason"] == "expected_particles_missing"

        small_report = plan_lhe_blocks.audit_input_longitudinal_symmetry(
            [str(small)], ["small-source"], [443], "fail", 0.20, 500
        )
        assert small_report["result"] == "insufficient_statistics"
        assert small_report["files"][0]["reason"] == "below_minimum_particles"

        manifest = tmp_path / "plan.json"
        result = subprocess.run(
            [
                "python3",
                str(BASE_DIR / "tools" / "plan_lhe_blocks.py"),
                "--pool-name", "pool_jpsi_CSCO_g",
                "--primary-seed", "1322",
                "--lhe-path", str(positive_only),
                "--output-dir", str(tmp_path / "manifests"),
                "--events-per-block", "100",
                "--shuffle-seed", "1322037",
                "--block-output-dir", str(tmp_path / "blocks"),
                "--manifest-output-path", str(manifest),
                "--lhe-shuffle-split-bin", "/bin/false",
                "--longitudinal-symmetry-pdg-ids", "443,9941003",
                "--longitudinal-symmetry-policy", "fail",
                "--longitudinal-symmetry-max-asymmetry", "0.20",
                "--longitudinal-symmetry-min-particles", "500",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=False,
        )
        assert result.returncode == 1
        assert "failed the longitudinal-symmetry audit" in result.stderr
        assert "lhe_shuffle_split failed" not in result.stderr
        audit_path = Path(str(manifest) + ".input_symmetry.json")
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        assert audit["result"] == "fail"
        assert audit["files"][0]["positive_pz"] == 600
        assert not manifest.exists()

    print("[OK] planner rejects one-sided quarkonium LHE inputs before splitting")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
