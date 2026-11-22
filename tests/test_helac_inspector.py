#!/usr/bin/env python3
"""Unit tests for the dependency-free HELAC progress inspector."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.helac_inspector import parse_root


class HelacInspectorTest(unittest.TestCase):
    def make_tree(self, tmp: str) -> Path:
        root = Path(tmp) / "HELAC-Onia-2.7.6"
        result_dir = root / "PROC_HO_7" / "P0_calc_0" / "output"
        input_dir = root / "PROC_HO_7" / "P0_calc_0" / "input"
        result_dir.mkdir(parents=True)
        input_dir.mkdir()
        (root / "input").mkdir()
        (root / "input" / "user.inp").write_text(
            "minptq 0.8d0\nnmc 20000000\nunwevt 100000\n",
            encoding="utf-8",
        )
        (result_dir / "RESULT_ggpsi1psi1.out").write_text(
            "\n".join(
                (
                    "sigma= 1.0D-01 2.0D-02 10 100 100",
                    "sigma= 1.1D-01 1.0D-02 20 200 200",
                    "total sigma (nb) = 1.1D-01 +/- 1.1D-03",
                    "% error: 1.0D+00",
                )
            )
            + "\n",
            encoding="utf-8",
        )
        (root / "PROC_HO_7" / "sample.lhe").write_text(
            "<event>\n1\n</event>\n<event>\n2\n</event>\n",
            encoding="utf-8",
        )
        return root

    def test_parses_iterations_final_result_and_parameters(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = parse_root(self.make_tree(tmp))

        result = summary["runs"][0]["calculations"][0]["result_files"][0]
        self.assertEqual(summary["stage"], "calculation-complete")
        self.assertEqual(result["iteration_count"], 2)
        self.assertEqual(result["last"]["points_used"], 200)
        self.assertEqual(result["final_sigma_nb"], 0.11)
        self.assertEqual(result["poisson_equivalent_events"], 10000)
        self.assertEqual(summary["parameters"]["minptq"], "0.8d0")
        self.assertIsNone(summary["observed_lhe_events"])
        self.assertFalse(summary["lhe_event_scan_performed"])

    def test_event_count_is_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = parse_root(self.make_tree(tmp), count_lhe_events=True)

        self.assertEqual(summary["observed_lhe_events"], 2)
        self.assertTrue(summary["lhe_event_scan_performed"])
        result = summary["runs"][0]["calculations"][0]["result_files"][0]
        self.assertEqual(result["additional_poisson_equivalent_events"], 9998)

    def test_event_counts_stay_with_their_helac_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.make_tree(tmp)
            second_result = root / "PROC_HO_8" / "P0_calc_0" / "output"
            second_result.mkdir(parents=True)
            (second_result / "RESULT_second.out").write_text(
                "sigma= 2.0D-01 1.0D-02 20 200 200\n", encoding="utf-8"
            )
            (root / "PROC_HO_8" / "second.lhe").write_text(
                "<event>\n1\n</event>\n", encoding="utf-8"
            )
            summary = parse_root(root, count_lhe_events=True)

        self.assertEqual(summary["observed_lhe_events"], 3)
        self.assertEqual(summary["observed_lhe_events_by_run"], {
            "PROC_HO_7": 2,
            "PROC_HO_8": 1,
        })
        results = {
            item["path"]: item
            for run in summary["runs"]
            for calculation in run["calculations"]
            for item in calculation["result_files"]
        }
        self.assertEqual(
            results["PROC_HO_7/P0_calc_0/output/RESULT_ggpsi1psi1.out"]
            ["observed_lhe_events"],
            2,
        )
        self.assertEqual(
            results["PROC_HO_8/P0_calc_0/output/RESULT_second.out"]
            ["observed_lhe_events"],
            1,
        )

    def test_cli_emits_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.make_tree(tmp)
            completed = subprocess.run(
                [
                    sys.executable,
                    "tools/helac_inspector.py",
                    "parse",
                    "--root",
                    str(root),
                ],
                check=True,
                capture_output=True,
                text=True,
            )

        self.assertEqual(json.loads(completed.stdout)["result_count"], 1)


if __name__ == "__main__":
    unittest.main()
