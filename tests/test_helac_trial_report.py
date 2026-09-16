#!/usr/bin/env python3
"""Focused tests for exact HELAC trial-report job identity and provenance."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

import helac_inspector
import helac_trial_report


class HelacTrialReportTest(unittest.TestCase):
    def test_mapping_preserves_process_and_schedd(self):
        mappings = helac_trial_report.parse_mapping(
            [
                "bigbird22.cern.ch:16639181.0=zero.json",
                "bigbird22.cern.ch:16639181.1=two.json",
                "bigbird08.cern.ch:16639181.0=other-schedd.json",
            ],
            "--config",
        )

        self.assertEqual(len(mappings), 3)
        self.assertEqual(
            helac_trial_report.mapped_path(
                mappings, "bigbird22.cern.ch", "16639181.0"
            ),
            Path("zero.json"),
        )
        self.assertEqual(
            helac_trial_report.mapped_path(
                mappings, "bigbird22.cern.ch", "16639181.1"
            ),
            Path("two.json"),
        )
        self.assertEqual(
            helac_trial_report.mapped_path(
                mappings, "bigbird08.cern.ch", "16639181.0"
            ),
            Path("other-schedd.json"),
        )

    @patch("helac_trial_report.remote_snapshot")
    @patch("helac_trial_report.queue_status")
    def test_requested_and_observed_configuration_are_separate(
        self, queue_status, remote_snapshot
    ):
        queue_status.return_value = {"state": "running", "status_code": 2}
        remote_snapshot.return_value = {
            "parameters": {
                "nmc": "500000",
                "unwevt": "100000",
                "minptq": "0.0d0",
                "minmqqp": "0.0d0",
                "minmqbeam": "0.0d0",
                "minptconia": "6.0d0",
                "minptbonia": "4.0d0",
                "ranhel": "91051",
            },
            "runs": [],
            "stage": "calculation-preparing",
        }

        record = helac_trial_report.report_one(
            "bigbird22.cern.ch",
            "16639181.0",
            {
                "nmc": 500000,
                "unwevt": 100000,
                "min_pt_q": 0.0,
                "min_m_qqp": 3.0969,
                "min_m_q_beam": 3.0969,
                "min_pt_conia": 6.0,
                "min_pt_bonia": 4.0,
                "seed": 91051,
            },
            None,
            False,
        )

        self.assertIn("minmqqp=3.0969", record["requested_configuration"])
        self.assertIn("minmqqp=0.0d0", record["observed_configuration"])

    @patch("helac_inspector.subprocess.run")
    def test_remote_snapshot_timeout_is_structured(self, run):
        run.side_effect = helac_inspector.subprocess.TimeoutExpired("ssh", 90)

        snapshot = helac_inspector.remote_snapshot(
            "bigbird22.cern.ch", "16639181.0", ssh_timeout_seconds=90
        )

        self.assertEqual(snapshot["stage"], "ssh-timeout")
        self.assertIn("90 s", snapshot["error"])


if __name__ == "__main__":
    unittest.main()
