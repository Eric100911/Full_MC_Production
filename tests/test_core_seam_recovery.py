"""Completion is terminal, and recovery entry points must not undo it.

Two windows sit outside the fold's crash-safety contract because they happen
after the last durable write:

* the gap between the completion state commit and the ``.complete`` sentinel,
  which a rescue or a manual ``internal advance-wave`` can re-enter;
* handoff export pointed at a run whose state predates this refactor.

Re-deriving leftover work in either place silently undoes or destroys a
completed run's records, so both must short-circuit or fail closed instead.
"""
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from full_mc.application.handoff import export_handoff
from full_mc.application.production import advance_wave
from full_mc.io import atomic_json, read_json
from tests.test_core_seam_acceptance import AcceptanceFixture


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class CompletedRunIsTerminal(AcceptanceFixture):

    def completed_run(self, base):
        run = self.drive(base)
        self.assertEqual(read_json(run / "state.json")["status"], "complete")
        self.assertTrue((run / ".complete").is_file())
        return run

    def test_losing_the_sentinel_recreates_it_without_reopening_the_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.completed_run(Path(temporary) / "run")
            before = read_json(run / "state.json")
            tasks_before = sorted(p.name for p in (run / "tasks").glob("RECLAIM_*.json"))
            dags_before = sorted(p.name for p in (run / "steps").glob("*.dag"))

            # Model a crash in the window between the completion commit and the
            # sentinel write.
            (run / ".complete").unlink()

            replayed = advance_wave(run)

            self.assertTrue(replayed.get("skip"))
            self.assertEqual(replayed["status"], "complete")
            self.assertTrue((run / ".complete").is_file())
            self.assertEqual(read_json(run / "state.json"), before)
            # No second terminal reclaim was derived from already-reclaimed refs.
            self.assertEqual(sorted(p.name for p in (run / "tasks").glob("RECLAIM_*.json")),
                             tasks_before)
            self.assertEqual(sorted(p.name for p in (run / "steps").glob("*.dag")), dags_before)

    def test_repeated_recovery_invocations_stay_complete(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.completed_run(Path(temporary) / "run")
            before = read_json(run / "state.json")
            (run / ".complete").unlink()
            for _ in range(3):
                advance_wave(run)
                self.assertEqual(read_json(run / "state.json"), before)
                self.assertTrue((run / ".complete").is_file())


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class HandoffRefusesLegacyState(AcceptanceFixture):

    def test_export_fails_closed_and_keeps_the_operator_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = self.drive(Path(temporary) / "run")
            legacy = {"status": "complete", "next_step": 3, "pending": None,
                      "groups": [{"task_id": "C_run3_2022_legacy_premix_v1_MERGE000000"}]}
            atomic_json(run / "state.json", legacy)
            snapshot = {"run_id": "acceptance", "runtime_release": "acceptance-release",
                        "groups": legacy["groups"]}
            atomic_json(run / "merge_groups.json", snapshot)

            with self.assertRaisesRegex(RuntimeError, "mcprod-state-v2"):
                export_handoff(run)

            self.assertEqual(read_json(run / "merge_groups.json"), snapshot)


if __name__ == "__main__":
    unittest.main()
