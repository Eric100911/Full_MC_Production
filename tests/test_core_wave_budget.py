"""Every wave the root DAG declares must be enough to finish the run.

A wave's PRE folds wave i-1 and prepares wave i, so the last prepared wave still
needs a successor to fold it.  A terminal reclaim wave is prepared into a slot
and therefore needs that successor in turn.  When the wave ceiling is one short,
DAGMan runs every wave it declares and reports success while ``.complete`` is
never written and ``state["status"]`` stays at "running": a silent
non-completion that no log line reports as a failure.
"""
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from full_mc.application.production import advance_wave, plan_run
from full_mc.io import read_json
from tests.test_core_seam_acceptance import AcceptanceFixture


@patch("full_mc.application.production.resolve_proxy_path",
       new=lambda: "/afs/cern.ch/user/test/x509_user_proxy")
class DeclaredWaveBudget(AcceptanceFixture):
    """Drive only the waves run.dag actually declares, as DAGMan would."""

    def advance_through_declared_waves(self, run, steps):
        for step in range(1, steps):
            pending = read_json(run / "state.json")["pending"]
            phase = pending["phase"] if pending else None
            if phase == "sources":
                self.plan_result(run)
            elif phase == "processing":
                self.processing_results(run)
                # A processing wave is consumed only once its own reclaim
                # result is present, so stage both before folding.
                self.write_reclaim_result(run)
            elif phase == "reclaim":
                self.write_reclaim_result(run)
            advance_wave(run, step)

    def test_last_declared_wave_completes_the_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            steps = plan_run(spec, run)["lazy_steps"]
            dag = (run / "run.dag").read_text()
            self.assertIn("WAVE_%06d" % (steps - 1), dag)
            self.assertNotIn("WAVE_%06d" % steps, dag)

            self.advance_through_declared_waves(run, steps)

            state = read_json(run / "state.json")
            self.assertEqual(state["status"], "complete")
            self.assertEqual(state["pending"], {})
            self.assertTrue((run / ".complete").is_file())

    def test_the_trailing_slots_carry_the_terminal_reclaim_and_its_fold(self):
        """The extra slots must be used by real work, not left as padding."""
        with tempfile.TemporaryDirectory() as temporary:
            spec, run = self.fixture(Path(temporary))
            steps = plan_run(spec, run)["lazy_steps"]
            self.plan_result(run)
            advance_wave(run, 1)
            self.processing_results(run)
            self.write_reclaim_result(run)

            prepared = advance_wave(run, 2)

            self.assertEqual(prepared["phase"], "reclaim")
            self.assertEqual(read_json(run / "state.json")["pending"]["step"], 2)
            # The reclaim wave is prepared at step 2, so folding it takes step
            # 3: the root DAG must declare that successor or the run never
            # commits completion.
            self.assertGreaterEqual(steps, 4)


if __name__ == "__main__":
    unittest.main()
