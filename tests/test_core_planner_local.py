"""Run the real LHE splitter/planner locally; mock only the container boundary."""
import gzip
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.workers.plan import execute
from tests.generate_synthetic_lhe import generate


@unittest.skipUnless(shutil.which("g++"), "local compiler is required")
class LocalPlanner(unittest.TestCase):
    def test_exact_count_cap_and_nonoverlapping_blocks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "sample.lhe"
            generate(str(source), 23)
            task = {
                "task_id": "plan-p-source9", "stage": "plan", "runtime": "utilities-el9",
                "runtime_release": "test", "log_level": "warning",
                "inputs": [{"artifact_id": "lhe:p:source9", "kind": "lhe",
                            "url": str(source), "events": 23}],
                "outputs": [], "parameters": {
                    "pool_name": "p", "primary_seed": 9, "group_id": "source9",
                    "events_per_block": 7, "shuffle_seed": 9037,
                    "max_events_per_plan": 18,
                    "block_output_dir": str(root / "staging"),
                    "max_output_bytes": 100_000}}
            def local_run(task, runtime, command, work_dir):
                subprocess.run(list(map(str, command)), cwd=work_dir, check=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            with patch("full_mc.workers.plan.run", side_effect=local_run):
                outcome = execute(task, root / "worker")
            self.assertEqual(outcome["actual_input_events"], 23)
            self.assertEqual(outcome["planned_events"], 18)
            self.assertEqual([r["events"] for r in outcome["outputs"]], [7, 7, 4])
            self.assertEqual([r["artifact_id"] for r in outcome["outputs"]],
                             [f"BLOCK:p:source9:{i:06d}" for i in range(3)])
            events = []
            for item in outcome["outputs"]:
                with gzip.open(item["url"], "rt") as stream:
                    lines = iter(stream)
                    for line in lines:
                        if line.strip() == "<event>":
                            events.append(next(lines).strip())
            self.assertEqual(len(events), 18)
            self.assertEqual(len(set(events)), 18)
            self.assertEqual(len(list((root / "staging").iterdir())), 3)


if __name__ == "__main__":
    unittest.main()
