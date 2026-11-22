"""Resource defaults emitted for production task DAGs."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.planning.graph import write_submit_files, write_tasks_dag


class LHEResourceDefaults(unittest.TestCase):
    def test_lhe_uses_one_cpu_three_gb_and_preserves_scratch_ceiling(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "run"
            for directory in ("tasks", "steps"):
                (root / directory).mkdir(parents=True, exist_ok=True)

            core = base / "core"
            bootstrap = core / "src/full_mc/runtime/bootstrap.py"
            bootstrap.parent.mkdir(parents=True)
            bootstrap.write_text("# test bootstrap\n", encoding="utf-8")

            runtime = base / "runtime"
            runtime.mkdir()
            (runtime / "runtime.json").write_text(json.dumps({
                "profiles": ["helac-el7"],
                "bundles": {"core": "core.tar.gz", "helac-el7": "lhe.tar.gz"},
            }), encoding="utf-8")

            submit_dir = write_submit_files(root, core, runtime, base / "proxy")
            lhe_submit = (submit_dir / "lhe.sub").read_text(encoding="utf-8")
            self.assertIn("request_cpus = 1", lhe_submit)

            scratch_bytes = 9 * 1024**3
            task = {"task_id": "LHE_000001", "stage": "lhe",
                    "parameters": {"scratch_bytes": scratch_bytes}}
            dag = write_tasks_dag(root, 0, [task], [], {})
            vars_line = next(line for line in dag.read_text(encoding="utf-8").splitlines()
                             if line.startswith("VARS LHE_000001 "))

            self.assertIn('memory="3072"', vars_line)
            self.assertIn(f'disk="{scratch_bytes // 1024}"', vars_line)


if __name__ == "__main__":
    unittest.main()
