"""Offline inventory-original retention contracts for the plan worker."""
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


def task(source, destination=None, cap=100_000, sidecar=None):
    reference = {"artifact_id": "lhe:p:s", "kind": "lhe", "url": str(source), "events": 5}
    if sidecar:
        reference["sidecar_url"] = sidecar
    return {
        "task_id": "plan-p-s", "stage": "plan", "runtime": "utilities-el9",
        "runtime_release": "test", "log_level": "warning", "inputs": [reference], "outputs": [],
        "parameters": {"pool_name": "p", "primary_seed": 1, "group_id": "s",
                       "events_per_block": 3, "shuffle_seed": 7,
                       "block_output_dir": str(source.parent / "blocks"),
                       "max_output_bytes": 100_000, "original_max_bytes": cap,
                       "original_output": {"artifact_id": "lhe:p:s", "kind": "lhe",
                                           "url": str(destination or source)}}}


def local_run(_task, _runtime, command, work_dir):
    subprocess.run(list(map(str, command)), cwd=work_dir, check=True,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


@unittest.skipUnless(shutil.which("g++"), "local compiler is required")
class OriginalRetentionContracts(unittest.TestCase):
    def test_uncompressed_source_is_gzipped_and_staged_once_with_source_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.lhe"
            target = root / "stage/original.lhe.gz"
            generate(str(source), 5)
            with patch("full_mc.workers.plan.run", side_effect=local_run):
                outcome = execute(task(source, target), root / "worker")
            self.assertEqual(len(outcome["originals"]), 1)
            original = outcome["originals"][0]
            self.assertEqual(original["events"], 5)
            self.assertEqual(original["url"], str(target))
            self.assertTrue(target.is_file())
            with gzip.open(target, "rt") as stream:
                self.assertEqual(sum(line.strip() == "<event>" for line in stream), 5)

    def test_existing_gzip_retains_bytes_and_existing_sidecar(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plain = root / "source.lhe"
            source = root / "source.lhe.gz"
            generate(str(plain), 5)
            with plain.open("rb") as incoming, gzip.open(source, "wb") as outgoing:
                shutil.copyfileobj(incoming, outgoing)
            before = source.read_bytes()
            sidecar = str(source) + ".json"
            with patch("full_mc.workers.plan.run", side_effect=local_run):
                outcome = execute(task(source, sidecar=sidecar), root / "worker")
            original = outcome["originals"][0]
            self.assertEqual(source.read_bytes(), before)
            self.assertEqual(original["size_bytes"], len(before))
            self.assertEqual(original["sidecar_url"], sidecar)
            self.assertFalse((root / "source.lhe.gz.json").exists())

    def test_original_cap_is_checked_before_any_stageout(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.lhe"
            target = root / "stage/original.lhe.gz"
            generate(str(source), 5)
            with patch("full_mc.workers.plan.run", side_effect=local_run):
                with self.assertRaisesRegex(RuntimeError, "original LHE exceeds"):
                    execute(task(source, target, cap=1), root / "worker")
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
