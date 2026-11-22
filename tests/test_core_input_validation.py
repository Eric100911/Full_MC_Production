"""Consumption-time revalidation of materialized inputs.

A producer manifest is trusted at the seam, but a later download must still be
checked byte-for-byte and event-for-event before it is used.
"""
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from full_mc.storage.transfer import materialize_input
from full_mc.workers.chain import execute_chain
from tests.generate_synthetic_lhe import generate


class FakeTransfer:
    """A TransferClient stand-in that copies one prepared local source."""

    def __init__(self, source):
        self.source = Path(source)

    def copy_in(self, url, destination):
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.source, destination)
        return destination


def mix_task(root, *, file_events, declared_events=None, size_bytes=False):
    """A one-slot mix task whose single block is a real local LHE file."""
    block = root / "block.lhe"
    generate(str(block), file_events)
    reference = {"artifact_id": "blk", "url": str(block), "kind": "lhe-block"}
    if declared_events is not None:
        reference["events"] = declared_events
    if size_bytes:
        reference["size_bytes"] = block.stat().st_size
    return {"task_id": "mix-1", "stage": "mix", "runtime": "pythia-el8",
            "runtime_release": "r1", "inputs": [reference],
            "parameters": {"analysis": "JJP", "campaign": "JJP_DPS1",
                           "sources": [{"inputs": ["blk"], "rng_seed": 7}],
                           "mixing_rng_seed": 11}}


class MaterializeSizeTests(unittest.TestCase):
    def test_downloaded_size_mismatch_removes_copy_and_raises(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "downloaded.lhe"
            generate(str(source), 3)
            reference = {"artifact_id": "blk",
                         "url": "root://eosuser.cern.ch///eos/x/block.lhe.gz",
                         "kind": "lhe-block",
                         "size_bytes": source.stat().st_size + 5}
            work = root / "work"
            with self.assertRaisesRegex(RuntimeError,
                                        "downloaded size disagrees with blk"):
                materialize_input(reference, work, FakeTransfer(source))
            self.assertFalse((work / "inputs" / "blk" / "block.lhe.gz").exists())

    def test_local_file_size_mismatch_is_removed_and_raises(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            local = root / "block.lhe"
            generate(str(local), 3)
            reference = {"artifact_id": "blk", "url": str(local),
                         "kind": "lhe-block",
                         "size_bytes": local.stat().st_size + 1}
            with self.assertRaisesRegex(RuntimeError,
                                        "downloaded size disagrees with blk"):
                materialize_input(reference, root / "work")
            self.assertFalse(local.exists())

    def test_stale_cached_copy_cannot_leak_through(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cached = root / "cached.lhe"
            generate(str(cached), 1)
            fresh = root / "fresh.lhe"
            generate(str(fresh), 5)
            reference = {"artifact_id": "blk", "local_path": str(cached),
                         "url": "root://eosuser.cern.ch///eos/x/block.lhe.gz",
                         "kind": "lhe-block",
                         "size_bytes": fresh.stat().st_size}
            destination = materialize_input(reference, root / "work",
                                            FakeTransfer(fresh))
            self.assertEqual(destination.stat().st_size, fresh.stat().st_size)
            self.assertNotEqual(destination, cached.resolve())

    def test_reference_without_size_bytes_is_not_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            local = root / "block.lhe"
            generate(str(local), 3)
            reference = {"artifact_id": "blk", "url": str(local),
                         "kind": "lhe-block"}
            self.assertEqual(materialize_input(reference, root / "work"),
                             local.resolve())


class MixBlockCountTests(unittest.TestCase):
    def test_event_count_mismatch_fails_before_shower(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = mix_task(root, file_events=5, declared_events=4, size_bytes=True)
            with patch("full_mc.workers.chain.native_tree") as tree, \
                    patch("full_mc.workers.chain.run") as run:
                with self.assertRaisesRegex(
                        RuntimeError, "blk: read 5 LHE events, expected 4"):
                    execute_chain(task, root / "work")
            self.assertFalse(run.called)
            self.assertFalse(tree.called)

    def test_correct_block_passes_the_count_check(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = mix_task(root, file_events=5, declared_events=5, size_bytes=True)
            with patch("full_mc.workers.chain.native_tree"), \
                    patch("full_mc.workers.chain.run") as run:
                execute_chain(task, root / "work")
            self.assertTrue(run.called)
            config = json.loads((root / "work" / "chain.json").read_text())
            self.assertEqual(config["sources"][0]["inputs"],
                             ["file:" + str((root / "block.lhe").resolve())])

    def test_manifest_without_events_or_size_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = mix_task(root, file_events=3)
            with patch("full_mc.workers.chain.native_tree"), \
                    patch("full_mc.workers.chain.run") as run:
                execute_chain(task, root / "work")
            self.assertTrue(run.called)


if __name__ == "__main__":
    unittest.main()
