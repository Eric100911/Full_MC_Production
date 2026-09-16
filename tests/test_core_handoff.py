"""Offline handoff tests: gate future IHEP work on its component MiniAODs."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.io import atomic_json
from full_mc.application.handoff import export_handoff


class HandoffContracts(unittest.TestCase):
    def test_exports_components_before_downstream_outputs_exist(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            mini = root / "mini.root"
            mini.write_bytes(b"fixture")
            ref = {"artifact_id": "mini", "kind": "miniaod", "url": str(mini)}
            atomic_json(str(mini) + ".json", dict(ref, status="ok", complete=True,
                        events=17, count_source="edmFileUtil", size_bytes=7))
            group = {"task_id": "group", "stage": "combined", "subtasks": {
                stage: {"task_id": stage, "stage": stage,
                        "inputs": [ref] if stage == "merge" else [],
                        "outputs": [{"artifact_id": stage, "kind": stage,
                                     "url": str(root / (stage + ".root"))}],
                        "parameters": {"core_root": "/cern/checkout"}}
                for stage in ("merge", "ntuple", "selection", "efficiency")}}
            atomic_json(root / "merge_groups.json", {"run_id": "r",
                        "runtime_release": "r1", "groups": [group]})
            payload = export_handoff(root)
            self.assertEqual(len(payload["tasks"]), 1)
            merge = payload["tasks"][0]["subtasks"]["merge"]
            self.assertEqual(merge["inputs"][0]["events"], 17)
            self.assertNotIn("core_root", merge["parameters"])
            self.assertFalse((root / "merge.root").exists())
            mini.unlink()
            self.assertEqual(export_handoff(root)["tasks"], [])


if __name__ == "__main__":
    unittest.main()
