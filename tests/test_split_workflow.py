#!/usr/bin/env python3
import json
import tempfile
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from tools.split_workflow import (
    audit_stage,
    export_mix_manifest,
    finalize,
    prepare_workspace,
)


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def main():
    with tempfile.TemporaryDirectory(prefix="/tmp/chiw/split_workflow_") as tmp:
        root = Path(tmp)
        campaign = "MOCK_SPLIT"
        output_dir = root / "remote" / "output" / campaign
        components = []
        for index in range(2):
            job_id = f"JOB000000_BLOCK{index:06d}"
            job_dir = output_dir / job_id
            miniaod = job_dir / "output_MINIAOD.root"
            miniaod.parent.mkdir(parents=True, exist_ok=True)
            miniaod.write_text("root\n", encoding="utf-8")
            manifest = job_dir / f"processing_manifest_{campaign}_{job_id}.json"
            write_json(manifest, {"status": "ok", "complete": True})
            components.append({
                "block_index": index, "job_id": job_id, "expected_events": 5,
                "packing_weight_events": 5, "inputs": [], "sources": [],
                "miniaod_url": str(miniaod), "processing_manifest_url": str(manifest),
            })
        merge_id = "JOB000000_MERGE000000"
        merged = output_dir / merge_id / "output_MINIAOD.root"
        ntuple = output_dir / merge_id / "output_ntuple.root"
        coord = {
            "campaign": campaign, "job_index": 0, "output_mode": "mix-only",
            "analysis_type": "JJP", "efficiency_ntuple": False, "cleanup": True,
            "processing_max_events": 5, "target_eos_base": str(root / "remote"),
            "storage": {}, "miniaod_merge_enabled": True,
            "miniaod_merge_validation": "event-count",
            "merge_groups": [{
                "job_id": merge_id, "expected_events": 10,
                "packing_weight_events": 10, "components": components,
                "merged_miniaod_url": str(merged),
            }],
            "ntuples": [{"job_id": merge_id, "miniaod_input": str(merged),
                         "ntuple_url": str(ntuple)}],
        }
        coord_path = root / "cern/plan_subdags" / campaign / "job_0" / f"coord_manifest_{campaign}_0.json"
        write_json(coord_path, coord)
        split_path = root / "split.json"
        split = export_mix_manifest(str(root / "cern"), str(split_path))
        assert len(split["campaigns"][campaign]["merge_tasks"]) == 1
        bundle = root / "ntuple_runtime_bundle.tar.gz"
        bundle.write_text("bundle\n", encoding="utf-8")
        ihep = root / "ihep"
        metadata = prepare_workspace(str(split_path), str(ihep), str(BASE_DIR),
                                     str(bundle), proxy=str(root / "proxy"))
        submit_merge = Path(metadata["campaigns"][campaign]["submit_merge"]).read_text()
        submit_ntuple = Path(metadata["campaigns"][campaign]["submit_ntuple"]).read_text()
        assert '-argu "%{ProcId}" -n 1' in submit_merge
        assert f"merge_{campaign}.json" in submit_ntuple
        first = audit_stage(str(ihep), "merge")
        assert not first["complete"]
        retry_submit = Path(first["campaigns"][campaign]["retry_submit"])
        assert retry_submit.is_file()
        merge_task = split["campaigns"][campaign]["merge_tasks"][0]
        write_json(Path(merge_task["manifest_url"]), {
            "status": "ok",
            "output_url": merge_task["config"]["output_url"],
        })
        assert audit_stage(str(ihep), "merge")["complete"]
        write_json(Path(merge_task["manifest_url"]), {
            "status": "partial",
            "output_url": merge_task["config"]["output_url"],
        })
        assert not audit_stage(str(ihep), "merge")["complete"]
        write_json(Path(merge_task["manifest_url"]), {
            "status": "partial", "merge_eligible": True,
            "output_url": merge_task["config"]["output_url"],
        })
        assert audit_stage(str(ihep), "merge")["complete"]
        ntuple_task = split["campaigns"][campaign]["ntuple_tasks"][0]
        write_json(Path(ntuple_task["manifest_url"]), {
            "status": "ok", "ntuple_url": ntuple_task["ntuple_url"],
        })
        assert audit_stage(str(ihep), "ntuple")["complete"]
        preview = finalize(str(ihep))
        assert {item["action"] for item in preview["records"]} == {"would-delete"}
        assert all(Path(item["url"]).exists() for item in preview["records"])
        applied = finalize(str(ihep), apply=True)
        assert {item["action"] for item in applied["records"]} == {"deleted"}
        assert all(not Path(item["url"]).exists() for item in applied["records"])
    print("[OK] split workflow export, HepJob clusters, gates, retry, and cleanup")


if __name__ == "__main__":
    main()
