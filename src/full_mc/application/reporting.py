"""Small read-only reporting helpers for explicit batch manifests."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def batch_report(manifest_path: str | Path) -> Dict[str, Any]:
    payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    jobs = payload.get("jobs", [])
    counts: Dict[str, int] = {"prepared": 0, "submitted": 0, "unknown": 0, "completed": 0}
    for job in jobs:
        state = str(job.get("state", "prepared"))
        counts[state] = counts.get(state, 0) + 1
    return {"run_id": payload.get("run_id"), "jobs": len(jobs), "states": counts,
            "memberships": {str(job.get("job_id")): job.get("task_ids", []) for job in jobs}}
