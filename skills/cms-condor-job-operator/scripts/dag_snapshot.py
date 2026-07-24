#!/usr/bin/env python3
"""Print one concise, read-only snapshot of a Full_MC_Production DAG."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from collections import Counter
from pathlib import Path
from types import ModuleType


LIVE_STATUS = {
    1: "idle",
    2: "running",
    3: "removed",
    4: "completed",
    5: "held",
    6: "transferring",
    7: "suspended",
}
ACTIVE_LOGICAL_STATUSES = {"running", "idle", "held"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Print a concise, one-shot DAGMan snapshot without polling or "
            "changing queue state."
        )
    )
    parser.add_argument("cluster", type=int, help="top-level DAGMan cluster ID")
    parser.add_argument("--schedd", required=True, help="recorded schedd hostname")
    parser.add_argument(
        "--dag-file",
        type=Path,
        help="root DAG file; normally inferred from the live DAGMan ClassAd",
    )
    parser.add_argument(
        "--max-active",
        type=int,
        default=5,
        help="maximum active logical nodes to print (default: 5)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the snapshot as JSON",
    )
    args = parser.parse_args()
    if args.max_active < 0:
        parser.error("--max-active must be non-negative")
    return args


def load_dag_progress() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    source = repo_root / "tools" / "dag_progress.py"
    if not source.is_file():
        raise RuntimeError(f"missing repository progress tool: {source}")
    spec = importlib.util.spec_from_file_location("_dag_progress", source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load repository progress tool: {source}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def live_counts(records: list[dict], *, controllers: bool) -> Counter:
    counts: Counter = Counter()
    for record in records:
        is_controller = record.get("JobUniverse") == 7
        if is_controller != controllers:
            continue
        status = LIVE_STATUS.get(int(record.get("JobStatus") or 0), "unknown")
        counts[status] += 1
    return counts


def format_age(seconds: int) -> str:
    seconds = max(0, seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60
    if days:
        return f"{days}d{hours}h"
    if hours:
        return f"{hours}h{minutes}m"
    return f"{minutes}m"


def build_snapshot(
    progress: ModuleType,
    schedd: str,
    cluster: int,
    dag_file: Path | None,
) -> dict:
    live = progress.query_jobs(schedd, cluster)
    root_dag = dag_file or progress.infer_dag_file(live)
    if root_dag is None:
        raise RuntimeError(
            "could not infer the root DAG file; pass it with --dag-file"
        )
    root_dag = root_dag.resolve()
    graph = progress.parse_dag_graph(root_dag)
    planned = progress.flatten_graph(graph)
    completed = progress.completed_records_from_logs(planned)
    records = progress.latest_node_records(live, completed)

    logical: Counter = Counter()
    active = []
    now = int(time.time())
    for name, node in planned.items():
        record = records.get(name)
        status = progress.record_status(record) if record else "pending"
        logical[status] += 1
        if status not in ACTIVE_LOGICAL_STATUSES:
            continue
        timestamp = progress.record_timestamp(record)
        active.append(
            {
                "status": status,
                "stage": node.stage,
                "name": name,
                "job_id": (
                    f"{record.get('ClusterId', '?')}."
                    f"{record.get('ProcId', '?')}"
                ),
                "age_seconds": max(0, now - timestamp),
                "hold_reason": record.get("HoldReason") or "",
            }
        )

    active.sort(
        key=lambda item: (
            {"held": 0, "idle": 1, "running": 2}.get(item["status"], 3),
            -item["age_seconds"],
            item["name"],
        )
    )
    payload = live_counts(live, controllers=False)
    controllers = live_counts(live, controllers=True)
    total = sum(logical.values())
    done = logical["done"]
    if logical["failed"] or logical["held"] or payload["held"]:
        assessment = "blocked"
    elif total and done == total:
        assessment = "complete"
    elif logical["running"] or logical["idle"]:
        assessment = "active"
    else:
        assessment = "waiting"

    return {
        "assessment": assessment,
        "schedd": schedd,
        "cluster": cluster,
        "dag_file": str(root_dag),
        "logical": {
            "done": done,
            "total": total,
            "running": logical["running"],
            "idle": logical["idle"],
            "held": logical["held"],
            "failed": logical["failed"],
            "waiting": logical["pending"],
        },
        "live_payload": dict(payload),
        "live_controllers": dict(controllers),
        "active": active,
    }


def count_text(counts: dict) -> str:
    order = ("running", "idle", "held", "transferring", "suspended", "removed")
    parts = [f"{counts.get(status, 0)} {status}" for status in order]
    return ", ".join(parts)


def render_text(snapshot: dict, max_active: int) -> str:
    logical = snapshot["logical"]
    lines = [
        (
            f"DAG {snapshot['cluster']} on {snapshot['schedd']}: "
            f"{snapshot['assessment']}"
        ),
        (
            f"Logical: {logical['done']}/{logical['total']} done; "
            f"{logical['running']} running, {logical['idle']} idle, "
            f"{logical['held']} held, {logical['failed']} failed, "
            f"{logical['waiting']} waiting"
        ),
        f"Live payload: {count_text(snapshot['live_payload'])}",
        f"DAGMan controllers: {count_text(snapshot['live_controllers'])}",
    ]
    active = snapshot["active"]
    if active and max_active:
        lines.append("Active logical nodes:")
        for item in active[:max_active]:
            suffix = (
                f"; {item['hold_reason']}" if item["hold_reason"] else ""
            )
            lines.append(
                f"  {item['status'].upper():<7} {item['stage']:<10} "
                f"{item['job_id']:<12} {format_age(item['age_seconds']):>6} "
                f"{item['name']}{suffix}"
            )
        omitted = len(active) - max_active
        if omitted > 0:
            lines.append(f"  ... {omitted} more active logical node(s)")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    try:
        progress = load_dag_progress()
        snapshot = build_snapshot(
            progress,
            args.schedd,
            args.cluster,
            args.dag_file,
        )
    except Exception as exc:
        print(f"dag_snapshot.py: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
    else:
        print(render_text(snapshot, args.max_active))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
