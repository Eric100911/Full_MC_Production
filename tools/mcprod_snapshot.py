#!/usr/bin/env python3
"""Read-only snapshot of the recorded static production attempt."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import subprocess
import sys

if __package__:
    from . import dag_progress
else:
    import dag_progress


def read_json(path: Path, *, required: bool = True) -> dict | None:
    if not path.is_file():
        if required:
            raise ValueError(f"required run record is missing: {path}")
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read JSON record {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON record must be an object: {path}")
    return value


def normalized_cluster(value: object, label: str) -> int:
    try:
        cluster = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer cluster ID") from exc
    if cluster <= 0:
        raise ValueError(f"{label} must be a positive cluster ID")
    return cluster


def resolve_identity(
    run_dir: Path,
    requested_schedd: str | None = None,
    requested_cluster: int | str | None = None,
) -> dict:
    """Resolve exactly the recorded current attempt; never consult last_status."""
    run_dir = run_dir.expanduser().resolve()
    submission = read_json(run_dir / "submission.json")
    run_record = read_json(run_dir / "run.json")
    if run_record.get("orchestration") != "static_subdag":
        raise ValueError("snapshot requires static_subdag orchestration")
    number = submission.get("current_attempt")
    if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
        raise ValueError("submission.json has no valid current_attempt")
    attempts = submission.get("attempts")
    if not isinstance(attempts, list):
        raise ValueError("submission.json attempts must be a list")
    matches = [item for item in attempts
               if isinstance(item, dict) and item.get("number") == number]
    if len(matches) != 1:
        raise ValueError("current attempt is ambiguous")
    attempt = matches[0]

    schedd = attempt.get("scheduler")
    if not isinstance(schedd, str) or not schedd.strip():
        raise ValueError("current attempt has no scheduler")
    cluster = normalized_cluster(attempt.get("root_cluster_id"),
                                 "current attempt root_cluster_id")
    top_cluster = normalized_cluster(submission.get("root_cluster_id"),
                                     "submission root_cluster_id")
    if submission.get("scheduler") != schedd or top_cluster != cluster:
        raise ValueError("submission identity disagrees with current attempt")

    recorded_run_dir = submission.get("run_dir")
    recorded_run_path = (
        Path(recorded_run_dir).expanduser().resolve()
        if isinstance(recorded_run_dir, str)
        else None
    )
    if recorded_run_path != run_dir:
        raise ValueError("submission run_dir disagrees with --run")
    expected_dag = (run_dir / "run.dag").resolve()
    recorded_dag = submission.get("dag")
    recorded_dag_path = (
        Path(recorded_dag).expanduser().resolve()
        if isinstance(recorded_dag, str)
        else None
    )
    if recorded_dag_path != expected_dag:
        raise ValueError("submission DAG path disagrees with the run directory")
    if not expected_dag.is_file():
        raise ValueError(f"run DAG is missing: {expected_dag}")

    if requested_schedd is not None and requested_schedd != schedd:
        raise ValueError("--schedd conflicts with the current attempt")
    if requested_cluster is not None:
        override = normalized_cluster(requested_cluster, "--cluster")
        if override != cluster:
            raise ValueError("--cluster conflicts with the current attempt")

    return {
        "run_dir": run_dir,
        "dag_file": expected_dag,
        "run_record": run_record,
        "submission": submission,
        "attempt_number": number,
        "schedd": schedd,
        "cluster": cluster,
    }


def query_queue(schedd: str, cluster: int) -> list[dict]:
    """Query only the requested root DAG on its recorded schedd."""
    result = subprocess.run(
        ["condor_q", "-name", schedd, "-dag", str(cluster), "-json"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if not result.stdout.strip():
        return []
    records = json.loads(result.stdout)
    if not isinstance(records, list) or any(
        not isinstance(item, dict) for item in records
    ):
        raise RuntimeError("condor_q returned JSON other than a list of ads")
    return records


def build_snapshot(run_dir, *, schedd=None, cluster=None, queue_reader=query_queue):
    identity = resolve_identity(run_dir, schedd, cluster)
    graph = dag_progress.parse_dag_graph(identity["dag_file"])
    planned = dag_progress.flatten_graph(graph)
    logs = dag_progress.completed_records_from_logs(planned)
    try:
        live = queue_reader(identity["schedd"], identity["cluster"])
        queue = {"status": "ok", "records": len(live)}
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        live = []
        queue = {"status": "error", "error": str(exc)}

    records = dag_progress.latest_node_records(live, logs)
    nodes = {}
    counts = defaultdict(Counter)
    for name, node in planned.items():
        stage = node.stage
        record = records.get(name)
        status = dag_progress.record_status(record) if record else "pending"
        source = "DAGMan/Condor" if record else "DAG"
        nodes[name] = {"stage": stage, "status": status, "source": source}
        counts[stage][status] += 1

    metrics = read_json(identity["run_dir"] / "run.dag.metrics", required=False)
    if metrics and str(metrics.get("dagman_id")) != str(identity["cluster"]):
        metrics = None
    completion = read_json(identity["run_dir"] / "results/RECLAIM.json", required=False)
    status = "running"
    if metrics:
        if metrics.get("exitcode") == 0 and completion and completion.get("status") == "ok":
            status = "complete"
        elif metrics.get("exitcode") != 0:
            status = "failed"
    return {"run_id": identity["run_record"]["run_id"], "schedd": identity["schedd"],
            "root_cluster_id": identity["cluster"], "attempt": identity["attempt_number"],
            "status": status, "queue": queue,
            "root_metrics": metrics,
            "stages": dict(counts), "nodes": nodes}


def render_text(snapshot):
    lines = [f'{snapshot["run_id"]} {snapshot["schedd"]} '
             f'root={snapshot["root_cluster_id"]} {snapshot["status"]}']
    if snapshot["queue"]["status"] == "error":
        lines.append("Queue query failed: " + snapshot["queue"]["error"])
    for stage, counts in sorted(snapshot["stages"].items()):
        lines.append(stage + "  " + " ".join(f"{name}={value}"
                     for name, value in sorted(counts.items())))
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run", required=True, type=Path, help="explicit mcprod run directory"
    )
    parser.add_argument("--schedd", help="assert the recorded current-attempt schedd")
    parser.add_argument(
        "--cluster", help="assert the recorded current root DAGMan cluster"
    )
    parser.add_argument("--json", action="store_true", help="emit JSON")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        snapshot = build_snapshot(args.run, schedd=args.schedd, cluster=args.cluster)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        print(f"mcprod_snapshot.py: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
    else:
        print(render_text(snapshot))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
