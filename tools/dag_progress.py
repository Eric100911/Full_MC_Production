#!/usr/bin/env python3
"""Render stage-by-stage progress for an HTCondor DAG and its SubDAGs."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


STAGES = (
    ("plan", "Plan"),
    ("coordinate", "Coordinate"),
    ("subdag", "SubDAG"),
    ("processing", "Process"),
    ("merge", "Merge"),
    ("ntuple", "Ntuple"),
    ("final", "Final"),
)
STATUS_ORDER = ("done", "running", "idle", "held", "failed", "pending")
STATUS_LABELS = {
    "done": "DONE",
    "running": "RUN",
    "idle": "IDLE",
    "held": "HELD",
    "failed": "FAIL",
    "pending": "WAIT",
}
CONDOR_STATUS = {
    1: "idle",
    2: "running",
    3: "failed",
    4: "done",
    5: "held",
    6: "running",
    7: "running",
}
QUERY_ATTRIBUTES = ",".join(
    (
        "ClusterId",
        "ProcId",
        "JobStatus",
        "JobUniverse",
        "DAGNodeName",
        "EnteredCurrentStatus",
        "QDate",
        "CompletionDate",
        "ExitCode",
        "HoldReason",
        "NumShadowExceptions",
        "UserLog",
    )
)


@dataclass(frozen=True)
class PlannedNode:
    name: str
    stage: str
    dag_file: Path


def run_json(command: list[str]) -> list[dict]:
    result = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if not result.stdout.strip():
        return []
    data = json.loads(result.stdout)
    if not isinstance(data, list):
        raise RuntimeError(f"expected a JSON list from {command[0]}")
    return data


def batch_constraint(cluster: int) -> str:
    return f'JobBatchId == "{cluster}.0"'


def query_jobs(schedd: str, cluster: int) -> list[dict]:
    common = [
        "-name",
        schedd,
        "-constraint",
        batch_constraint(cluster),
        "-attributes",
        QUERY_ATTRIBUTES,
        "-json",
    ]
    return run_json(["condor_q", *common])


def infer_dag_file(records: Iterable[dict]) -> Path | None:
    for record in records:
        if record.get("DAGNodeName"):
            continue
        user_log = record.get("UserLog")
        if not isinstance(user_log, str):
            continue
        suffix = ".dagman.log"
        if user_log.endswith(suffix):
            candidate = Path(user_log[: -len(suffix)])
            if candidate.is_file():
                return candidate
    return None


def node_stage(name: str, directive: str) -> str:
    if directive == "SUBDAG":
        return "subdag"
    if name.startswith("PLAN_"):
        return "plan"
    if name.startswith("COORD_"):
        return "coordinate"
    if name.startswith("MERGE_"):
        return "merge"
    if name.startswith("NTUPLE_"):
        return "ntuple"
    if name.startswith("FINAL_"):
        return "final"
    if "_BLOCK" in name or name.startswith(("MIX_", "PROCESS_")):
        return "processing"
    return "processing"


def parse_dag_tree(root_dag: Path) -> dict[str, PlannedNode]:
    planned: dict[str, PlannedNode] = {}
    visited: set[Path] = set()

    def visit(dag_file: Path) -> None:
        dag_file = dag_file.resolve()
        if dag_file in visited or not dag_file.is_file():
            return
        visited.add(dag_file)
        for raw_line in dag_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                fields = shlex.split(line)
            except ValueError:
                continue
            if not fields:
                continue
            directive = fields[0].upper()
            if directive == "JOB" and len(fields) >= 2:
                name = fields[1]
                planned[name] = PlannedNode(
                    name=name,
                    stage=node_stage(name, directive),
                    dag_file=dag_file,
                )
            elif directive == "FINAL" and len(fields) >= 2:
                name = fields[1]
                planned[name] = PlannedNode(name, "final", dag_file)
            elif (
                directive == "SUBDAG"
                and len(fields) >= 4
                and fields[1].upper() == "EXTERNAL"
            ):
                name = fields[2]
                planned[name] = PlannedNode(name, "subdag", dag_file)
                child = Path(fields[3])
                if not child.is_absolute():
                    child = dag_file.parent / child
                visit(child)

    visit(root_dag)
    return planned


SUCCESS_PATTERN = re.compile(r"\bNode\s+(\S+).* completed successfully\.")
NODE_PATTERN = re.compile(r"\bNode\s+(\S+)\b")


def completed_records_from_logs(
    planned: dict[str, PlannedNode],
) -> list[dict]:
    """Read terminal node states from the DAGMan logs beside each DAG file."""
    states: dict[str, dict] = {}
    dag_files = {node.dag_file for node in planned.values()}
    for dag_file in sorted(dag_files):
        dagman_out = Path(f"{dag_file}.dagman.out")
        if not dagman_out.is_file():
            continue
        for line in dagman_out.read_text(
            encoding="utf-8", errors="replace"
        ).splitlines():
            success = SUCCESS_PATTERN.search(line)
            if success:
                name = success.group(1)
                if name in planned:
                    states[name] = {
                        "DAGNodeName": name,
                        "JobStatus": 4,
                        "ExitCode": 0,
                    }
                continue
            lowered = line.lower()
            if " failed" not in lowered and "status_error" not in lowered:
                continue
            failed = NODE_PATTERN.search(line)
            if failed and failed.group(1) in planned:
                name = failed.group(1)
                states[name] = {
                    "DAGNodeName": name,
                    "JobStatus": 4,
                    "ExitCode": 1,
                }
    return list(states.values())


def record_timestamp(record: dict) -> int:
    for key in ("EnteredCurrentStatus", "CompletionDate", "QDate"):
        value = record.get(key)
        if isinstance(value, (int, float)):
            return int(value)
    return 0


def record_status(record: dict) -> str:
    status = CONDOR_STATUS.get(int(record.get("JobStatus", 0)), "failed")
    if status == "done" and record.get("ExitCode") not in (None, 0):
        return "failed"
    return status


def latest_node_records(live: list[dict], history: list[dict]) -> dict[str, dict]:
    selected: dict[str, tuple[int, int, dict]] = {}
    for is_live, records in ((0, history), (1, live)):
        for record in records:
            name = record.get("DAGNodeName")
            if not isinstance(name, str) or not name:
                continue
            key = (is_live, record_timestamp(record))
            if name not in selected or key > selected[name][:2]:
                selected[name] = (*key, record)
    return {name: item[2] for name, item in selected.items()}


def progress_bar(done: int, total: int, width: int = 24) -> str:
    if total <= 0:
        return " " * width
    filled = min(width, round(width * done / total))
    return "█" * filled + "░" * (width - filled)


def format_age(seconds: int) -> str:
    seconds = max(0, seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"


def render(
    schedd: str,
    cluster: int,
    root_dag: Path,
    live: list[dict],
    show_details: bool,
) -> str:
    planned = parse_dag_tree(root_dag)
    completed = completed_records_from_logs(planned)
    records = latest_node_records(live, completed)
    counts = {stage: Counter() for stage, _ in STAGES}
    exceptional: list[tuple[str, str, dict]] = []

    for name, node in planned.items():
        record = records.get(name)
        status = record_status(record) if record else "pending"
        counts[node.stage][status] += 1
        if status in {"idle", "held", "failed"}:
            exceptional.append((status, name, record or {}))

    root_records = [
        record
        for record in live
        if not record.get("DAGNodeName") and record.get("JobUniverse") == 7
    ]
    controllers = Counter(record_status(record) for record in root_records)
    now = int(time.time())
    lines = [
        f"DAG {cluster} on {schedd}",
        f"Source: {root_dag}",
        "",
        "Plan → Coordinate → SubDAG → Process → Merge → Ntuple → Final",
        "",
        "Stage          Progress                    Done  Run Idle Held Fail Wait",
        "-------------  --------------------------  ----  --- ---- ---- ---- ----",
    ]
    for stage, label in STAGES:
        stage_counts = counts[stage]
        total = sum(stage_counts.values())
        bar = progress_bar(stage_counts["done"], total)
        lines.append(
            f"{label:<13}  [{bar}]"
            f" {stage_counts['done']:>4} {stage_counts['running']:>4}"
            f" {stage_counts['idle']:>4} {stage_counts['held']:>4}"
            f" {stage_counts['failed']:>4} {stage_counts['pending']:>4}"
        )

    total_counts = Counter()
    for stage_counts in counts.values():
        total_counts.update(stage_counts)
    total = sum(total_counts.values())
    lines.extend(
        (
            "",
            f"Logical nodes: {total_counts['done']}/{total} done; "
            f"{total_counts['running']} running, {total_counts['idle']} idle, "
            f"{total_counts['held']} held, {total_counts['failed']} failed, "
            f"{total_counts['pending']} waiting",
            "Root DAGMan: "
            + ", ".join(
                f"{controllers[status]} {STATUS_LABELS[status].lower()}"
                for status in STATUS_ORDER
                if controllers[status]
            ),
        )
    )

    if exceptional:
        lines.extend(("", "Attention:"))
        for status, name, record in sorted(exceptional):
            age = format_age(now - record_timestamp(record))
            reason = record.get("HoldReason") or ""
            shadow = int(record.get("NumShadowExceptions") or 0)
            suffix = f"; shadow exceptions={shadow}" if shadow else ""
            if reason:
                suffix += f"; {reason}"
            lines.append(
                f"  {STATUS_LABELS[status]:<4} {name} "
                f"(for {age}, {record.get('ClusterId', '?')}."
                f"{record.get('ProcId', '?')}{suffix})"
            )

    if show_details:
        active = []
        for name, record in records.items():
            status = record_status(record)
            if status not in {"idle", "running", "held"}:
                continue
            node = planned.get(name)
            stage = node.stage if node else "unknown"
            active.append((stage, status, name, record))
        if active:
            lines.extend(("", "Active nodes:"))
            for stage, status, name, record in sorted(active):
                age = format_age(now - record_timestamp(record))
                lines.append(
                    f"  {stage:<10} {STATUS_LABELS[status]:<4} "
                    f"{record.get('ClusterId', '?')}."
                    f"{record.get('ProcId', '?')} {age:>7}  {name}"
                )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Visualize an HTCondor DAG and its nested processing stages."
    )
    parser.add_argument("cluster", type=int, help="root DAGMan cluster ID")
    parser.add_argument(
        "--schedd", required=True, help="schedd name, for example bigbird08.cern.ch"
    )
    parser.add_argument(
        "--dag-file",
        type=Path,
        help="root .dag file (normally inferred from the DAGMan classad)",
    )
    parser.add_argument(
        "--details", action="store_true", help="list every active logical node"
    )
    parser.add_argument(
        "--watch",
        type=float,
        metavar="SECONDS",
        help="refresh continuously at this interval",
    )
    args = parser.parse_args()
    if args.watch is not None and args.watch <= 0:
        parser.error("--watch must be positive")
    return args


def main() -> int:
    args = parse_args()
    while True:
        try:
            live = query_jobs(args.schedd, args.cluster)
            root_dag = args.dag_file or infer_dag_file(live)
            if root_dag is None:
                raise RuntimeError(
                    "could not infer the root DAG file; pass it with --dag-file"
                )
            output = render(
                args.schedd,
                args.cluster,
                root_dag,
                live,
                args.details,
            )
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            print(f"dag_progress.py: {exc}", file=sys.stderr)
            return 1
        except RuntimeError as exc:
            print(f"dag_progress.py: {exc}", file=sys.stderr)
            return 2

        if args.watch is not None and sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        print(output, flush=True)
        if args.watch is None:
            return 0
        time.sleep(args.watch)


if __name__ == "__main__":
    raise SystemExit(main())
