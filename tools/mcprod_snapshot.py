#!/usr/bin/env python3
"""Read-only one-shot snapshot for a mcprod run directory."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable, Iterable

if __package__:
    from .dag_progress import (
        DagGraph,
        PlannedNode,
        PRE_FAILURE_PATTERN,
        PRESKIP_EVENT_PATTERN,
        SUCCESS_PATTERN,
        flatten_graph,
        parse_dag_graph,
    )
else:
    from dag_progress import (
        DagGraph,
        PlannedNode,
        PRE_FAILURE_PATTERN,
        PRESKIP_EVENT_PATTERN,
        SUCCESS_PATTERN,
        flatten_graph,
        parse_dag_graph,
    )


STATUSES = ("running", "queued", "held", "failed", "done", "skipped", "pending")
LIVE_STATUS = {1: "queued", 2: "running", 3: "failed", 4: "done",
               5: "held", 6: "running", 7: "running"}
QUEUE_PRIORITY = {"held": 0, "running": 1, "queued": 2, "failed": 3, "done": 4}
STARTUP_PATTERN = re.compile(
    r"condor_scheduniv_exec\.(?P<cluster>\d+)\.0 .*STARTING UP"
)
# HTCondor 25.13+ no longer includes the condor_scheduniv_exec.<id>.0
# banner in DAGMan's stdout log. It emits the DAGMan identity separately;
# ignoring this format makes every node in a completed run look pending once
# the live queue has drained.
WORKFLOW_BATCH_PATTERN = re.compile(
    r"Workflow batch-id:\s*<(?P<cluster>\d+)\.\d+>"
)
DAG_EVENT_PATTERN = re.compile(
    r"\bEvent: ULOG_(?P<event>[A-Z_]+) for HTCondor Node "
    r"(?P<name>\S+) \((?P<cluster>\d+)\.\d+\.\d+\)"
)
EXIT_PATTERN = re.compile(r"EXITING WITH STATUS (?P<status>\d+)", re.IGNORECASE)
ASSIGNED_PATTERN = re.compile(r"assigned HTCondor ID \((?P<cluster>\d+)\.\d+\.\d+\)")
NODE_SUBMIT_PATTERN = re.compile(r"Submitting (?:HTCondor )?Node (?P<name>WAVE_\S+)")
ULOG_HEADER_PATTERN = re.compile(
    r"^(?P<event>\d{3}) \((?P<cluster>\d+)\.(?P<proc>\d+)\.\d+\)"
)
ULOG_NODE_PATTERN = re.compile(r"^\s*DAG Node: (?P<name>\S+)\s*$")
TERMINAL_STATUSES = {"failed", "done", "skipped"}


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
    state = read_json(run_dir / "state.json")

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

    next_step = state.get("next_step")
    if not isinstance(next_step, int) or isinstance(next_step, bool) or next_step < 0:
        raise ValueError("state.json has no valid next_step")
    return {
        "run_dir": run_dir,
        "dag_file": expected_dag,
        "run_record": run_record,
        "submission": submission,
        "state": state,
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


def read_lines(path: Path) -> list[str] | None:
    if not path.is_file():
        return None
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None


def current_startup_segment(
    lines: list[str] | None,
    cluster: int,
    *,
    additional_clusters: Iterable[int] = (),
) -> list[str] | None:
    """Return the last contiguous run segment for a cluster, or no evidence."""
    if lines is None:
        return None
    starts = []
    for index, line in enumerate(lines):
        match = STARTUP_PATTERN.search(line) or WORKFLOW_BATCH_PATTERN.search(line)
        if match:
            starts.append((index, int(match.group("cluster"))))
    allowed_clusters = {cluster, *additional_clusters}
    if not starts or starts[-1][1] not in allowed_clusters:
        return None
    first = len(starts) - 1
    while first and starts[first - 1][1] == cluster:
        first -= 1
    return lines[starts[first][0]:]


def event_cluster_ids(lines: Iterable[str]) -> set[int]:
    found: set[int] = set()
    for line in lines:
        for match in re.finditer(r"\((\d+)\.\d+\.\d+\)", line):
            found.add(int(match.group(1)))
    return found


def parse_dagman_states(
    lines: list[str],
    planned: dict[str, PlannedNode],
    source: str,
) -> tuple[dict[str, dict], dict[str, int | None]]:
    states: dict[str, dict] = {}
    wave_clusters: dict[str, int | None] = {}
    next_wave: str | None = None

    def set_state(name: str, status: str, line_number: int) -> None:
        if name in planned:
            states[name] = {
                "status": status,
                "source": f"{source}:{line_number}",
                "last_observed": True,
            }

    for index, line in enumerate(lines, start=1):
        submit = NODE_SUBMIT_PATTERN.search(line)
        if submit:
            next_wave = submit.group("name")
        assigned = ASSIGNED_PATTERN.search(line)
        if assigned and next_wave:
            wave_clusters[next_wave] = int(assigned.group("cluster"))
            next_wave = None

        pre_failure = PRE_FAILURE_PATTERN.search(line)
        if pre_failure:
            name = pre_failure.group("name")
            node = planned.get(name)
            if node and node.pre_skip_code == int(pre_failure.group("code")):
                continue
            set_state(name, "failed", index)
            continue

        preskip = PRESKIP_EVENT_PATTERN.search(line)
        if preskip:
            set_state(preskip.group("name"), "skipped", index)
            continue

        event = DAG_EVENT_PATTERN.search(line)
        if event:
            name = event.group("name")
            event_name = event.group("event")
            if event_name == "SUBMIT":
                status = "queued"
                if name.startswith("WAVE_"):
                    wave_clusters[name] = int(event.group("cluster"))
            elif event_name == "EXECUTE":
                status = "running"
            elif event_name == "JOB_HELD":
                status = "held"
            elif event_name == "JOB_RELEASED":
                status = "queued"
            elif event_name in {"JOB_ABORTED", "JOB_EVICTED"}:
                status = "queued"
            else:
                status = ""
            if status:
                set_state(name, status, index)
            continue

        success = SUCCESS_PATTERN.search(line)
        if success:
            set_state(success.group(1), "done", index)
            continue

        lowered = line.lower()
        if " failed" in lowered or "status_error" in lowered:
            match = re.search(r"\bNode\s+(\S+)\b", line)
            if match:
                set_state(match.group(1), "failed", index)

    return states, wave_clusters


def parse_root_exit(lines: list[str] | None) -> tuple[int | None, str | None]:
    if lines is None:
        return None, None
    matches = [EXIT_PATTERN.search(line) for line in lines]
    statuses = [int(match.group("status")) for match in matches if match]
    if not statuses:
        return None, None
    return statuses[-1], "run.dag.dagman.out"


def matching_userlog_terminal(
    path: Path,
    allowed_clusters: set[int],
) -> tuple[dict[str, dict], set[int]]:
    lines = read_lines(path)
    if lines is None:
        return {}, set()
    blocks: list[tuple[int, int, list[str]]] = []
    current: tuple[int, int, list[str]] | None = None
    for line in lines:
        header = ULOG_HEADER_PATTERN.match(line)
        if header:
            if current is not None:
                blocks.append(current)
            current = (int(header.group("event")), int(header.group("cluster")), [line])
        elif current is not None:
            current[2].append(line)
    if current is not None:
        blocks.append(current)

    job_nodes: dict[tuple[int, int], str] = {}
    states: dict[str, dict] = {}
    used_clusters: set[int] = set()
    for event_code, cluster, block in blocks:
        if cluster not in allowed_clusters:
            continue
        used_clusters.add(cluster)
        header = ULOG_HEADER_PATTERN.match(block[0])
        assert header is not None
        job_key = (cluster, int(header.group("proc")))
        for line in block:
            node = ULOG_NODE_PATTERN.match(line)
            if node:
                job_nodes[job_key] = node.group("name")
        name = job_nodes.get(job_key)
        if not name:
            continue

        status: str | None = None
        if event_code == 0:
            status = "queued"
        elif event_code == 1:
            status = "running"
        elif event_code in {4, 13}:
            status = "queued"
        elif event_code == 12:
            status = "held"
        elif event_code == 5:
            text = "\n".join(block)
            return_code = re.search(r"return value (\d+)", text, re.IGNORECASE)
            status = (
                "done"
                if return_code and int(return_code.group(1)) == 0
                else "failed"
            )
        elif event_code == 9:
            status = "failed"
        if status:
            states[name] = {
                "status": status,
                "source": str(path),
                "last_observed": True,
                "job_id": f"{cluster}.{job_key[1]}",
            }
    return states, used_clusters


def load_metrics(run_dir: Path, cluster: int) -> tuple[dict | None, str]:
    path = run_dir / "run.dag.metrics"
    try:
        metrics = read_json(path, required=False)
    except ValueError:
        return None, "invalid"
    if metrics is None:
        return None, "missing"
    try:
        metrics_cluster = int(metrics.get("dagman_id"))
    except (TypeError, ValueError):
        return None, "invalid"
    if metrics_cluster != cluster:
        return None, "stale"
    return metrics, "current"


def read_root_userlog(run_dir: Path, cluster: int) -> tuple[int | None, str | None]:
    path = run_dir / "run.dag.dagman.log"
    lines = read_lines(path)
    if lines is None:
        return None, None
    code: int | None = None
    result: str | None = None
    blocks: list[list[str]] = []
    current: list[str] | None = None
    for line in lines:
        if ULOG_HEADER_PATTERN.match(line):
            if current is not None:
                blocks.append(current)
            current = [line]
        elif current is not None:
            current.append(line)
    if current is not None:
        blocks.append(current)
    for block in blocks:
        header = ULOG_HEADER_PATTERN.match(block[0])
        if not header or int(header.group("cluster")) != cluster:
            continue
        if int(header.group("event")) == 5:
            text = "\n".join(block)
            match = re.search(r"return value (\d+)", text, re.IGNORECASE)
            if match:
                code = int(match.group(1))
                result = str(path)
    return code, result


def queue_node_states(
    records: list[dict],
) -> tuple[dict[str, dict], dict[str, Counter]]:
    selected: dict[str, dict] = {}
    root_statuses: dict[str, Counter] = {
        "payload": Counter(),
        "controllers": Counter(),
    }
    for record in records:
        try:
            job_status = int(record.get("JobStatus") or 0)
        except (TypeError, ValueError):
            continue
        status = LIVE_STATUS.get(job_status)
        if not status:
            continue
        node_name = record.get("DAGNodeName")
        if isinstance(node_name, str) and node_name:
            root_statuses["payload"][status] += 1
            candidate = dict(record, _snapshot_status=status, _snapshot_source="queue")
            current = selected.get(node_name)
            if current is None or QUEUE_PRIORITY.get(status, 9) < QUEUE_PRIORITY.get(
                current["_snapshot_status"], 9
            ):
                selected[node_name] = candidate
        elif record.get("JobUniverse") == 7:
            root_statuses["controllers"][status] += 1
    return selected, root_statuses


def task_stage(run_dir: Path, node: PlannedNode) -> str:
    task_path = run_dir / "tasks" / f"{node.name}.json"
    task = read_json(task_path, required=False)
    stage = task.get("stage") if task else None
    return stage if isinstance(stage, str) and stage else node.stage


def resolve_node_status(
    name: str,
    queue_states: dict[str, dict],
    dag_states: dict[str, dict],
    userlog_states: dict[str, dict],
) -> dict:
    live = queue_states.get(name)
    if live:
        status = live["_snapshot_status"]
        if status in {"running", "queued", "held"}:
            return {"status": status, "source": "queue", "record": live}
    dag = dag_states.get(name)
    userlog = userlog_states.get(name)
    for record in (dag, userlog):
        if record and record.get("status") in TERMINAL_STATUSES:
            return dict(record)
    if live:
        return {"status": live["_snapshot_status"], "source": "queue", "record": live}
    if userlog:
        return dict(userlog)
    if dag:
        return dict(dag)
    return {"status": "pending", "source": "none", "record": None}


def counts_template() -> dict[str, int]:
    return {status: 0 for status in STATUSES}


def summarize_nodes(
    nodes: Iterable[PlannedNode],
    run_dir: Path,
    queue_states: dict[str, dict],
    dag_states: dict[str, dict],
    userlog_states: dict[str, dict],
) -> dict:
    counts = counts_template()
    key_nodes: list[dict] = []
    ordered_nodes = sorted(nodes, key=lambda item: item.name)
    resolved = {}
    for node in ordered_nodes:
        state = resolve_node_status(node.name, queue_states, dag_states, userlog_states)
        resolved[node.name] = state
        counts[state["status"]] += 1
        record = state.get("record") or {}
        if state["status"] in {"running", "queued", "held", "failed", "pending"}:
            key_nodes.append({
                "name": node.name,
                "stage": task_stage(run_dir, node),
                "status": state["status"],
                "source": state.get("source", ""),
                "last_observed": bool(state.get("last_observed", False)),
                "job_id": (
                    f"{record.get('ClusterId', '?')}.{record.get('ProcId', '?')}"
                    if record.get("ClusterId") is not None else record.get("job_id", "")
                ),
                "hold_reason": record.get("HoldReason") or "",
            })
    if not key_nodes:
        for node in ordered_nodes:
            state = resolved[node.name]
            if state["status"] in {"done", "skipped"}:
                key_nodes.append({
                    "name": node.name,
                    "stage": task_stage(run_dir, node),
                    "status": state["status"],
                    "source": state.get("source", ""),
                    "last_observed": bool(state.get("last_observed", False)),
                    "job_id": "",
                    "hold_reason": "",
                })
                if len(key_nodes) >= 5:
                    break
    terminal_evidence = all(
        resolved[node.name]["status"] in TERMINAL_STATUSES
        for node in ordered_nodes
        if resolved[node.name]["status"] != "pending"
    )
    return {
        "counts": counts,
        "total": len(ordered_nodes),
        "completed": counts["done"] + counts["skipped"],
        "key_nodes": key_nodes[:25],
        "resolved": resolved,
        "terminal_evidence": terminal_evidence,
    }


def stage_summaries(
    nodes: Iterable[PlannedNode],
    run_dir: Path,
    queue_states: dict[str, dict],
    dag_states: dict[str, dict],
    userlog_states: dict[str, dict],
) -> dict[str, dict]:
    grouped: dict[str, list[PlannedNode]] = defaultdict(list)
    for node in nodes:
        grouped[task_stage(run_dir, node)].append(node)
    return {
        stage: summarize_nodes(items, run_dir, queue_states, dag_states, userlog_states)
        for stage, items in sorted(grouped.items())
    }


def artifact_key(reference: dict) -> tuple[str, str]:
    return str(reference.get("artifact_id") or ""), str(reference.get("url") or "")


def add_artifact(
    collection: dict[tuple[str, str], dict],
    reference: dict,
    *,
    task_id: str,
    result_status: str,
    committed: bool,
    step: int | None,
    source: str,
    role: str,
) -> dict:
    key = artifact_key(reference)
    item = collection.setdefault(key, {
        "task_ids": [],
        "artifact_id": reference.get("artifact_id", ""),
        "kind": reference.get("kind", ""),
        "result_status": result_status or "",
        "merged": bool(committed),
        "step": step if step is not None else "",
        "events": reference.get("events", ""),
        "size_bytes": reference.get("size_bytes", ""),
        "url": reference.get("url", ""),
        "sidecar_url": reference.get("sidecar_url", ""),
        "archive_member": False,
        "reclaimed_staging": False,
        "remote_presence": "not_checked",
        "credential_sources": [],
    })
    if task_id and task_id not in item["task_ids"]:
        item["task_ids"].append(task_id)
    for field in ("kind", "events", "size_bytes", "url", "sidecar_url"):
        if item.get(field) in (None, "") and reference.get(field) not in (None, ""):
            item[field] = reference[field]
    if not item.get("result_status") and result_status:
        item["result_status"] = result_status
    item["merged"] = bool(item["merged"] or committed)
    if step is not None and item["step"] in (None, ""):
        item["step"] = step
    evidence = {
        "source": source,
        "role": role,
        "task_id": task_id or "",
        "result_status": result_status or "",
    }
    if evidence not in item["credential_sources"]:
        item["credential_sources"].append(evidence)
    if role == "archive_member":
        item["archive_member"] = True
    elif role == "reclaimed_staging":
        item["reclaimed_staging"] = True
    return item


def collect_artifacts(run_dir: Path, state: dict) -> dict:
    next_step = int(state["next_step"])
    committed: dict[tuple[str, str], dict] = {}
    uncommitted: dict[tuple[str, str], dict] = {}
    committed_results: dict[str, tuple[int, str]] = {}
    wave_dir = run_dir / "ledger" / "waves"
    if wave_dir.is_dir():
        for path in sorted(wave_dir.glob("*.json")):
            receipt = read_json(path)
            step = receipt.get("step")
            if not isinstance(step, int) or isinstance(step, bool):
                continue
            if step >= next_step:
                continue
            results = receipt.get("results", {})
            if not isinstance(results, dict):
                continue
            for task_id, result in results.items():
                if not isinstance(result, dict):
                    continue
                committed_results[task_id] = (step, str(result.get("status") or ""))
                for field in ("outputs", "originals", "members"):
                    references = result.get(field, [])
                    if not isinstance(references, list):
                        continue
                    for index, reference in enumerate(references):
                        if isinstance(reference, dict):
                            add_artifact(
                                committed,
                                reference,
                                task_id=task_id,
                                result_status=str(result.get("status") or ""),
                                committed=True,
                                step=step,
                                source=f"{path}#results/{task_id}/{field}/{index}",
                                role=(
                                    "archive_member"
                                    if field == "members"
                                    else "worker_result"
                                ),
                            )

    archive_dir = run_dir / "ledger" / "archives"
    if archive_dir.is_dir():
        for path in sorted(archive_dir.glob("*.json")):
            receipt = read_json(path)
            step = receipt.get("step")
            if not isinstance(step, int) or isinstance(step, bool) or step >= next_step:
                continue
            task_id = str(receipt.get("task_id") or "")
            result_status = committed_results.get(task_id, (step, ""))[1]
            for index, reference in enumerate(receipt.get("outputs", [])):
                if isinstance(reference, dict):
                    add_artifact(
                        committed,
                        reference,
                        task_id=task_id,
                        result_status=result_status,
                        committed=True,
                        step=step,
                        source=f"{path}#outputs/{index}",
                        role="archive_output",
                    )
            for index, reference in enumerate(receipt.get("members", [])):
                if isinstance(reference, dict):
                    add_artifact(
                        committed,
                        reference,
                        task_id=task_id,
                        result_status=result_status,
                        committed=True,
                        step=step,
                        source=f"{path}#members/{index}",
                        role="archive_member",
                    )

    for task_id, (step, result_status) in committed_results.items():
        result_path = run_dir / "ledger" / "waves" / f"{step:06d}.json"
        receipt = read_json(result_path)
        result = receipt.get("results", {}).get(task_id, {})
        for entry in result.get("reclaimed", []) if isinstance(result, dict) else []:
            if not isinstance(entry, dict):
                continue
            for object_index, obj in enumerate(entry.get("objects", [])):
                if not isinstance(obj, dict):
                    continue
                reference = {
                    "artifact_id": entry.get("artifact_id", ""),
                    "url": obj.get("url", ""),
                    "kind": obj.get("kind", ""),
                }
                add_artifact(
                    committed,
                    reference,
                    task_id=task_id,
                    result_status=result_status,
                    committed=True,
                    step=step,
                    source=(f"{result_path}#results/{task_id}/reclaimed/"
                            f"{entry.get('artifact_id', '')}/objects/{object_index}"),
                    role="reclaimed_staging",
                )

    pending = state.get("pending") or {}
    task_ids = pending.get("task_ids", []) if isinstance(pending, dict) else []
    result_dir = run_dir / "results"
    if isinstance(task_ids, list) and result_dir.is_dir():
        for task_id in task_ids:
            if not isinstance(task_id, str) or Path(task_id).name != task_id:
                continue
            path = result_dir / f"{task_id}.json"
            result = read_json(path, required=False)
            if not result:
                continue
            status = str(result.get("status") or "")
            step = pending.get("step") if isinstance(pending.get("step"), int) else None
            for field in ("outputs", "originals"):
                references = result.get(field, [])
                if not isinstance(references, list):
                    continue
                for index, reference in enumerate(references):
                    if isinstance(reference, dict):
                        add_artifact(
                            uncommitted,
                            reference,
                            task_id=task_id,
                            result_status=status,
                            committed=False,
                            step=step,
                            source=f"{path}#{field}/{index}",
                            role="uncommitted_result",
                        )

    def values(collection: dict[tuple[str, str], dict]) -> list[dict]:
        output = []
        for item in collection.values():
            item["task_ids"].sort()
            output.append(item)
        return sorted(output, key=lambda item: (item["artifact_id"], item["url"]))

    return {
        "committed": values(committed),
        "uncommitted": values(uncommitted),
        "remote_stat_checked": False,
    }


def count_fields(counts: dict) -> dict:
    return {status: int(counts.get(status, 0)) for status in STATUSES}


def format_counts(summary: dict) -> str:
    counts = summary["counts"]
    complete = counts.get("done", 0) + counts.get("skipped", 0)
    return (
        f"{complete}/{summary['total']} complete "
        f"({counts.get('done', 0)} done, {counts.get('skipped', 0)} skipped); "
        + ", ".join(f"{status}={counts.get(status, 0)}" for status in STATUSES
                     if status not in {"done", "skipped"})
    )


def public_summary(summary: dict) -> dict:
    return {
        "counts": count_fields(summary["counts"]),
        "total": summary["total"],
        "completed": summary["completed"],
        "key_nodes": summary["key_nodes"],
    }


def build_snapshot(
    run_dir: Path,
    *,
    schedd: str | None = None,
    cluster: int | str | None = None,
    queue_query: Callable[[str, int], list[dict]] = query_queue,
) -> dict:
    identity = resolve_identity(run_dir, schedd, cluster)
    run_dir = identity["run_dir"]
    root_dag = identity["dag_file"]
    root_graph = parse_dag_graph(root_dag)
    root_nodes = list(root_graph.nodes.values())
    flattened = flatten_graph(root_graph)

    queue_state = {"status": "ok", "error": ""}
    try:
        queue_records = queue_query(identity["schedd"], identity["cluster"])
    except (
        OSError,
        subprocess.SubprocessError,
        RuntimeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        queue_records = []
        detail = getattr(exc, "stderr", None)
        queue_state = {
            "status": "error",
            "error": detail.strip() if detail else str(exc),
        }
    else:
        queue_state["status"] = "empty" if not queue_records else "records"
    queue_states, queue_counts = queue_node_states(queue_records)

    root_out_path = Path(f"{root_dag}.dagman.out")
    root_segment = current_startup_segment(
        read_lines(root_out_path), identity["cluster"]
    )
    root_dag_states: dict[str, dict] = {}
    wave_clusters: dict[str, int | None] = {}
    evidence_missing: list[str] = []
    if root_segment is None:
        evidence_missing.append("current root DAGMan output segment")
    else:
        root_dag_states, wave_clusters = parse_dagman_states(
            root_segment, root_graph.nodes, str(root_out_path)
        )

    wave_data = []
    all_dag_states = dict(root_dag_states)
    userlog_states: dict[str, dict] = {}
    matching_child_segments = 0
    current_log_clusters = {identity["cluster"]}
    if root_segment is not None:
        current_log_clusters.update(event_cluster_ids(root_segment))
    child_terminal_evidence: dict[str, bool] = {}
    child_graphs: dict[str, DagGraph] = {}
    for node in sorted(root_nodes, key=lambda item: item.name):
        if node.directive != "SUBDAG" or not node.name.startswith("WAVE_"):
            continue
        child_graph = root_graph.children.get(node.name)
        submitted_cluster = wave_clusters.get(node.name)
        child_states: dict[str, dict] = {}
        child_userlog_states: dict[str, dict] = {}
        child_segment: list[str] | None = None
        if child_graph is not None:
            child_graphs[node.name] = child_graph
        root_node_status = resolve_node_status(
            node.name, queue_states, root_dag_states, {}
        )["status"]
        if submitted_cluster is not None and child_graph is not None:
            child_out = Path(f"{child_graph.dag_file}.dagman.out")
            child_segment = current_startup_segment(
                read_lines(child_out), submitted_cluster,
                # Modern HTCondor writes the root DAGMan batch ID into each
                # recursively submitted SubDAG log, not the SubDAG node's
                # submit cluster ID. Accept that root identity while still
                # rejecting a segment from another attempt.
                additional_clusters=(identity["cluster"],),
            )
            if child_segment is None:
                evidence_missing.append(f"current {node.name} SubDAG log segment")
            else:
                matching_child_segments += 1
                child_states, _ = parse_dagman_states(
                    child_segment, child_graph.nodes, str(child_out)
                )
                current_log_clusters.update(event_cluster_ids(child_segment))
            node_log = Path(f"{child_graph.dag_file}.nodes.log")
            states, clusters = matching_userlog_terminal(node_log, current_log_clusters)
            child_userlog_states.update(states)
            current_log_clusters.update(clusters)
        elif root_node_status == "done":
            evidence_missing.append(f"submitted child DAG for {node.name}")

        # Queue records are current. Terminal states come from the matching
        # child DAGMan segment; event logs fill gaps for its worker tasks.
        all_dag_states.update(child_states)
        userlog_states.update(child_userlog_states)
        children = list(child_graph.nodes.values()) if child_graph else []
        child_summary = summarize_nodes(
            children, run_dir, queue_states, child_states, child_userlog_states
        )
        child_terminal_evidence[node.name] = (
            child_graph is not None
            and child_segment is not None
            and all(child_summary["resolved"][item.name]["status"] in TERMINAL_STATUSES
                    for item in children)
        )
        stage_summary = stage_summaries(
            children, run_dir, queue_states, child_states, child_userlog_states
        )
        wave_data.append({
            "name": node.name,
            "cluster": submitted_cluster if submitted_cluster is not None else "",
            "status": root_node_status,
            "summary": public_summary(child_summary),
            "stages": {
                name: public_summary(item)
                for name, item in stage_summary.items()
            },
            "child_log_matched": child_segment is not None,
        })

    # Re-read all current-attempt user logs with the complete cluster allowlist,
    # so jobs observed before a later wave was discovered are not discarded.
    root_node_log = run_dir / "run.dag.nodes.log"
    root_user_states, root_user_clusters = matching_userlog_terminal(
        root_node_log, current_log_clusters
    )
    userlog_states.update(root_user_states)
    current_log_clusters.update(root_user_clusters)
    event_log = run_dir / "logs" / "events.log"
    event_states, event_clusters = matching_userlog_terminal(
        event_log, current_log_clusters
    )
    userlog_states.update(event_states)
    current_log_clusters.update(event_clusters)
    for wave_name, child_graph in child_graphs.items():
        node_log = Path(f"{child_graph.dag_file}.nodes.log")
        states, clusters = matching_userlog_terminal(node_log, current_log_clusters)
        userlog_states.update(states)
        current_log_clusters.update(clusters)

    # Recompute wave summaries after merging current-attempt worker user logs.
    for wave in wave_data:
        child = child_graphs.get(wave["name"])
        if child is None:
            continue
        dag_states = {}
        child_out = Path(f"{child.dag_file}.dagman.out")
        segment = current_startup_segment(
            read_lines(child_out),
            int(wave["cluster"]) if wave["cluster"] else -1,
            additional_clusters=(identity["cluster"],),
        )
        if segment is not None:
            dag_states, _ = parse_dagman_states(segment, child.nodes, str(child_out))
        summary = summarize_nodes(
            child.nodes.values(), run_dir, queue_states, dag_states, userlog_states
        )
        wave["summary"] = public_summary(summary)
        stages = stage_summaries(
            child.nodes.values(), run_dir, queue_states, dag_states, userlog_states
        )
        wave["stages"] = {name: public_summary(item) for name, item in stages.items()}
        child_terminal_evidence[wave["name"]] = (
            wave["child_log_matched"]
            and all(
                summary["resolved"][item.name]["status"] in TERMINAL_STATUSES
                for item in child.nodes.values()
            )
        )
        all_dag_states.update(dag_states)

    root_summary_internal = summarize_nodes(
        root_nodes, run_dir, queue_states, root_dag_states, root_user_states
    )
    root_summary = public_summary(root_summary_internal)
    all_task_nodes = [
        node for name, node in flattened.items() if name not in root_graph.nodes
    ]
    all_stages = stage_summaries(
        all_task_nodes, run_dir, queue_states, all_dag_states, userlog_states
    )
    public_stages = {name: public_summary(item) for name, item in all_stages.items()}

    metrics, metrics_status = load_metrics(run_dir, identity["cluster"])
    out_exit, _ = parse_root_exit(root_segment)
    userlog_exit, _ = read_root_userlog(run_dir, identity["cluster"])
    exit_evidence = [("dagman_out", out_exit), ("dagman_log", userlog_exit)]
    if metrics is not None:
        try:
            exit_evidence.append(("metrics", int(metrics.get("exitcode"))))
        except (TypeError, ValueError):
            pass
    metrics_dag_status = None
    if metrics is not None:
        try:
            metrics_dag_status = int(metrics.get("DagStatus"))
        except (TypeError, ValueError):
            pass
    metrics_exit = next(
        (code for name, code in exit_evidence if name == "metrics"), None
    )
    metrics_conflict = (
        metrics_exit is not None
        and metrics_dag_status is not None
        and metrics_exit != metrics_dag_status
    )
    known_exit_codes = {code for _, code in exit_evidence if code is not None}
    state_complete = identity["state"].get("status") == "complete"
    complete_file = (run_dir / ".complete").is_file()
    root_terminal = all(
        root_summary_internal["resolved"][item.name]["status"] in TERMINAL_STATUSES
        for item in root_nodes
    )
    ran_waves = [wave for wave in wave_data if wave["status"] != "skipped"]
    children_terminal = all(child_terminal_evidence.get(wave["name"], False)
                            for wave in ran_waves if wave["status"] == "done")
    controller_active = any(
        status_count for status, status_count in queue_counts["controllers"].items()
        if status in {"running", "queued", "held"}
    )
    active_nodes = any(
        count for summary in [root_summary, *[
            wave["summary"] for wave in wave_data
        ], *public_stages.values()]
        for status, count in summary["counts"].items()
        if status in {"running", "queued", "held"}
    )
    has_failed = bool(
        root_summary["counts"]["failed"]
        or queue_counts["controllers"].get("failed", 0)
        or any(item["counts"]["failed"] for item in public_stages.values())
    )
    has_held = bool(
        root_summary["counts"]["held"]
        or any(item["counts"]["held"] for item in public_stages.values())
    )
    if queue_counts["controllers"].get("held", 0) or has_held:
        overall = "held"
    elif controller_active or active_nodes:
        overall = "active"
    elif (
        len(known_exit_codes) > 1
        or metrics_conflict
        or state_complete != complete_file
        or (known_exit_codes == {0} and not state_complete)
        or (known_exit_codes == {1} and state_complete)
        or (known_exit_codes == {0} and has_failed)
    ):
        overall = "inconsistent"
    elif has_failed or (known_exit_codes == {1}):
        overall = "failed"
    elif (known_exit_codes == {0} and state_complete and complete_file
          and root_terminal and children_terminal and not evidence_missing):
        overall = "complete"
    elif state_complete and (known_exit_codes == {0}) and evidence_missing:
        overall = "unknown"
    elif not root_segment:
        overall = "unknown"
    else:
        overall = "unknown"

    artifacts = collect_artifacts(run_dir, identity["state"])
    root_log_evidence = {
        "dagman_out_segment": root_segment is not None,
        "dagman_log_exit": userlog_exit is not None,
        "metrics": metrics_status,
        "metrics_dag_status": (
            metrics_dag_status if metrics_dag_status is not None else ""
        ),
        "matching_wave_logs": matching_child_segments,
        "missing": evidence_missing,
    }
    return {
        "run_dir": str(run_dir),
        "attempt": identity["attempt_number"],
        "schedd": identity["schedd"],
        "cluster": identity["cluster"],
        "overall": overall,
        "root_exit_code": (
            next(iter(known_exit_codes)) if len(known_exit_codes) == 1 else ""
        ),
        "state_status": identity["state"].get("status", ""),
        "complete_marker": complete_file,
        "queue": {
            **queue_state,
            "counts": {
                kind: {status: int(values.get(status, 0)) for status in STATUSES}
                for kind, values in queue_counts.items()
            },
        },
        "root": root_summary,
        "waves": wave_data,
        "stages": public_stages,
        "artifacts": artifacts,
        "remote_stat_checked": False,
        "evidence": root_log_evidence,
    }


def render_artifact(item: dict) -> str:
    task_ids = ",".join(item.get("task_ids", []))
    roles = ",".join(sorted({
        source.get("role", "") for source in item.get("credential_sources", [])
        if source.get("role")
    }))
    return (
        f"  {item.get('artifact_id', '')} [{item.get('kind', '')}] "
        f"task={task_ids} status={item.get('result_status', '')} "
        f"merged={str(item.get('merged', False)).lower()} role={roles} "
        f"events={item.get('events', '')} size_bytes={item.get('size_bytes', '')}\n"
        f"    url={item.get('url', '')}\n"
        f"    sidecar_url={item.get('sidecar_url', '')}"
    )


def render_text(snapshot: dict) -> str:
    queue_counts = snapshot["queue"]["counts"]

    def live_counts(kind: str) -> str:
        values = queue_counts[kind]
        parts = [f"{name}={count}" for name, count in values.items() if count]
        return ", ".join(parts) or "none"

    lines = [
        f"mcprod run: {snapshot['run_dir']}",
        f"Attempt {snapshot['attempt']}: {snapshot['schedd']} / {snapshot['cluster']}",
        f"Overall: {snapshot['overall']} (state={snapshot['state_status']}, "
        f".complete={str(snapshot['complete_marker']).lower()}, "
        f"root_exit={snapshot['root_exit_code']})",
        f"Queue: {snapshot['queue']['status']} (controllers: "
        f"{live_counts('controllers')}; payload: {live_counts('payload')})",
        f"Root DAG: {format_counts(snapshot['root'])}",
    ]
    for node in snapshot["root"]["key_nodes"]:
        observed = " [last observed]" if node.get("last_observed") else ""
        reason = f"; {node['hold_reason']}" if node.get("hold_reason") else ""
        lines.append(
            f"  {node['status']:<7} {node['name']}{observed}{reason}"
        )
    if snapshot["queue"].get("error"):
        lines.append(f"  queue query error: {snapshot['queue']['error']}")
    for wave in snapshot["waves"]:
        counts = wave["summary"]["counts"]
        completed = wave["summary"]["completed"]
        total = wave["summary"]["total"]
        lines.append(
            f"Wave {wave['name']} (cluster={wave['cluster'] or '—'}, "
            f"status={wave['status']}): {completed}/{total} child nodes complete "
            f"({counts['done']} done, {counts['skipped']} skipped, "
            f"{counts['failed']} failed)"
        )
        for stage, summary in sorted(wave["stages"].items()):
            lines.append(f"  {stage}: {format_counts(summary)}")
        for node in wave["summary"]["key_nodes"]:
            observed = " [last observed]" if node.get("last_observed") else ""
            lines.append(
                f"    {node['status']:<7} {node['stage']:<10} "
                f"{node['name']}{observed}"
            )
    if snapshot["stages"]:
        lines.append("Stages across executed waves:")
        for stage, summary in sorted(snapshot["stages"].items()):
            lines.append(f"  {stage}: {format_counts(summary)}")
            for node in summary["key_nodes"]:
                if node["status"] not in {"done", "skipped"}:
                    observed = " [last observed]" if node.get("last_observed") else ""
                    lines.append(f"    {node['status']:<7} {node['name']}{observed}")
    artifacts = snapshot["artifacts"]
    lines.append(f"Committed artifact credentials ({len(artifacts['committed'])}):")
    lines.extend(render_artifact(item) for item in artifacts["committed"])
    lines.append(f"Uncommitted current results ({len(artifacts['uncommitted'])}):")
    lines.extend(render_artifact(item) for item in artifacts["uncommitted"])
    lines.append("Remote files were not independently stat'ed.")
    lines.append(
        "Evidence: "
        f"root segment={str(snapshot['evidence']['dagman_out_segment']).lower()}, "
        f"metrics={snapshot['evidence']['metrics']}, "
        f"matched wave logs={snapshot['evidence']['matching_wave_logs']}"
    )
    if snapshot["evidence"]["missing"]:
        lines.append("Missing evidence: " + "; ".join(snapshot["evidence"]["missing"]))
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
