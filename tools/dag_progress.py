#!/usr/bin/env python3
"""Render stage-by-stage progress for an HTCondor DAG and its SubDAGs."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import textwrap
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


STAGES = (
    ("lhe", "LHE"),
    ("plan", "Plan"),
    ("coordinate", "Coordinate"),
    ("subdag", "SubDAG"),
    ("processing", "Process"),
    ("merge", "Merge"),
    ("ntuple", "Ntuple"),
    ("final", "Final"),
)
STAGE_LABELS = dict(STAGES)
STAGE_INDEX = {stage: index for index, (stage, _) in enumerate(STAGES)}
STATUS_ORDER = ("done", "running", "idle", "held", "failed", "pending")
STATUS_LABELS = {
    "done": "DONE",
    "running": "RUN",
    "idle": "IDLE",
    "held": "HELD",
    "failed": "FAIL",
    "pending": "WAIT",
}
STATUS_SYMBOLS = {
    "done": "✓",
    "running": "▶",
    "idle": "○",
    "held": "!",
    "failed": "✗",
    "pending": "·",
}
STATUS_COLORS = {
    "done": "32",
    "running": "36",
    "idle": "33",
    "held": "35",
    "failed": "31",
    "pending": "2",
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


@dataclass
class PlannedNode:
    name: str
    stage: str
    dag_file: Path
    directive: str = "JOB"
    category: str | None = None
    child_dag: Path | None = None


@dataclass
class DagGraph:
    dag_file: Path
    nodes: dict[str, PlannedNode] = field(default_factory=dict)
    edges: list[tuple[str, str]] = field(default_factory=list)
    maxjobs: dict[str, int] = field(default_factory=dict)
    children: dict[str, DagGraph | None] = field(default_factory=dict)


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
    if directive == "FINAL":
        return "final"
    if directive == "SUBDAG":
        return "subdag"
    if name.startswith("LHE_"):
        return "lhe"
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
    if "_BLOCK" in name or name.startswith(("MIX_", "PROC_", "PROCESS_")):
        return "processing"
    return "processing"


def parse_dag_graph(root_dag: Path) -> DagGraph:
    visited: set[Path] = set()

    def visit(dag_file: Path) -> DagGraph | None:
        dag_file = dag_file.resolve()
        if not dag_file.is_file():
            return None
        if dag_file in visited:
            raise RuntimeError(f"recursive SubDAG reference involving {dag_file}")
        visited.add(dag_file)
        graph = DagGraph(dag_file=dag_file)
        categories: dict[str, str] = {}
        child_paths: dict[str, Path] = {}
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
                graph.nodes[name] = PlannedNode(
                    name=name,
                    stage=node_stage(name, directive),
                    dag_file=dag_file,
                    directive=directive,
                )
            elif directive == "FINAL" and len(fields) >= 2:
                name = fields[1]
                graph.nodes[name] = PlannedNode(
                    name,
                    "final",
                    dag_file,
                    directive=directive,
                )
            elif (
                directive == "SUBDAG"
                and len(fields) >= 4
                and fields[1].upper() == "EXTERNAL"
            ):
                name = fields[2]
                graph.nodes[name] = PlannedNode(
                    name,
                    "subdag",
                    dag_file,
                    directive=directive,
                )
                child = Path(fields[3])
                if not child.is_absolute():
                    child = dag_file.parent / child
                child = child.resolve()
                graph.nodes[name].child_dag = child
                child_paths[name] = child
            elif directive == "PARENT" and "CHILD" in (
                field.upper() for field in fields
            ):
                child_index = next(
                    index
                    for index, field_value in enumerate(fields)
                    if field_value.upper() == "CHILD"
                )
                parents = fields[1:child_index]
                children = fields[child_index + 1 :]
                graph.edges.extend(
                    (parent, child)
                    for parent in parents
                    for child in children
                )
            elif directive == "CATEGORY" and len(fields) >= 3:
                categories[fields[1]] = fields[2]
            elif directive == "MAXJOBS" and len(fields) >= 3:
                try:
                    graph.maxjobs[fields[1]] = int(fields[2])
                except ValueError:
                    continue

        for name, category in categories.items():
            if name in graph.nodes:
                graph.nodes[name].category = category
        for name, child_path in child_paths.items():
            graph.children[name] = visit(child_path)
        visited.remove(dag_file)
        return graph

    graph = visit(root_dag)
    if graph is None:
        raise RuntimeError(f"DAG file does not exist: {root_dag}")
    return graph


def flatten_graph(graph: DagGraph) -> dict[str, PlannedNode]:
    planned: dict[str, PlannedNode] = {}

    def visit(current: DagGraph) -> None:
        planned.update(current.nodes)
        for child in current.children.values():
            if child is not None:
                visit(child)

    visit(graph)
    return planned


def parse_dag_tree(root_dag: Path) -> dict[str, PlannedNode]:
    return flatten_graph(parse_dag_graph(root_dag))


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


def colorize(text: str, status: str, enabled: bool) -> str:
    if not enabled:
        return text
    return f"\033[{STATUS_COLORS[status]}m{text}\033[0m"


def status_marker(status: str, enabled: bool) -> str:
    return colorize(STATUS_SYMBOLS[status], status, enabled)


def status_counts_text(counts: Counter, color_enabled: bool) -> str:
    parts = []
    for status in STATUS_ORDER:
        count = counts[status]
        if count:
            parts.append(
                colorize(
                    f"{STATUS_SYMBOLS[status]}{count}",
                    status,
                    color_enabled,
                )
            )
    return " ".join(parts) or colorize("·0", "pending", color_enabled)


class LabelFormatter:
    def __init__(self, width: int):
        self.width = width
        self.legend: dict[str, str] = {}

    def fit(self, name: str, available: int) -> str:
        available = max(8, available)
        if len(name) <= available:
            return name
        left = max(3, (available - 1) // 2)
        right = max(3, available - left - 1)
        candidate = f"{name[:left]}…{name[-right:]}"
        collision = self.legend.get(candidate)
        if collision is not None and collision != name:
            suffix = 2
            while True:
                marker = f"~{suffix}"
                trimmed = candidate[: max(3, available - len(marker))] + marker
                collision = self.legend.get(trimmed)
                if collision in (None, name):
                    candidate = trimmed
                    break
                suffix += 1
        self.legend[candidate] = name
        return candidate

    def render_legend(self) -> list[str]:
        if not self.legend:
            return []
        lines = ["", "Shortened node names:"]
        for short, full in self.legend.items():
            prefix = f"  {short} = "
            wrapped = textwrap.wrap(
                full,
                width=max(8, self.width - len(prefix)),
                break_long_words=True,
                break_on_hyphens=False,
            ) or [full]
            lines.append(prefix + wrapped[0])
            continuation = " " * len(prefix)
            lines.extend(continuation + part for part in wrapped[1:])
        return lines


SUPPORTED_JOB_PREFIXES = (
    "LHE_",
    "PLAN_",
    "COORD_",
    "PROC_",
    "PROCESS_",
    "MIX_",
    "MERGE_",
    "NTUPLE_",
)


def validate_structure_graph(graph: DagGraph) -> None:
    unsupported = []
    for node in graph.nodes.values():
        if node.directive == "FINAL":
            supported = node.name == "SUMMARY" or node.name.startswith("FINAL_")
        elif node.directive == "SUBDAG":
            supported = node.name.startswith("MIX_")
        else:
            supported = node.name.startswith(SUPPORTED_JOB_PREFIXES)
        if not supported:
            unsupported.append(node.name)
    if unsupported:
        names = ", ".join(sorted(unsupported)[:5])
        if len(unsupported) > 5:
            names += f", and {len(unsupported) - 5} more"
        raise RuntimeError(
            "structure view only supports repository DAG node names; "
            f"unsupported: {names}"
        )
    for child in graph.children.values():
        if child is not None:
            validate_structure_graph(child)


def graph_layers(graph: DagGraph) -> list[list[str]]:
    ordinary = {
        name
        for name, node in graph.nodes.items()
        if node.directive != "FINAL"
    }
    parents: dict[str, set[str]] = {name: set() for name in ordinary}
    children: dict[str, set[str]] = {name: set() for name in ordinary}
    for parent, child in graph.edges:
        if parent not in ordinary or child not in ordinary:
            raise RuntimeError(
                f"{graph.dag_file}: dependency references unknown node "
                f"{parent} -> {child}"
            )
        parents[child].add(parent)
        children[parent].add(child)

    remaining = set(ordinary)
    layers = []
    while remaining:
        layer = sorted(name for name in remaining if not (parents[name] & remaining))
        if not layer:
            cycle_nodes = ", ".join(sorted(remaining)[:5])
            raise RuntimeError(
                f"{graph.dag_file}: dependency cycle involving {cycle_nodes}"
            )
        layers.append(layer)
        remaining.difference_update(layer)
    return layers


def controls_text(graph: DagGraph) -> str:
    if not graph.maxjobs:
        return "none"
    return ", ".join(
        f"{category}={limit}"
        for category, limit in sorted(graph.maxjobs.items())
    )


def append_wrapped(
    lines: list[str],
    prefix: str,
    text: str,
    width: int,
) -> None:
    wrapped = textwrap.wrap(
        text,
        width=max(8, width - len(prefix)),
        break_long_words=False,
        break_on_hyphens=False,
    ) or [""]
    lines.append(prefix + wrapped[0])
    continuation = " " * len(prefix)
    lines.extend(continuation + part for part in wrapped[1:])


def render_full_structure(
    graph: DagGraph,
    records: dict[str, dict],
    width: int,
    color_enabled: bool,
) -> list[str]:
    labels = LabelFormatter(width)
    lines = ["", "DAG structure (full):"]

    def visit(current: DagGraph, indent: str, title: str) -> None:
        title_budget = width - len(indent) - 2
        lines.append(
            f"{indent}{title}: "
            f"{labels.fit(current.dag_file.name, title_budget - len(title))}"
        )
        append_wrapped(
            lines,
            f"{indent}  MAXJOBS: ",
            controls_text(current),
            width,
        )
        parent_map: dict[str, list[str]] = defaultdict(list)
        for parent, child in current.edges:
            parent_map[child].append(parent)

        for layer_index, layer in enumerate(graph_layers(current), start=1):
            lines.append(f"{indent}  Layer {layer_index}:")
            for name in layer:
                node = current.nodes[name]
                record = records.get(name)
                status = record_status(record) if record else "pending"
                category = (
                    f" [{node.category}]"
                    if node.category is not None
                    else ""
                )
                available = width - len(indent) - len(category) - 7
                shown = labels.fit(name, available)
                marker = status_marker(status, color_enabled)
                lines.append(f"{indent}    {marker} {shown}{category}")
                for parent in sorted(parent_map[name]):
                    parent_label = labels.fit(
                        parent,
                        width - len(indent) - 8,
                    )
                    lines.append(f"{indent}      ← {parent_label}")
                if node.directive == "SUBDAG" and current.children.get(name) is None:
                    child_path = node.child_dag or Path("?")
                    shown_path = labels.fit(
                        str(child_path),
                        width - len(indent) - 16,
                    )
                    lines.append(
                        f"{indent}      ↳ child DAG pending: {shown_path}"
                    )

        finals = [
            node
            for node in current.nodes.values()
            if node.directive == "FINAL"
        ]
        for node in sorted(finals, key=lambda item: item.name):
            record = records.get(node.name)
            status = record_status(record) if record else "pending"
            shown = labels.fit(node.name, width - len(indent) - 34)
            marker = status_marker(status, color_enabled)
            lines.append(
                f"{indent}  ⇢ {marker} {shown} "
                "[FINAL: after DAG termination]"
            )

        for controller, child in sorted(current.children.items()):
            if child is None:
                continue
            shown_controller = labels.fit(
                controller,
                width - len(indent) - 13,
            )
            lines.append("")
            visit(child, indent + "  ", f"SubDAG {shown_controller}")

    visit(graph, "  ", "Root")
    lines.extend(labels.render_legend())
    return lines


INDEXED_FAMILY_PATTERN = re.compile(r"^(.+)_([0-9]+)$")


def indexed_family(name: str) -> str:
    match = INDEXED_FAMILY_PATTERN.match(name)
    return match.group(1) if match else name


def campaign_for_root_node(node: PlannedNode) -> str | None:
    prefixes = ("COORD_", "PROC_", "PROCESS_", "NTUPLE_")
    if node.directive == "SUBDAG":
        prefixes = ("MIX_",)
    for prefix in prefixes:
        if not node.name.startswith(prefix):
            continue
        remainder = node.name[len(prefix) :]
        campaign, separator, index = remainder.rpartition("_")
        if separator and campaign and index.isdigit():
            return campaign
    return None


def collect_graph_nodes(graph: DagGraph) -> Iterable[PlannedNode]:
    yield from graph.nodes.values()
    for child in graph.children.values():
        if child is not None:
            yield from collect_graph_nodes(child)


def render_collapsed_structure(
    graph: DagGraph,
    records: dict[str, dict],
    width: int,
    color_enabled: bool,
) -> list[str]:
    labels = LabelFormatter(width)
    lines = [
        "",
        "DAG structure (campaign-collapsed):",
    ]
    append_wrapped(lines, "  Root MAXJOBS: ", controls_text(graph), width)
    root_campaigns = {
        name: campaign_for_root_node(node)
        for name, node in graph.nodes.items()
    }
    campaigns = sorted(
        {campaign for campaign in root_campaigns.values() if campaign}
    )

    outgoing_targets: dict[str, set[str]] = defaultdict(set)
    for parent, child in graph.edges:
        campaign = root_campaigns.get(child)
        if campaign:
            outgoing_targets[parent].add(campaign)
        elif (
            child in graph.nodes
            and graph.nodes[child].stage in {"lhe", "plan"}
        ):
            outgoing_targets[parent].add(indexed_family(child))

    input_families: dict[str, list[str]] = defaultdict(list)
    for name, node in graph.nodes.items():
        if node.stage in {"lhe", "plan"}:
            input_families[indexed_family(name)].append(name)
    if input_families:
        lines.append("  Shared input families:")
        for family, names in sorted(input_families.items()):
            counts = Counter(
                record_status(records[name]) if name in records else "pending"
                for name in names
            )
            destinations = sorted(
                {
                    campaign
                    for name in names
                    for campaign in outgoing_targets.get(name, set())
                }
            )
            shown = labels.fit(family, max(12, width - 34))
            destination_text = ", ".join(destinations) or "unconnected"
            lines.append(
                f"    {shown} ×{len(names)} "
                f"[{status_counts_text(counts, color_enabled)}]"
            )
            wrapped = textwrap.wrap(
                f"→ {destination_text}",
                width=max(20, width - 6),
                subsequent_indent="      ",
            )
            lines.extend(f"      {part}" if index == 0 else part
                         for index, part in enumerate(wrapped))

    for campaign in campaigns:
        root_names = [
            name
            for name, candidate in root_campaigns.items()
            if candidate == campaign
        ]
        child_graphs = [
            graph.children[name]
            for name in root_names
            if name in graph.children and graph.children[name] is not None
        ]
        nodes = [graph.nodes[name] for name in root_names]
        for child in child_graphs:
            if child is not None:
                nodes.extend(collect_graph_nodes(child))

        stage_counts: dict[str, Counter] = defaultdict(Counter)
        stage_totals = Counter()
        for node in nodes:
            record = records.get(node.name)
            status = record_status(record) if record else "pending"
            stage_counts[node.stage][status] += 1
            stage_totals[node.stage] += 1

        shown_campaign = labels.fit(campaign, max(12, width - 16))
        source_count = sum(
            1 for name in root_names if graph.nodes[name].stage == "subdag"
        )
        if not source_count:
            source_count = sum(
                1
                for name in root_names
                if graph.nodes[name].stage == "processing"
            )
        if not source_count:
            source_count = sum(
                1
                for name in root_names
                if graph.nodes[name].stage in {"coordinate", "ntuple"}
            )
        lines.extend(
            ("", f"  Campaign {shown_campaign} ({source_count} source jobs):")
        )
        for stage, label in STAGES:
            total = stage_totals[stage]
            if not total:
                continue
            lines.append(
                f"    {label:<10} ×{total:<4} "
                f"[{status_counts_text(stage_counts[stage], color_enabled)}]"
            )

        edge_types = Counter()
        for parent, child in graph.edges:
            if parent in root_names and child in root_names:
                edge_types[
                    (
                        graph.nodes[parent].stage,
                        graph.nodes[child].stage,
                    )
                ] += 1
        child_controls = Counter()
        missing_children = 0
        for name in root_names:
            if name not in graph.children:
                continue
            child = graph.children[name]
            if child is None:
                missing_children += 1
                continue
            for parent, child_name in child.edges:
                edge_types[
                    (
                        child.nodes[parent].stage,
                        child.nodes[child_name].stage,
                    )
                ] += 1
            child_controls.update(
                {
                    f"{category}={limit}": 1
                    for category, limit in child.maxjobs.items()
                }
            )

        if edge_types:
            lines.append("    Dependencies:")
            for (parent_stage, child_stage), count in sorted(
                edge_types.items(),
                key=lambda item: (
                    STAGE_INDEX.get(item[0][0], len(STAGES)),
                    STAGE_INDEX.get(item[0][1], len(STAGES)),
                ),
            ):
                parent_label = STAGE_LABELS.get(parent_stage, parent_stage)
                child_label = STAGE_LABELS.get(child_stage, child_stage)
                lines.append(
                    f"      {parent_label} ─{count}→ {child_label}"
                )
        if child_controls:
            controls = ", ".join(sorted(child_controls))
            append_wrapped(
                lines,
                "    Per-SubDAG MAXJOBS: ",
                controls,
                width,
            )
        if missing_children:
            lines.append(
                f"    Child DAG pending: {missing_children} controller(s)"
            )

    finals = [
        node
        for node in graph.nodes.values()
        if node.directive == "FINAL"
    ]
    for node in finals:
        record = records.get(node.name)
        status = record_status(record) if record else "pending"
        marker = status_marker(status, color_enabled)
        shown = labels.fit(node.name, width - 42)
        lines.append(
            f"  ⇢ {marker} {shown} [FINAL: after root DAG termination]"
        )
    lines.extend(labels.render_legend())
    return lines


def render_structure(
    graph: DagGraph,
    records: dict[str, dict],
    mode: str,
    width: int,
    color_enabled: bool,
) -> list[str]:
    validate_structure_graph(graph)
    selected_mode = mode
    if mode == "auto":
        selected_mode = "full" if len(flatten_graph(graph)) <= 150 else "collapsed"
    if selected_mode == "full":
        return render_full_structure(graph, records, width, color_enabled)
    return render_collapsed_structure(graph, records, width, color_enabled)


def render(
    schedd: str,
    cluster: int,
    root_dag: Path,
    live: list[dict],
    show_details: bool,
    structure_mode: str | None = None,
    width: int = 120,
    color_enabled: bool = False,
) -> str:
    graph = parse_dag_graph(root_dag)
    planned = flatten_graph(graph)
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
        "LHE → Plan → Coordinate → SubDAG → Process → Merge → Ntuple → Final",
        "",
        "Stage          Progress                    Done  Run Idle Held Fail Wait",
        "-------------  --------------------------  ----  --- ---- ---- ---- ----",
    ]
    for stage, label in STAGES:
        stage_counts = counts[stage]
        total = sum(stage_counts.values())
        bar = progress_bar(stage_counts["done"], total)
        if color_enabled:
            done_length = len(bar) - len(bar.lstrip("█"))
            bar = (
                colorize(bar[:done_length], "done", True)
                + colorize(bar[done_length:], "pending", True)
            )

        def count_cell(status: str, cell_width: int = 4) -> str:
            value = f"{stage_counts[status]:>{cell_width}}"
            if stage_counts[status]:
                return colorize(value, status, color_enabled)
            return value

        lines.append(
            f"{label:<13}  [{bar}]"
            f" {count_cell('done')} {count_cell('running')}"
            f" {count_cell('idle')} {count_cell('held')}"
            f" {count_cell('failed')} {count_cell('pending')}"
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
            status_label = colorize(
                f"{STATUS_LABELS[status]:<4}",
                status,
                color_enabled,
            )
            lines.append(
                f"  {status_label} {name} "
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
                status_label = colorize(
                    f"{STATUS_LABELS[status]:<4}",
                    status,
                    color_enabled,
                )
                lines.append(
                    f"  {stage:<10} {status_label} "
                    f"{record.get('ClusterId', '?')}."
                    f"{record.get('ProcId', '?')} {age:>7}  {name}"
                )
    if structure_mode is not None:
        lines.extend(
            render_structure(
                graph,
                records,
                structure_mode,
                width,
                color_enabled,
            )
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
        "--structure",
        nargs="?",
        const="auto",
        choices=("auto", "collapsed", "full"),
        help=(
            "append DAG topology; optionally choose auto, collapsed, or full "
            "(default when flag is bare: auto)"
        ),
    )
    parser.add_argument(
        "--color",
        choices=("always", "auto", "never"),
        default="auto",
        help="ANSI color mode for the full dashboard (default: auto)",
    )
    parser.add_argument(
        "--width",
        type=int,
        help="structure display width (default: terminal width, or 120)",
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
    if args.width is not None and args.width < 60:
        parser.error("--width must be at least 60")
    return args


def resolve_color_mode(
    mode: str,
    is_tty: bool,
    environment: dict[str, str] | None = None,
) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    environment = os.environ if environment is None else environment
    return is_tty and "NO_COLOR" not in environment


def main() -> int:
    args = parse_args()
    color_enabled = resolve_color_mode(args.color, sys.stdout.isatty())
    width = max(
        60,
        args.width or shutil.get_terminal_size(fallback=(120, 24)).columns,
    )
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
                args.structure,
                width,
                color_enabled,
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
