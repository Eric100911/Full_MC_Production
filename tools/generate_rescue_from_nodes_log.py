#!/usr/bin/env python3
"""Generate DAGMan rescue files from existing nodes.log files.

Each SubDAG's blocks_processing.dag.nodes.log records every node lifecycle
event (submitted, executing, terminated, held, aborted).  When DAGMan is
killed without a proper shutdown no rescue file is written, but the
nodes.log preserves enough information to reconstruct one.

Rescue files allow ``condor_submit_dag`` to skip already-completed nodes,
avoiding redundant re-processing of blocks whose MiniAOD and manifests are
already staged on EOS.

Usage::

    python3 tools/generate_rescue_from_nodes_log.py \
        --plan-subdags generated/.../plan_subdags \
        --eos-base root://cceos.ihep.ac.cn:1094///store/user/chiw/.../output \
        --dry-run

Add ``--apply`` to write the rescue files.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

EVENT_PATTERN = re.compile(r"^(\d{3}) \(([\d.]+)\)")
NODE_PATTERN = re.compile(r"^\s+DAG Node:\s+(\S+)")


def parse_nodes_log(path: str) -> Dict[str, List[Tuple[str, str, str]]]:
    """Return {node_name: [(event_code, cluster_id, raw_rest), ...]}.

    The Condor nodes.log associates events with DAG nodes through the
    ``DAG Node:`` annotation, which only appears for the *first* event
    of each cluster (normally 000, the submission).  Subsequent events
    for the same cluster (001 executing, 005 terminated, 009 aborted,
    012 held) relate to the same node but are NOT re-annotated.
    """
    nodes: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    cluster_to_node: Dict[str, str] = {}
    current_event: Optional[str] = None
    current_cluster: Optional[str] = None
    current_rest: Optional[str] = None

    with open(path, "r", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")
            m = EVENT_PATTERN.match(line)
            if m:
                # Flush any pending event that was waiting for a DAG Node line
                if current_cluster and current_cluster in cluster_to_node:
                    node_name = cluster_to_node[current_cluster]
                    nodes[node_name].append(
                        (current_event, current_cluster, current_rest)
                    )
                current_event = m.group(1)
                current_cluster = m.group(2)
                current_rest = line[m.end():].strip()
                continue
            m = NODE_PATTERN.match(line)
            if m and current_cluster:
                node_name = m.group(1)
                cluster_to_node[current_cluster] = node_name
                nodes[node_name].append(
                    (current_event, current_cluster, current_rest)
                )
                current_cluster = None
                current_event = None
                current_rest = None
                continue

    # Flush final pending event
    if current_cluster and current_cluster in cluster_to_node:
        node_name = cluster_to_node[current_cluster]
        nodes[node_name].append(
            (current_event, current_cluster, current_rest)
        )

    return dict(nodes)


def node_last_status(events: List[Tuple[str, str, str]]) -> Tuple[str, str]:
    """Return (last_event_code, last_rest) for a node."""
    last_code, _, last_rest = events[-1]
    return last_code, last_rest


def exit_code_from_termination(rest: str) -> Optional[int]:
    """Extract return value from termination detail like '(1) Normal termination (return value 0)'."""
    m = re.search(r"return value (-?\d+)", rest)
    if m:
        return int(m.group(1))
    return None


def node_is_done(events: List[Tuple[str, str, str]]) -> bool:
    """A node is DONE if its last event is termination (005).

    The exit code from termination details (the line after 005) is harder to
    extract reliably because nodes.log formats multi-line detail blocks with
    tabs.  In practice DAGMan marks a node done after any 005 event; non-zero
    exits trigger RETRY, which produces a subsequent 000/001/005 cycle.
    """
    last_code, _last_rest = node_last_status(events)
    return last_code == "005"


def extract_job_id(node_name: str) -> str:
    """Extract JOB000000_BLOCK000042 or JOB000000_MERGE000042 from node name."""
    parts = node_name.rsplit("_", 1)
    if len(parts) >= 2:
        return parts[1]
    # multi-underscore: MIX_JJP_TPS_..._BLOCKxxxxxx → last segment
    for seg in ["_BLOCK", "_MERGE", "_GROUP", "_FINAL"]:
        idx = node_name.rfind(seg)
        if idx >= 0:
            return node_name[idx + 1:]
    return node_name


def eos_list_dir(eos_base: str, campaign: str) -> List[str]:
    """List all entries under output/<campaign>/ on EOS. xrdfs ls."""
    eos_url = eos_base.rstrip("/")
    # eos_base: root://cceos.ihep.ac.cn:1094///store/user/chiw/.../output
    # Need to extract host and path
    host_path = eos_url.replace("root://", "", 1)
    host, sep, path = host_path.partition("/")
    full_path = f"/{path}/{campaign}/"
    cmd = ["xrdfs", f"root://{host}", "ls", full_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"xrdfs ls failed for {full_path}: {result.stderr}", file=sys.stderr)
            return []
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]
    except Exception as exc:
        print(f"xrdfs error for {full_path}: {exc}", file=sys.stderr)
        return []


def eos_file_exists(eos_base: str, campaign: str, job_id: str, filename: str) -> bool:
    """Check if a specific file exists on EOS. Uses xrdfs stat."""
    eos_url = eos_base.rstrip("/")
    host_path = eos_url.replace("root://", "", 1)
    host, sep, path = host_path.partition("/")
    full_path = f"/{path}/{campaign}/{job_id}/{filename}"
    cmd = ["xrdfs", f"root://{host}", "stat", full_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            return False
        # Check for Size > 0
        for line in result.stdout.splitlines():
            if line.startswith("Size:") and int(line.split(":")[1].strip()) > 0:
                return True
        return False
    except Exception:
        return False


def eos_done_set(eos_base: str, campaign: str, file_filter: str = "output_MINIAOD.root") -> Set[str]:
    """Scan EOS and return set of JOB* IDs that have the given file.

    This is an optimization over one-by-one checks: list all entries,
    then for each job dir, check for the specific file.
    """
    entries = eos_list_dir(eos_base, campaign)
    done: Set[str] = set()
    # Filter to MERGE or BLOCK dirs only
    for entry in entries:
        basename = entry.rstrip("/").split("/")[-1]
        if "BLOCK" not in basename and "MERGE" not in basename:
            continue
        job_id = basename
        file_path = f"{entry}/{file_filter}"
        # We already have the path, just need to stat
        eos_url = eos_base.rstrip("/")
        host_path = eos_url.replace("root://", "", 1)
        host, sep, path = host_path.partition("/")
        full_path = file_path if file_path.startswith("/") else f"/{path}/{campaign}/{job_id}/{file_filter}"
        cmd = ["xrdfs", f"root://{host}", "stat", full_path]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if line.startswith("Size:") and int(line.split(":")[1].strip()) > 0:
                        done.add(job_id)
                        break
        except Exception:
            pass
    return done


def generate_rescue(
    nodes_log_path: str,
    campaign: str,
    eos_base: Optional[str] = None,
    cross_check_eos: bool = False,
) -> Tuple[List[str], List[str], Dict[str, List[str]]]:
    """Generate DONE node list for a rescue file.

    Returns (done_nodes, skipped_nodes, overrides).
    overrides maps node_name → [reason].
    """
    nodes = parse_nodes_log(nodes_log_path)
    done_candidates = []
    abort_or_held = []
    overrides: Dict[str, List[str]] = defaultdict(list)

    for node_name, events in sorted(nodes.items()):
        if node_is_done(events):
            done_candidates.append(node_name)
        else:
            abort_or_held.append(node_name)

    if not cross_check_eos or not eos_base:
        return done_candidates, abort_or_held, overrides

    # EOS cross-check: for MERGE → check output_MINIAOD.root
    # for NTUPLE → check output_ntuple.root
    print(f"  EOS cross-check for {campaign}...", file=sys.stderr)
    merge_done = eos_done_set(eos_base, campaign, "output_MINIAOD.root")
    ntuple_done = eos_done_set(eos_base, campaign, "output_ntuple.root")

    verified_done = []
    for node_name in done_candidates:
        if "NTUPLE" in node_name:
            job_id = extract_job_id(node_name)
            if job_id not in ntuple_done:
                overrides[node_name].append("NTUPLE: ntuple not found on EOS")
                abort_or_held.append(node_name)
                continue
        elif "MERGE" in node_name:
            job_id = extract_job_id(node_name)
            if job_id not in merge_done:
                overrides[node_name].append("MERGE: MiniAOD not found on EOS")
                abort_or_held.append(node_name)
                continue
        verified_done.append(node_name)

    return verified_done, abort_or_held, overrides


def write_rescue_file(
    dag_path: str,
    done_nodes: List[str],
    dry_run: bool = False,
) -> Optional[str]:
    """Write a .rescue001 file for the given DAG."""
    rescue_path = dag_path + ".rescue001"
    header = (
        "# Rescue DAG file, created by generate_rescue_from_nodes_log.py\n"
        "# Generated from nodes.log → DONE nodes replayed.\n"
        "# Submit with:  condor_submit_dag <dag>\n"
        "# DAGMan will auto-detect this rescue and skip DONE nodes.\n"
    )
    lines = [header]
    lines.append(f"DOT {dag_path}.dagman.rescue001.dot\n")
    for node in sorted(done_nodes):
        lines.append(f"DONE {node}")
    lines.append("")

    content = "\n".join(lines) + "\n"

    if dry_run:
        print(f"Would write {rescue_path}: {len(done_nodes)} DONE nodes")
        for line in lines[-10:]:
            print(f"  {line.rstrip()}")
        return None

    with open(rescue_path, "w", encoding="utf-8") as f:
        f.write(content)
    return rescue_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan-subdags", required=True,
                        help="Path to plan_subdags/ directory")
    parser.add_argument("--eos-base",
                        help="EOS output base URL for cross-check")
    parser.add_argument("--cross-check-eos", action="store_true",
                        help="Verify MERGE/NTUPLE node output against EOS (slow)")
    parser.add_argument("--exclude-nodes", nargs="*", default=[],
                        help="Node names to exclude from DONE even if terminated "
                             "(for known silent stageout failures)")
    parser.add_argument("--exclude-file",
                        help="JSON file mapping campaign/job → list of node names to exclude")
    parser.add_argument("--campaigns", nargs="*",
                        help="Limit to specific campaigns (default: all)")
    parser.add_argument("--jobs", nargs="*",
                        help="Limit to specific job_N dirs (e.g. job_6)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true",
                        help="Actually write rescue files")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Error: use --dry-run or --apply", file=sys.stderr)
        return 1

    plan_dir = Path(args.plan_subdags)
    if not plan_dir.is_dir():
        print(f"Error: --plan-subdags is not a directory: {plan_dir}", file=sys.stderr)
        return 1

    eos_base = args.eos_base
    if args.cross_check_eos and not eos_base:
        print("Error: --cross-check-eos requires --eos-base", file=sys.stderr)
        return 1

    # Load explicit exclusions
    exclude_map: Dict[str, set] = defaultdict(set)  # "campaign/job_N" → {node_names}
    for node_name in args.exclude_nodes:
        # node_name format: campaign/job_N:node_name or just node_name
        if ":" in node_name:
            key, name = node_name.split(":", 1)
            exclude_map[key].add(name)
        else:
            exclude_map["*"].add(node_name)
    if args.exclude_file:
        with open(args.exclude_file, "r") as f:
            data = json.load(f)
        for campaign, jobs in data.items():
            for job_n, nodes in jobs.items():
                key = f"{campaign}/{job_n}"
                exclude_map[key].update(nodes)

    processed = 0
    written = 0

    for campaign_dir in sorted(plan_dir.iterdir()):
        if not campaign_dir.is_dir():
            continue
        campaign = campaign_dir.name
        if campaign in ("campaign_shard_allocation.json", "_shared", "GG", "JpsiG_CSCO"):
            continue

        if args.campaigns and campaign not in args.campaigns:
            continue

        for job_dir in sorted(campaign_dir.iterdir()):
            if not job_dir.is_dir() or not job_dir.name.startswith("job_"):
                continue
            if args.jobs and job_dir.name not in args.jobs:
                continue

            dag_path = job_dir / "blocks_processing.dag"
            nl_path = job_dir / "blocks_processing.dag.nodes.log"

            if not dag_path.exists():
                print(f"  SKIP {campaign}/{job_dir.name}: no blocks_processing.dag")
                continue

            if not nl_path.exists():
                print(f"  SKIP {campaign}/{job_dir.name}: no nodes.log (never ran)")
                continue

            print(f"Processing {campaign}/{job_dir.name}...", file=sys.stderr)
            done, skipped, overrides = generate_rescue(
                str(nl_path), campaign,
                eos_base=eos_base if args.cross_check_eos else None,
                cross_check_eos=args.cross_check_eos,
            )

            # Apply explicit exclusions
            key = f"{campaign}/{job_dir.name}"
            for node_name in list(done):
                short = node_name.rsplit("_", 1)[-1] if "_" in node_name else node_name
                excluded = (
                    node_name in exclude_map.get(key, set())
                    or short in exclude_map.get(key, set())
                    or node_name in exclude_map.get("*", set())
                    or short in exclude_map.get("*", set())
                )
                if excluded:
                    done.remove(node_name)
                    skipped.append(node_name)
                    overrides[node_name].append("manual exclusion (silent failure / EOS gap)")

            processing_total = len(done) + len(skipped)
            pct = len(done) * 100 // max(processing_total, 1)
            print(f"  DONE: {len(done)}/{processing_total} ({pct}%)  "
                  f"NEED RERUN: {len(skipped)}", file=sys.stderr)
            if overrides:
                for node, reasons in sorted(overrides.items()):
                    short_name = node.rsplit("_", 1)[-1]
                    print(f"    overridden: {short_name}: {'; '.join(reasons)}",
                          file=sys.stderr)

            processed += 1

            if args.apply and done:
                rp = write_rescue_file(str(dag_path), done, dry_run=False)
                if rp:
                    print(f"  → wrote {rp}", file=sys.stderr)
                    written += 1
            elif args.dry_run:
                write_rescue_file(str(dag_path), done, dry_run=True)

    print(f"\nProcessed {processed} subdags, wrote {written} rescue files.")
    if args.dry_run:
        print("DRY RUN — no files written. Use --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
