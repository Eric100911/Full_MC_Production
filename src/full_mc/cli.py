"""Public CLI. Import execution backends only for the selected command."""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from .settings import CORE_ROOT, LOG_LEVELS, catalog, load_run_spec, read_json
from .io import atomic_json


def _leaf(commands, name, help_text):
    parser = commands.add_parser(name, help=help_text)
    parser.add_argument("--log-level", choices=LOG_LEVELS, default=argparse.SUPPRESS)
    return parser


def build_parser():
    parser = argparse.ArgumentParser(prog="mcprod", description="CMS CERN/IHEP production")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--log-level", choices=LOG_LEVELS, default=None)
    commands = parser.add_subparsers(dest="command", required=True)
    _leaf(commands, "catalog", "List supported campaigns and profile catalogues")
    runtime = commands.add_parser("runtime").add_subparsers(dest="action", required=True)
    p = _leaf(runtime, "prepare", "Prepare one reusable runtime release")
    p.add_argument("--site", choices=("cern", "ihep"), required=True)
    p.add_argument("--release", required=True)
    p.add_argument("--output-dir", type=Path)
    p.add_argument("--profile", action="append")
    p.add_argument("--core", type=Path, default=CORE_ROOT)
    runs = commands.add_parser("run").add_subparsers(dest="action", required=True)
    p = _leaf(runs, "plan", "Freeze a run and prepare its DAG without submitting")
    p.add_argument("--spec", type=Path, required=True)
    p.add_argument("--output-dir", type=Path)
    for name in ("submit", "status", "resume"):
        p = _leaf(runs, name, name.capitalize() + " a CERN run")
        p.add_argument("--run", required=True)
        p.add_argument("--site", choices=("cern",), default="cern")
    handoff = commands.add_parser("handoff").add_subparsers(dest="action", required=True)
    p = _leaf(handoff, "export", "Export ready component groups")
    p.add_argument("--run", required=True)
    p.add_argument("--ready-only", action="store_true", default=True)
    p = _leaf(handoff, "transfer", "Transfer an explicit handoff")
    p.add_argument("--run", required=True)
    p.add_argument("--site", choices=("ihep",), default="ihep")
    p.add_argument("--site-config", type=Path)
    batches = commands.add_parser("batch").add_subparsers(dest="action", required=True)
    p = _leaf(batches, "prepare", "Build a resumable IHEP batch without submitting")
    p.add_argument("--handoff", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--proxy", default=None)
    p.add_argument("--runtime-dir", type=Path,
                   help="Prepared IHEP runtime required by a pinned handoff")
    for name in ("submit", "resume", "status"):
        p = _leaf(batches, name, name.capitalize() + " an exact IHEP batch")
        p.add_argument("--manifest", type=Path)
        p.add_argument("--run")
        p.add_argument("--batch")
    p = _leaf(batches, "worker", "Execute one combined IHEP task")
    p.add_argument("--task", type=Path, required=True)
    p.add_argument("--work-dir", type=Path, required=True)
    analysis = commands.add_parser("analysis").add_subparsers(dest="action", required=True)
    p = _leaf(analysis, "aggregate", "Prepare the aggregation task")
    p.add_argument("--run", required=True)
    p.add_argument("--prepare", action="store_true", required=True)
    p.add_argument("--handoff", type=Path)
    p.add_argument("--output", type=Path)
    p.add_argument("--campaign", default="JJP")
    p = _leaf(analysis, "worker", "Execute a prepared aggregation")
    p.add_argument("--task", type=Path, required=True)
    p.add_argument("--work-dir", type=Path, required=True)
    internal = commands.add_parser("internal").add_subparsers(dest="action", required=True)
    p = _leaf(internal, "advance-wave", "Advance one serialized production wave")
    p.add_argument("--run-dir", type=Path, required=True)
    p.add_argument("--step", type=int)
    for action in ("prepare-source", "prepare-shard", "admit-producer",
                   "accept-result", "complete-source", "complete-shard",
                   "prepare-cleanup", "finalize-shards"):
        p = _leaf(internal, action, "Run one local campaign-shard control operation")
        p.add_argument("--run-dir", type=Path, required=True)
        if action in ("prepare-source", "complete-source"):
            p.add_argument("--source-index", type=int, required=True)
        if action in ("prepare-shard", "complete-shard"):
            p.add_argument("--campaign", required=True)
            p.add_argument("--shard-index", type=int, required=True)
        if action in ("admit-producer", "accept-result"):
            p.add_argument("--task-id", required=True)
        if action in ("accept-result", "complete-source", "complete-shard"):
            p.add_argument("--return-code", type=int, required=True)
        if action == "prepare-cleanup":
            p.add_argument("--pool", required=True)
            p.add_argument("--slot", required=True)
    p = _leaf(commands, "worker", "Execute a single staged worker task")
    p.add_argument("--task", type=Path, required=True)
    p.add_argument("--work-dir", type=Path)
    p.add_argument("--result", type=Path)
    workspace = commands.add_parser("workspace").add_subparsers(dest="action", required=True)
    p = _leaf(workspace, "init", "Create a production wrapper with a pinned core")
    p.add_argument("--directory", type=Path, required=True)
    p.add_argument("--core", default=str(CORE_ROOT))
    p.add_argument("--revision")
    return parser


def _run_dir(args):
    supplied = Path(args.run)
    if supplied.is_dir():
        return supplied.resolve()
    return (args.workspace / "work" / args.run / "cern").resolve()


def _batch_manifest(args):
    if args.manifest:
        return args.manifest.resolve()
    if not args.run or not args.batch:
        raise ValueError("supply --manifest or both --run and --batch")
    return args.workspace / "work" / args.run / "ihep" / args.batch / "batch_manifest.json"


def dispatch(args):
    if args.command == "catalog":
        return catalog()
    if args.command == "workspace":
        from .application.workspace import initialize
        return initialize(args.directory, args.core, args.revision)
    if args.command == "runtime":
        from .runtime.packages import prepare_runtime
        destination = args.output_dir or args.workspace / "runtimes" / args.release / args.site
        return prepare_runtime(args.core, destination, args.release, args.site, profiles=args.profile)
    if args.command == "run":
        from .application import production
        if args.action == "plan":
            spec = load_run_spec(args.spec)
            output = args.output_dir or args.workspace / "work" / spec["run_id"] / "cern"
            return production.plan_run(args.spec, output, log_level=args.log_level)
        from .backends import condor
        return {"submit": condor.submit_run, "status": condor.status_run,
                "resume": condor.resume_run}[args.action](_run_dir(args))
    if args.command == "internal":
        if args.action == "advance-wave":
            from .application.production import advance_wave
            return advance_wave(args.run_dir, args.step)
        from .application import shards
        operation = (shards.finalize_run if args.action == "finalize-shards"
                     else getattr(shards, args.action.replace("-", "_")))
        if args.action in ("prepare-source", "complete-source"):
            arguments = [args.run_dir, args.source_index]
        elif args.action in ("prepare-shard", "complete-shard"):
            arguments = [args.run_dir, args.campaign, args.shard_index]
        elif args.action in ("admit-producer", "accept-result"):
            arguments = [args.run_dir, args.task_id]
        elif args.action == "prepare-cleanup":
            arguments = [args.run_dir, args.pool, args.slot]
        else:
            arguments = [args.run_dir]
        if hasattr(args, "return_code"):
            arguments.append(args.return_code)
        return operation(*arguments)
    if args.command == "handoff":
        from .application.handoff import export_handoff, transfer_handoff
        root = _run_dir(args)
        if args.action == "export":
            return export_handoff(root, ready_only=True)
        config = args.site_config or args.workspace / "sites" / "ihep.json"
        return transfer_handoff(root / "handoff.json", read_json(config))
    if args.command == "batch":
        if args.action == "worker":
            from .application.resume import run_combined_task
            return run_combined_task(args.task, args.work_dir, log_level=args.log_level)
        from .backends.hepjob import prepare_batch, submit_batch, resume_batch, status_batch
        if args.action == "prepare":
            kwargs = {"log_level": args.log_level}
            if args.proxy:
                kwargs["proxy"] = args.proxy
            if args.runtime_dir:
                kwargs["runtime_dir"] = args.runtime_dir
            return prepare_batch(args.handoff, args.output_dir, **kwargs)
        if args.action == "status":
            return status_batch(_batch_manifest(args))
        return {"submit": submit_batch, "resume": resume_batch}[args.action](_batch_manifest(args))
    if args.command == "analysis":
        from .application.analysis import prepare_aggregate, run_aggregate_task
        if args.action == "worker":
            return run_aggregate_task(args.task, args.work_dir, log_level=args.log_level)
        root = _run_dir(args)
        return prepare_aggregate(args.handoff or root / "handoff.json",
                                 args.output or root / "aggregation.json", args.campaign,
                                 log_level=args.log_level)
    if args.command == "worker":
        from .workers import execute_task
        task = read_json(args.task)
        if args.log_level:
            task["log_level"] = args.log_level
        logging.getLogger().setLevel(task.get("log_level", "info").upper())
        root = args.work_dir
        if root is None:
            scratch = Path("/tmp/chiw")
            scratch.mkdir(parents=True, exist_ok=True)
            root = Path(tempfile.mkdtemp(prefix="mcprod-worker-", dir=scratch))
        root.mkdir(parents=True, exist_ok=True)
        result = execute_task(task, root)
        output = args.result or task.get("parameters", {}).get("result_path") or root / "result.json"
        atomic_json(output, result)
        return {"task_id": result["task_id"], "stage": result["stage"],
                "status": result["status"], "result_path": str(output),
                "output_count": len(result.get("outputs", []))}
    raise ValueError(f"unsupported command: {args.command}")


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, (args.log_level or "info").upper()),
                        format="%(levelname)s: %(message)s")
    try:
        result = dispatch(args)
    except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        logging.error("%s", exc)
        return 1
    print(json.dumps(result, indent=2, default=str))
    if args.command == "internal" and result.get("skip"):
        return 99
    if args.command == "internal" and "exit_code" in result:
        return int(result["exit_code"])
    return 0
