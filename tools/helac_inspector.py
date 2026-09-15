#!/usr/bin/env python3
"""Inspect HELAC-Onia calculation progress and final integration results.

The parser is deliberately dependency-free because the same file is bundled
onto HELAC worker nodes.  ``live`` is a read-only Condor frontend; ``parse``
can inspect a worker directory or an extracted diagnostic archive.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


SCHEMA_VERSION = "helac-inspector-v1"
DEFAULT_SSH_TIMEOUT_SECONDS = 90
NUMBER = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[DEde][+-]?\d+)?"
SIGMA_RE = re.compile(
    rf"^\s*sigma=\s*(?P<sigma>{NUMBER})\s+(?P<rel>{NUMBER})\s+"
    r"(?P<nonzero>\d+)\s+(?P<used>\d+)\s+(?P<target>\d+)"
)
FINAL_RE = re.compile(
    rf"total sigma\s*\(nb\)\s*=\s*(?P<sigma>{NUMBER})\s*\+/-\s*(?P<error>{NUMBER})",
    re.IGNORECASE,
)
PERCENT_RE = re.compile(rf"%\s*error:\s*(?P<percent>{NUMBER})", re.IGNORECASE)
ASSIGNMENT_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+([^#!\s]+)")


def fortran_float(value: str) -> float:
    return float(value.replace("D", "E").replace("d", "e"))


def numeric_run_key(path: Path) -> Tuple[int, str]:
    match = re.search(r"_(\d+)$", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def find_helac_root(path: Path) -> Path:
    path = path.resolve()
    candidates = [path]
    if path.name != "HELAC-Onia-2.7.6":
        candidates.append(path / "HELAC-Onia-2.7.6")
        candidates.append(path / "runtime" / "lhe_generation" / "HELAC-Onia-2.7.6")
    for candidate in candidates:
        if candidate.is_dir() and (
            candidate.name == "HELAC-Onia-2.7.6"
            or any(candidate.glob("PROC_HO_*"))
        ):
            return candidate
    raise FileNotFoundError(f"HELAC-Onia root not found below {path}")


def discover_run_dirs(root: Path) -> List[Path]:
    return sorted(
        (entry for entry in root.glob("PROC_HO_*") if entry.is_dir()),
        key=numeric_run_key,
    )


def discover_calculation_dirs(run_dir: Path) -> List[Path]:
    return sorted(
        (
            entry
            for entry in run_dir.iterdir()
            if entry.is_dir()
            and (entry.name.startswith("P0_calc_") or entry.name.startswith("P0_addon_"))
        ),
        key=lambda path: path.name,
    )


def parse_assignments(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.is_file():
        return values
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return values
    for line in lines:
        match = ASSIGNMENT_RE.match(line)
        if match:
            values[match.group(1)] = match.group(2)
    return values


def count_events(path: Path) -> Optional[int]:
    if not path.is_file():
        return None
    count = 0
    try:
        with path.open("rb") as handle:
            for line in handle:
                if line.strip() == b"<event>":
                    count += 1
    except OSError:
        return None
    return count


def parse_result_file(path: Path, root: Path) -> Dict[str, Any]:
    last_sigma: Optional[Dict[str, Any]] = None
    recent_sigmas: List[Dict[str, Any]] = []
    iteration_count = 0
    final_sigma: Optional[float] = None
    final_error: Optional[float] = None
    final_percent: Optional[float] = None
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError as exc:
        return {
            "path": str(path.relative_to(root)),
            "status": "unreadable",
            "error": str(exc),
        }

    for line in lines:
        match = SIGMA_RE.match(line)
        if match:
            last_sigma = {
                "sigma_nb": fortran_float(match.group("sigma")),
                "relative_error_fraction": fortran_float(match.group("rel")),
                "relative_error_percent": fortran_float(match.group("rel")) * 100.0,
                "nonzero_points": int(match.group("nonzero")),
                "points_used": int(match.group("used")),
                "points_target": int(match.group("target")),
            }
            iteration_count += 1
            recent_sigmas.append(last_sigma)
            if len(recent_sigmas) > 8:
                recent_sigmas.pop(0)
        match = FINAL_RE.search(line)
        if match:
            final_sigma = fortran_float(match.group("sigma"))
            final_error = fortran_float(match.group("error"))
        match = PERCENT_RE.search(line)
        if match:
            final_percent = fortran_float(match.group("percent"))

    record: Dict[str, Any] = {
        "path": str(path.relative_to(root)),
        "status": "complete" if final_sigma is not None else "running",
        "mtime": path.stat().st_mtime,
        "last": last_sigma,
        "iteration_count": iteration_count,
        "recent_iterations": recent_sigmas,
        "final_sigma_nb": final_sigma,
        "final_error_nb": final_error,
        "final_relative_error_percent": final_percent,
    }
    if last_sigma is not None and last_sigma["relative_error_fraction"] > 0:
        current_relative = last_sigma["relative_error_fraction"]
        record["current_relative_error_fraction"] = current_relative
        record["current_poisson_equivalent_events"] = math.ceil(
            1.0 / (current_relative * current_relative)
        )
    if final_sigma is not None and final_error is not None and final_sigma != 0:
        relative = abs(final_error / final_sigma)
        record["final_relative_error_fraction"] = relative
        record["poisson_equivalent_events"] = math.ceil(1.0 / (relative * relative))
    elif "current_poisson_equivalent_events" in record:
        record["poisson_equivalent_events"] = record["current_poisson_equivalent_events"]
    return record


def _lhe_files(run_dirs: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    for run_dir in run_dirs:
        for path in run_dir.rglob("*.lhe"):
            if not path.name.endswith("_py8.lhe"):
                files.append(path)
    return sorted(set(files))


def parse_root(root_path: Path, count_lhe_events: bool = False) -> Dict[str, Any]:
    root = find_helac_root(root_path)
    run_dirs = discover_run_dirs(root)
    calculations: List[Dict[str, Any]] = []
    result_files: List[Dict[str, Any]] = []

    for run_dir in run_dirs:
        run_record: Dict[str, Any] = {
            "name": run_dir.name,
            "path": str(run_dir.relative_to(root)),
            "calculations": [],
        }
        for calc_dir in discover_calculation_dirs(run_dir):
            results = sorted((calc_dir / "output").glob("RESULT_*.out"))
            parsed_results = [parse_result_file(path, root) for path in results]
            calc_record = {
                "name": calc_dir.name,
                "path": str(calc_dir.relative_to(root)),
                "has_input": (calc_dir / "input").is_dir(),
                "has_output": (calc_dir / "output").is_dir(),
                "result_files": parsed_results,
            }
            run_record["calculations"].append(calc_record)
            result_files.extend(parsed_results)
        calculations.append(run_record)

    event_paths: List[Path] = []
    events_by_run: Dict[str, int] = {}
    if count_lhe_events:
        for run_dir in run_dirs:
            run_event_paths = _lhe_files([run_dir])
            event_paths.extend(run_event_paths)
            run_event_counts = [count_events(path) for path in run_event_paths]
            valid_counts = [count for count in run_event_counts if count is not None]
            if valid_counts:
                events_by_run[run_dir.name] = sum(valid_counts)
    latest_result = max(result_files, key=lambda item: item.get("mtime", 0), default=None)
    has_lhe = bool(event_paths)
    if not run_dirs:
        stage = "pre-helac"
    elif not calculations or not any(run["calculations"] for run in calculations):
        stage = "helac-directory-created"
    elif not result_files:
        stage = "calculation-preparing"
    elif any(item["status"] == "running" for item in result_files):
        stage = "calculation-running"
    elif has_lhe:
        stage = "postprocessing"
    else:
        stage = "calculation-complete"

    observed_events = sum(events_by_run.values()) if events_by_run else None
    for result in result_files:
        run_name = Path(result["path"]).parts[0]
        run_events = events_by_run.get(run_name)
        if run_events is not None and result.get("poisson_equivalent_events") is not None:
            result["observed_lhe_events"] = run_events
            result["additional_poisson_equivalent_events"] = max(
                0, result["poisson_equivalent_events"] - run_events
            )

    return {
        "schema_version": SCHEMA_VERSION,
        "root": str(root),
        "stage": stage,
        "latest_result": latest_result.get("path") if latest_result else None,
        "run_count": len(run_dirs),
        "runs": calculations,
        "result_count": len(result_files),
        "lhe_files": [str(path.relative_to(root)) for path in event_paths],
        "observed_lhe_events": observed_events,
        "observed_lhe_events_by_run": events_by_run,
        "lhe_event_scan_performed": count_lhe_events,
        "parameters": parse_assignments(root / "input" / "user.inp"),
    }


def condor_status(schedd: str, job: str) -> Dict[str, Any]:
    command = [
        "condor_q",
        "-name",
        schedd,
        job,
        "-nobatch",
        "-af",
        "ClusterId",
        "ProcId",
        "JobStatus",
        "LastRemoteHost",
        "RemoteWallClockTime",
    ]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0 or not result.stdout.strip():
        return {
            "state": "not-live",
            "schedd": schedd,
            "job": job,
            "queue_error": (result.stderr or result.stdout).strip(),
        }
    fields = result.stdout.splitlines()[0].split()
    status_code = int(fields[2]) if len(fields) > 2 and fields[2].isdigit() else None
    wall_clock: Optional[float] = None
    if len(fields) > 4:
        try:
            wall_clock = float(fields[4])
        except ValueError:
            pass
    status_names = {
        1: "idle",
        2: "running",
        3: "removed",
        4: "completed",
        5: "held",
        6: "transferring-output",
        7: "suspended",
    }
    return {
        "state": status_names.get(status_code, "unknown"),
        "status_code": status_code,
        "schedd": schedd,
        "job": job,
        "last_remote_host": fields[3] if len(fields) > 3 else None,
        "remote_wall_clock_seconds": wall_clock,
    }


def remote_snapshot(
    schedd: str,
    job: str,
    count_lhe_events: bool = False,
    ssh_timeout_seconds: int = DEFAULT_SSH_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    count_events_arg = " --count-lhe-events" if count_lhe_events else ""
    command = (
        "set -eu; "
        "TOOL=''; ROOT=''; "
        "for BASE in \"$PWD\" \"$PWD/..\" \"$PWD/../..\"; do "
        "  [ -n \"$TOOL\" ] || TOOL=$(find \"$BASE\" -path '*/runtime/tools/helac_inspector.py' -type f -print -quit 2>/dev/null || true); "
        "  [ -n \"$ROOT\" ] || ROOT=$(find \"$BASE\" -type d -path '*/runtime/lhe_generation/HELAC-Onia-2.7.6' -print -quit 2>/dev/null || true); "
        "done; "
        "[ -n \"$TOOL\" ] && [ -n \"$ROOT\" ] || { echo 'inspector runtime not found' >&2; exit 66; }; "
        "python3 \"$TOOL\" parse --root \"$ROOT\"" + count_events_arg
    )
    try:
        result = subprocess.run(
            ["condor_ssh_to_job", "-name", schedd, job, "bash", "-lc", command],
            text=True,
            capture_output=True,
            check=False,
            timeout=ssh_timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return {
            "schema_version": SCHEMA_VERSION,
            "stage": "ssh-timeout",
            "schedd": schedd,
            "job": job,
            "error": f"condor_ssh_to_job exceeded {ssh_timeout_seconds} s",
        }
    if result.returncode != 0:
        return {
            "schema_version": SCHEMA_VERSION,
            "stage": "ssh-error",
            "schedd": schedd,
            "job": job,
            "ssh_returncode": result.returncode,
            "error": (result.stderr or result.stdout).strip(),
        }
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    try:
        payload = json.loads(lines[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        return {
            "schema_version": SCHEMA_VERSION,
            "stage": "invalid-ssh-response",
            "schedd": schedd,
            "job": job,
            "error": str(exc),
            "stdout_tail": lines[-20:],
        }
    payload["schedd"] = schedd
    payload["job"] = job
    return payload


def live_snapshot(
    schedd: str,
    job: str,
    count_lhe_events: bool = False,
    ssh_timeout_seconds: int = DEFAULT_SSH_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    queue = condor_status(schedd, job)
    if queue.get("state") != "running":
        return {"schema_version": SCHEMA_VERSION, "condor": queue}
    payload = remote_snapshot(
        schedd,
        job,
        count_lhe_events=count_lhe_events,
        ssh_timeout_seconds=ssh_timeout_seconds,
    )
    payload["condor"] = queue
    return payload


def emit(payload: Dict[str, Any], pretty: bool = False) -> None:
    print(json.dumps(payload, indent=2 if pretty else None, sort_keys=True), flush=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_parser = subparsers.add_parser("parse", help="parse a HELAC worker directory")
    parse_parser.add_argument("--root", required=True, type=Path)
    parse_parser.add_argument("--pretty", action="store_true")
    parse_parser.add_argument(
        "--count-lhe-events",
        action="store_true",
        help="recursively count <event> records; disabled by default because it can be expensive",
    )

    live_parser = subparsers.add_parser("live", help="inspect a running Condor HELAC job")
    live_parser.add_argument("--schedd", required=True)
    live_parser.add_argument("--job", required=True, help="cluster.proc")
    live_parser.add_argument("--watch", type=int, default=0, help="repeat interval in seconds")
    live_parser.add_argument(
        "--ssh-timeout",
        type=int,
        default=DEFAULT_SSH_TIMEOUT_SECONDS,
        help="maximum seconds for one condor_ssh_to_job snapshot",
    )
    live_parser.add_argument("--pretty", action="store_true")
    live_parser.add_argument(
        "--count-lhe-events",
        action="store_true",
        help="recursively count <event> records during each snapshot",
    )

    args = parser.parse_args(argv)
    if args.command == "parse":
        emit(parse_root(args.root, count_lhe_events=args.count_lhe_events), args.pretty)
        return 0
    if args.watch < 0:
        parser.error("--watch must be non-negative")
    if args.ssh_timeout <= 0:
        parser.error("--ssh-timeout must be positive")
    while True:
        emit(
            live_snapshot(
                args.schedd,
                args.job,
                count_lhe_events=args.count_lhe_events,
                ssh_timeout_seconds=args.ssh_timeout,
            ),
            args.pretty,
        )
        if args.watch == 0:
            return 0
        time.sleep(args.watch)


if __name__ == "__main__":
    raise SystemExit(main())
