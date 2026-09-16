#!/usr/bin/env python3
"""Report HELAC trial status with configuration and physics output together.

Examples:
  python3 tools/helac_trial_report.py \
    --job bigbird17.cern.ch:9571100 \
    --config 9571100=generated/.../LHE_SPS_G_nmc1e7_unwevt1e5.json \
    --archive 9571100=/tmp/chiw/9571100.tar.gz

The tool is read-only apart from a temporary extraction directory used for a
supplied diagnostic archive.  A live job is read through ``condor_ssh_to_job``
and therefore reports the worker's actual ``user.inp`` values.  Use
``--count-live-events`` for an explicit live LHE-event scan; supplied completed
archives are always counted.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tarfile
import tempfile
import warnings
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

from helac_inspector import parse_root, remote_snapshot


STATUS_NAMES = {
    1: "idle",
    2: "running",
    3: "removed",
    4: "completed",
    5: "held",
    6: "transferring-output",
    7: "suspended",
}


MappingKey = Tuple[Optional[str], str]


def normalize_job(job: str) -> str:
    return job if "." in job else f"{job}.0"


def parse_mapping_key(value: str) -> MappingKey:
    if ":" in value:
        return parse_job(value)
    return None, normalize_job(value)


def parse_mapping(values: Iterable[str], option: str) -> Dict[MappingKey, Path]:
    mapping: Dict[MappingKey, Path] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"{option} must be JOB=PATH, got {value!r}")
        job, path = value.split("=", 1)
        mapping[parse_mapping_key(job)] = Path(path)
    return mapping


def parse_job(value: str) -> tuple[str, str]:
    if ":" not in value:
        raise ValueError(f"--job must be SCHEDD:CLUSTER[.PROC], got {value!r}")
    schedd, job = value.split(":", 1)
    if not schedd or not job:
        raise ValueError(f"--job must be SCHEDD:CLUSTER[.PROC], got {value!r}")
    return schedd, job if "." in job else f"{job}.0"


def mapped_path(
    mapping: Dict[MappingKey, Path], schedd: str, job: str
) -> Optional[Path]:
    return mapping.get((schedd, job)) or mapping.get((None, job))


def load_config(path: Optional[Path]) -> Dict[str, Any]:
    if path is None:
        return {}
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def latest_result(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    candidates = []
    for run in snapshot.get("runs", []):
        for calculation in run.get("calculations", []):
            candidates.extend(calculation.get("result_files", []))
    return max(candidates, key=lambda item: item.get("mtime", 0), default={})


def safe_extract_archive(archive: Path, destination: Path) -> None:
    with tarfile.open(archive, "r:gz") as handle:
        for member in handle.getmembers():
            target = (destination / member.name).resolve()
            if destination.resolve() not in target.parents and target != destination.resolve():
                raise ValueError(f"Unsafe archive member: {member.name}")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            handle.extractall(destination)


def archive_snapshot(archive: Path) -> Dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="helac-trial-report-") as tmpdir:
        root = Path(tmpdir) / "HELAC-Onia-2.7.6"
        root.mkdir()
        safe_extract_archive(archive, root)
        return parse_root(root, count_lhe_events=True)


def queue_status(schedd: str, job: str) -> Dict[str, Any]:
    command = [
        "condor_q", "-name", schedd, job, "-nobatch", "-af",
        "JobStatus", "HoldReason", "LastRemoteHost", "RemoteWallClockTime",
    ]
    try:
        result = subprocess.run(
            command, text=True, capture_output=True, check=False, timeout=20,
        )
    except subprocess.TimeoutExpired:
        return {"state": "not-live", "queue_error": "condor_q timed out"}
    fields = result.stdout.strip().split()
    if result.returncode != 0 or not fields or not fields[0].isdigit():
        return {"state": "not-live", "queue_error": (result.stderr or "").strip()}
    code = int(fields[0])
    return {"state": STATUS_NAMES.get(code, "unknown"), "status_code": code}


def history_status(schedd: str, job: str) -> Dict[str, Any]:
    command = [
        "condor_history", "-name", schedd, job, "-af",
        "JobStatus", "ExitCode", "ExitBySignal", "CompletionDate", "HoldReason",
    ]
    try:
        result = subprocess.run(
            command, text=True, capture_output=True, check=False, timeout=20,
        )
    except subprocess.TimeoutExpired:
        return {"state": "not-found", "queue_error": "condor_history timed out"}
    fields = result.stdout.strip().split()
    if not fields or not fields[0].isdigit():
        return {"state": "not-found"}
    code = int(fields[0])
    return {
        "state": STATUS_NAMES.get(code, "unknown"),
        "exit_code": fields[1] if len(fields) > 1 else None,
        "exit_by_signal": fields[2] if len(fields) > 2 else None,
        "completion_date": fields[3] if len(fields) > 3 else None,
        "hold_reason": " ".join(fields[4:]) if len(fields) > 4 else None,
    }


def compact_config(config: Dict[str, Any], parameters: Dict[str, Any]) -> str:
    def value(json_key: str, inp_key: str, default: str = "?") -> str:
        raw = config.get(json_key, parameters.get(inp_key, default))
        return str(raw)

    return (
        f"nmc={value('nmc', 'nmc')}; unwevt={value('unwevt', 'unwevt')}; "
        f"minptq={value('min_pt_q', 'minptq')}; "
        f"minmqqp={value('min_m_qqp', 'minmqqp', '0')}; "
        f"minmqbeam={value('min_m_q_beam', 'minmqbeam', '0')}; "
        f"minptconia={value('min_pt_conia', 'minptconia')}; "
        f"minptbonia={value('min_pt_bonia', 'minptbonia')}; "
        f"seed={value('seed', 'ranhel')}"
    )


def compact_result(snapshot: Dict[str, Any]) -> str:
    result = latest_result(snapshot)
    events = snapshot.get("observed_lhe_events")
    if result.get("final_sigma_nb") is not None:
        return (
            f"sigma={result['final_sigma_nb']:.8g} +/- {result['final_error_nb']:.8g} nb; "
            f"rel={result.get('final_relative_error_percent', 0):.5g}%; "
            f"events={events if events is not None else '?'}"
        )
    last = result.get("last")
    if last:
        event_text = str(events) if events is not None else "not counted"
        return (
            f"sigma={last['sigma_nb']:.8g} nb; rel={last['relative_error_percent']:.5g}%; "
            f"points={last['points_used']}/{last['points_target']}; "
            f"events={event_text}"
        )
    event_text = str(events) if events is not None else "not counted"
    return f"stage={snapshot.get('stage', 'unknown')}; events={event_text}"


def report_one(
    schedd: str,
    job: str,
    config: Dict[str, Any],
    archive: Optional[Path],
    count_live_events: bool,
) -> Dict[str, str]:
    queue = queue_status(schedd, job)
    state = queue.get("state", "unknown")
    snapshot: Dict[str, Any] = {}
    if state == "running":
        snapshot = remote_snapshot(
            schedd, job, count_lhe_events=count_live_events,
        )
        parameters = snapshot.get("parameters", {})
    else:
        queue = history_status(schedd, job)
        state = queue.get("state", "not-found")
        if archive is not None:
            snapshot = archive_snapshot(archive)
            parameters = snapshot.get("parameters", {})
            if state == "not-found":
                state = "completed-output"
        else:
            parameters = {}
    return {
        "job": f"{schedd}:{job}",
        "status": state,
        "requested_configuration": compact_config(config, {}),
        "observed_configuration": (
            compact_config({}, parameters) if parameters else "unavailable"
        ),
        "result": compact_result(snapshot) if snapshot else "result unavailable",
        "detail": (
            f"exit={queue.get('exit_code')}" if state == "completed" else
            str(queue.get("hold_reason") or queue.get("queue_error") or "")
        ),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", action="append", required=True,
                        help="SCHEDD:CLUSTER[.PROC]; repeat for each job")
    parser.add_argument(
        "--config", action="append", default=[], metavar="[SCHEDD:]JOB=PATH",
        help="requested JSON configuration; preserve the Condor process suffix",
    )
    parser.add_argument(
        "--archive", action="append", default=[], metavar="[SCHEDD:]JOB=PATH",
        help="diagnostic archive for a completed job",
    )
    parser.add_argument(
        "--count-live-events", action="store_true",
        help="count <event> records on live workers; can be slow for large LHE files",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON instead of Markdown")
    args = parser.parse_args(argv)
    try:
        configs = parse_mapping(args.config, "--config")
        archives = parse_mapping(args.archive, "--archive")
        records = []
        for raw_job in args.job:
            schedd, job = parse_job(raw_job)
            records.append(report_one(
                schedd,
                job,
                load_config(mapped_path(configs, schedd, job)),
                mapped_path(archives, schedd, job),
                args.count_live_events,
            ))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    if args.json:
        print(json.dumps(records, indent=2))
        return 0
    print("| Job | Status | Requested configuration | Worker/archive configuration | Result | Detail |")
    print("|---|---|---|---|---|---|")
    for record in records:
        print(
            "| {job} | {status} | {requested_configuration} | "
            "{observed_configuration} | {result} | {detail} |".format(**record)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
