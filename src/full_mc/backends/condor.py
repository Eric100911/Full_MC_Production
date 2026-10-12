"""Durable, exact-attempt HTCondor DAGMan operations.

The backend deliberately does not search a user's queue.  A submission records
the schedd and DAGMan (root) cluster ID together, and all later observations
address that exact pair.  In particular, an interrupted submit is ambiguous:
it is never retried automatically.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

from full_mc.credentials import require_initial_proxy_lifetime
from full_mc.io import atomic_json
from full_mc.storage.reservation import reserve_run_namespaces


SCHEMA = "full-mc-condor-submission-v1"
_CLUSTER = re.compile(r"(?:submitted\s+to\s+)?cluster\s+(\d+)", re.IGNORECASE)
_RESCUE = re.compile(r"^run\.dag\.rescue(\d+)$")
_JOB_STATUS = {
    "1": "idle", "2": "running", "3": "removed", "4": "completed",
    "5": "held", "6": "transferring_output", "7": "suspended",
}
_TERMINAL = {"removed", "completed"}
# -limit/-match bound only the jobs condor_history displays; without -scanlimit
# the schedd still walks its whole history, which costs minutes on a shared
# CERN schedd and scales with how deep the requested cluster sits.  10000 is
# below the ~15000 rotation boundary where per-job cost jumps ~14x.  The client
# timeout only releases this process; the schedd-side scan is capped by
# -scanlimit alone.
_HISTORY_SCAN_LIMIT = 10000
_STATUS_QUERY_TIMEOUT_SECONDS = 60


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid JSON %s: %s" % (path, exc)) from exc
    if not isinstance(value, dict):
        raise ValueError("%s must contain a JSON object" % path)
    return value


def _run_paths(run_dir: str | Path) -> tuple[Path, Path, Path]:
    root = Path(run_dir).resolve()
    dag = root / "run.dag"
    spec = root / "run.json"
    if not dag.is_file():
        raise FileNotFoundError("missing DAG: %s" % dag)
    if not spec.is_file():
        raise FileNotFoundError("missing run specification: %s" % spec)
    return root, dag, spec


def _configured_schedd(spec: Dict[str, Any]) -> str | None:
    condor = spec.get("condor", {})
    if not isinstance(condor, dict):
        raise ValueError("run.json condor must be an object")
    scheduler = spec.get("scheduler", {})
    if scheduler is not None and not isinstance(scheduler, dict):
        raise ValueError("run.json scheduler must be an object")
    value = condor.get("schedd") or spec.get("schedd")
    if value is None and isinstance(scheduler, dict):
        value = scheduler.get("schedd")
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        raise ValueError("configured schedd must not be empty")
    return value


def _assert_submittable(spec: Dict[str, Any]) -> None:
    if spec.get("runtime_source_dirty"):
        raise RuntimeError("refusing scheduler submission from a dirty runtime source")


def _worker_proxy_paths(root: Path) -> List[str]:
    """Read the proxy paths pinned in this run's generated worker submit files."""
    submit_files = sorted((root / "submit").glob("*.sub"))
    if not submit_files:
        raise RuntimeError("run has no worker submit files with a pinned X.509 proxy")
    paths = set()
    for submit_file in submit_files:
        matches = re.findall(r"(?im)^[ \t]*x509userproxy[ \t]*=[ \t]*(\S+)[ \t]*$",
                             submit_file.read_text(encoding="utf-8"))
        if len(matches) != 1 or not Path(matches[0]).is_absolute():
            raise RuntimeError(f"invalid pinned X.509 proxy in {submit_file}")
        paths.add(matches[0])
    return sorted(paths)


def _run(command: List[str], runner: Callable[..., Any], *, cwd: Path,
         scheduler: str | None = None, timeout: float | None = None) -> Any:
    environment = None
    if scheduler:
        environment = dict(os.environ, _CONDOR_SCHEDD_HOST=scheduler)
    return runner(command, check=False, text=True, capture_output=True, cwd=str(cwd),
                  timeout=timeout,
                  **({"env": environment} if environment is not None else {}))


def _output(completed: Any) -> str:
    return (str(getattr(completed, "stdout", "")) + "\n" +
            str(getattr(completed, "stderr", "")))[-4000:]


def _schedd(spec: Dict[str, Any], runner: Callable[..., Any], root: Path) -> str:
    """Use an explicitly pinned schedd, or resolve the local default once."""
    configured = _configured_schedd(spec)
    if configured:
        return configured
    try:
        completed = _run(["condor_config_val", "SCHEDD_NAME"], runner, cwd=root)
    except OSError as exc:
        raise RuntimeError("cannot resolve the default schedd: %s" % exc) from exc
    value = str(getattr(completed, "stdout", "")).strip()
    if getattr(completed, "returncode", 1) != 0 or not value:
        raise RuntimeError("cannot resolve the default schedd: %s" % _output(completed))
    return value.splitlines()[0].strip()


def _submission_path(root: Path) -> Path:
    return root / "submission.json"


def _load_submission(root: Path) -> Dict[str, Any]:
    path = _submission_path(root)
    if not path.is_file():
        raise FileNotFoundError("run has not been submitted: %s" % path)
    record = _read_json(path)
    if record.get("schema") != SCHEMA:
        raise ValueError("unsupported Condor submission schema")
    return record


def _write(root: Path, record: Dict[str, Any]) -> None:
    atomic_json(_submission_path(root), record)


def _begin_attempt(record: Dict[str, Any], *, kind: str, scheduler: str,
                   command: List[str], rescue_file: str | None = None) -> Dict[str, Any]:
    attempts = record.setdefault("attempts", [])
    attempt = {"number": len(attempts) + 1, "kind": kind, "state": "submitting",
               "scheduler": scheduler, "root_cluster_id": None,
               "command": command, "submitted_at": _now()}
    if rescue_file:
        attempt["rescue_file"] = rescue_file
    attempts.append(attempt)
    # These duplicated top-level fields keep the current exact target easy for
    # shell operators to inspect, while attempts preserve older root DAGs.
    record.update(state="submitting", scheduler=scheduler, root_cluster_id=None,
                  current_attempt=attempt["number"], updated_at=_now())
    return attempt


def _finish_attempt(record: Dict[str, Any], attempt: Dict[str, Any], completed: Any) -> None:
    text = _output(completed)
    match = _CLUSTER.search(text)
    cluster = match.group(1) if match else None
    attempt.update(finished_at=_now(), submit_output=text, root_cluster_id=cluster)
    if getattr(completed, "returncode", 1) == 0 and cluster:
        attempt["state"] = "submitted"
        record.update(state="submitted", scheduler=attempt["scheduler"],
                      root_cluster_id=cluster, updated_at=_now())
    else:
        # A successful server-side submit can be followed by a client-side
        # transport failure.  Treat every incomplete acknowledgement as unsafe.
        attempt.update(state="unknown", reconciliation_required=True)
        record.update(state="unknown", scheduler=attempt["scheduler"],
                      root_cluster_id=cluster, reconciliation_required=True,
                      updated_at=_now())


def submit_run(run_dir: str | Path, runner: Callable[..., Any] = subprocess.run) -> Dict[str, Any]:
    """Submit ``run.dag`` once, persisting the attempt before invoking Condor."""
    root, _dag, spec_path = _run_paths(run_dir)
    destination = _submission_path(root)
    if destination.exists():
        record = _load_submission(root)
        raise RuntimeError("submission already recorded as %s; use status or explicit rescue resume" %
                           record.get("state", "unknown"))
    spec_bytes = spec_path.read_bytes()
    spec = _read_json(spec_path)
    _assert_submittable(spec)
    for proxy_path in _worker_proxy_paths(root):
        require_initial_proxy_lifetime(proxy_path)
    # Ownership is established before the durable Condor attempt record and
    # before invoking condor_submit_dag.  A failed reservation therefore cannot
    # leave an ambiguous scheduler submission behind.
    if spec.get("storage"):
        reserve_run_namespaces(spec, spec_bytes=spec_bytes)
    scheduler = _schedd(spec, runner, root)
    # ``-Remote`` enables spool submission, which CERN rejects unless the root
    # DAG declares spool outputs. Select the recorded schedd through HTCondor's
    # process-local configuration and submit normally instead.
    command = ["condor_submit_dag", "-no_recurse", "run.dag"]
    record: Dict[str, Any] = {"schema": SCHEMA, "run_dir": str(root),
                              "dag": str(root / "run.dag"), "created_at": _now(),
                              "attempts": []}
    attempt = _begin_attempt(record, kind="submit", scheduler=scheduler, command=command)
    _write(root, record)
    try:
        completed = _run(command, runner, cwd=root, scheduler=scheduler)
    except OSError as exc:
        attempt.update(state="unknown", reconciliation_required=True, finished_at=_now(),
                       submit_output=str(exc))
        record.update(state="unknown", reconciliation_required=True, updated_at=_now())
        _write(root, record)
        return record
    _finish_attempt(record, attempt, completed)
    _write(root, record)
    return record


def _parse_status(text: str, cluster: str) -> List[tuple[str, str | None]]:
    states = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 3 or fields[0] != cluster:
            continue
        state = _JOB_STATUS.get(fields[2], fields[2].lower())
        states.append((state, fields[3] if len(fields) > 3 else None))
    return states


def _query(root: Path, scheduler: str, cluster: str, executable: str,
           runner: Callable[..., Any]) -> List[tuple[str, str | None]] | None:
    command = [executable, "-name", scheduler, cluster, "-autoformat",
               "ClusterId", "ProcId", "JobStatus", "ExitCode"]
    if executable == "condor_history":
        command[1:1] = ["-scanlimit", str(_HISTORY_SCAN_LIMIT)]
    try:
        completed = _run(command, runner, cwd=root,
                         timeout=_STATUS_QUERY_TIMEOUT_SECONDS)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if getattr(completed, "returncode", 1) != 0:
        return None
    return _parse_status(str(getattr(completed, "stdout", "")), cluster)


def _local_success_evidence(root: Path, cluster: str) -> Dict[str, Any] | None:
    """Confirm mcprod success after the exact DAGMan proc leaves queue/history.

    A blank queue and a rotated/empty history scan do not prove failure. The
    run-owned completion marker, completed controller state, and metrics tied
    to this root cluster together provide durable success evidence.
    """
    try:
        state = _read_json(root / "state.json")
        metrics = _read_json(root / "run.dag.metrics")
        metrics_cluster = str(metrics.get("dagman_id", ""))
        dag_nodes = int(metrics["dag_nodes"])
        succeeded = int(metrics["dag_nodes_succeeded"])
        failed = int(metrics["dag_nodes_failed"])
        exit_code = int(metrics["exitcode"])
        dag_status = int(metrics["DagStatus"])
    except (OSError, KeyError, TypeError, ValueError):
        return None
    if not (
        state.get("status") == "complete"
        and (root / ".complete").is_file()
        and metrics_cluster == cluster
        and exit_code == 0
        and dag_status == 0
        and failed == 0
        and succeeded == dag_nodes
    ):
        return None
    return {
        "state": "terminal",
        "terminal": True,
        "successful": True,
        "query": "local-evidence",
        "local_evidence": {
            "state_complete": True,
            "complete_marker": True,
            "metrics_cluster": metrics_cluster,
            "dag_status": dag_status,
            "root_exit_code": exit_code,
            "dag_nodes_succeeded": succeeded,
            "dag_nodes_failed": failed,
        },
    }


def _local_failed_terminal_evidence(root: Path, cluster: str) -> Dict[str, Any] | None:
    """Confirm an exact failed root when queue/history no longer retain it.

    DAGMan writes metrics and a rescue file before leaving the queue.  Together
    they are sufficient to authorize a rescue of this exact root, but never
    count as successful completion evidence.
    """
    try:
        metrics = _read_json(root / "run.dag.metrics")
        metrics_cluster = str(metrics["dagman_id"])
        dag_nodes = int(metrics["dag_nodes"])
        succeeded = int(metrics["dag_nodes_succeeded"])
        failed = int(metrics["dag_nodes_failed"])
        exit_code = int(metrics["exitcode"])
        dag_status = int(metrics["DagStatus"])
    except (OSError, KeyError, TypeError, ValueError):
        return None
    rescue = _latest_rescue(root)
    if not (
        metrics_cluster == cluster
        and rescue is not None
        and (exit_code != 0 or dag_status != 0 or failed != 0)
    ):
        return None
    return {
        "state": "terminal",
        "terminal": True,
        "successful": False,
        "query": "local-evidence",
        "local_evidence": {
            "metrics_cluster": metrics_cluster,
            "dag_status": dag_status,
            "root_exit_code": exit_code,
            "dag_nodes": dag_nodes,
            "dag_nodes_succeeded": succeeded,
            "dag_nodes_failed": failed,
            "rescue_file": str(rescue),
        },
    }


def status_run(run_dir: str | Path, runner: Callable[..., Any] = subprocess.run) -> Dict[str, Any]:
    """Report the state of the recorded root DAGMan job, never a queue window."""
    root, _dag, _spec = _run_paths(run_dir)
    record = _load_submission(root)
    cluster = record.get("root_cluster_id")
    scheduler = record.get("scheduler")
    if not cluster or not scheduler:
        return {"state": record.get("state", "unknown"), "terminal": False,
                "scheduler": scheduler, "root_cluster_id": cluster,
                "reconciliation_required": True}
    cluster, scheduler = str(cluster), str(scheduler)
    states = _query(root, scheduler, cluster, "condor_q", runner)
    source = "queue"
    if states == []:
        states = _query(root, scheduler, cluster, "condor_history", runner)
        source = "history"
    if states is None or not states:
        result = _local_success_evidence(root, cluster)
        if result is None:
            result = _local_failed_terminal_evidence(root, cluster)
        if result is None:
            result = {"state": "unknown", "terminal": False, "scheduler": scheduler,
                      "root_cluster_id": cluster, "query": source}
        else:
            result.update(scheduler=scheduler, root_cluster_id=cluster)
    else:
        unique = sorted({state for state, _exit_code in states})
        terminal = all(state in _TERMINAL for state in unique)
        successful = bool(states) and all(state == "completed" and exit_code == "0"
                                          for state, exit_code in states)
        result = {"state": "terminal" if terminal else "active", "terminal": terminal,
                  "successful": successful, "scheduler": scheduler,
                  "root_cluster_id": cluster, "query": source,
                  "scheduler_states": unique}
    record["last_status"] = dict(result, observed_at=_now())
    _write(root, record)
    return result


def _latest_rescue(root: Path) -> Path | None:
    candidates = []
    for path in root.glob("run.dag.rescue*"):
        match = _RESCUE.match(path.name)
        if match and path.is_file():
            candidates.append((int(match.group(1)), path))
    return max(candidates, default=(None, None))[1]


def resume_run(run_dir: str | Path, runner: Callable[..., Any] = subprocess.run) -> Dict[str, Any]:
    """Submit an explicitly selected rescue DAG after exact terminal confirmation."""
    root, _dag, _spec = _run_paths(run_dir)
    _assert_submittable(_read_json(_spec))
    record = _load_submission(root)
    if record.get("state") in {"submitting", "unknown"}:
        raise RuntimeError("a submission attempt is still ambiguous; reconcile it before resuming")
    observed = status_run(root, runner=runner)
    if not observed.get("terminal"):
        raise RuntimeError("rescue resume requires a known terminal root DAGMan state")
    rescue = _latest_rescue(root)
    if rescue is None:
        raise FileNotFoundError("rescue resume requires an explicit run.dag.rescueNNN file")
    rescue_match = _RESCUE.match(rescue.name)
    if rescue_match is None:
        raise ValueError("invalid rescue filename: %s" % rescue.name)
    rescue_number = str(int(rescue_match.group(1)))
    scheduler = str(observed["scheduler"])
    command = ["condor_submit_dag", "-no_recurse", "-AutoRescue", "0",
               "-DoRescueFrom", rescue_number, "run.dag"]
    attempt = _begin_attempt(record, kind="rescue", scheduler=scheduler, command=command,
                             rescue_file=str(rescue))
    _write(root, record)
    try:
        completed = _run(command, runner, cwd=root, scheduler=scheduler)
    except OSError as exc:
        attempt.update(state="unknown", reconciliation_required=True, finished_at=_now(),
                       submit_output=str(exc))
        record.update(state="unknown", reconciliation_required=True, updated_at=_now())
        _write(root, record)
        return record
    _finish_attempt(record, attempt, completed)
    _write(root, record)
    return record
