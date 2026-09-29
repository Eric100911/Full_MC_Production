"""Explicit, resumable HepJob submission for combined IHEP merge groups.

No queue-wide heuristics live here: every decision is based on a manifest job
ID and exact task membership.  Submission is serial; it does not imply that scheduled workers run serially.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

from full_mc.io import atomic_json as _atomic_json
from full_mc.runtime.packages import validate_runtime_release
from full_mc.settings import CORE_ROOT, LOG_LEVELS


SCHEMA = "full-mc-ihep-batch-v2"
_SCHEMAS = {"full-mc-ihep-batch-v1", SCHEMA}
SAFE_NTUPLE_NODES = (
    "ams017", "ams018", "ams019", "ams020", "cws047", "cws048", "cws049",
    "cws051", "cws052", "cws053", "cws054",
)
_ACTIVE_CODES = {"I", "R", "H", "S"}
_FAILED_CODES = {"E", "F", "X"}


def _log_level(value):
    value = str(value or "info")
    if value not in LOG_LEVELS:
        raise ValueError("log_level must be one of %s" % (LOG_LEVELS,))
    return value


def _read(path: str | Path) -> Dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid JSON %s: %s" % (path, exc)) from exc
    if not isinstance(value, dict):
        raise ValueError("JSON object required")
    return value


def _group_key(task: Dict[str, Any]) -> str:
    parameters = task.get("parameters", {})
    return str(parameters.get("merge_group") or parameters.get("group_id") or task.get("task_id"))


def _combined_tasks(handoff: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    by_group: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for task in handoff.get("tasks", []):
        if task.get("stage") == "combined":
            yield task
            continue
        stage = str(task.get("stage", ""))
        if stage not in {"merge", "ntuple", "selection", "efficiency"}:
            continue
        by_group.setdefault(_group_key(task), {})[stage] = task
    for group, stages in sorted(by_group.items()):
        analysis = str(stages.get("merge", {}).get("parameters", {}).get("analysis", "JJP"))
        required = ("merge", "ntuple") if analysis == "JUP" else (
            "merge", "ntuple", "selection", "efficiency")
        if analysis not in {"JJP", "JUP"}:
            raise ValueError("merge group %s has unsupported analysis %s" % (group, analysis))
        missing = [stage for stage in required if stage not in stages]
        if missing:
            raise ValueError("merge group %s is incomplete: missing %s" % (group, ", ".join(missing)))
        merge = stages["merge"]
        yield {
            "task_id": "combined:" + group,
            "stage": "combined",
            "runtime_release": merge.get("runtime_release"),
            "log_level": merge.get("log_level", "info"),
            "subtasks": stages,
            "task_membership": [stages[name]["task_id"] for name in required],
        }


def _write_worker(path, task_path, core_root, proxy):
    source = shlex.quote(str(proxy)) if proxy else '"${X509_USER_PROXY:?missing proxy}"'
    command = shlex.join(["python3", str(Path(core_root) / "mcprod"), "batch", "worker",
                          "--task", str(task_path)])
    text = (
        "#!/bin/bash\nset -euo pipefail\n"
        "proxy_source=" + source + "\n"
        '[[ -s "$proxy_source" ]] || { echo "missing proxy" >&2; exit 2; }\n'
        "mkdir -p /tmp/chiw\n"
        "job_scratch=$(mktemp -d /tmp/chiw/mcprod-ihep.XXXXXX)\n"
        'cd "$job_scratch"\n'
        "mkdir credentials\n"
        'install -m 600 "$proxy_source" credentials/x509_user_proxy\n'
        'export X509_USER_PROXY="$PWD/credentials/x509_user_proxy"\n'
        "exec " + command + ' --work-dir "$job_scratch/worker"\n'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


def _runtime_for_handoff(handoff, runtime_dir):
    """Require the receiving release to be the handoff's frozen core revision."""
    expected_release = str(handoff.get("runtime_release", ""))
    expected_revision = str(handoff.get("core_revision", ""))
    if not expected_release or not expected_revision:
        # Transitional handoffs from before the core pin existed are accepted
        # only for backwards-compatible offline records.
        return None
    if runtime_dir is None:
        raise ValueError("a pinned handoff requires --runtime-dir at the receiving site")
    runtime = validate_runtime_release(runtime_dir, expected_release=expected_release,
                                       expected_site="ihep")
    if runtime.get("git_revision") != expected_revision:
        raise ValueError("IHEP runtime core revision does not match the handoff")
    if runtime.get("source_dirty"):
        raise ValueError("refusing a receiving runtime built from a dirty core source")
    available_profiles = set(runtime["profiles"])
    required = {
        "cmssw12-sim-el8" if "cmssw12-sim-el8" in available_profiles else "cmssw12-el8",
        "cmssw15-ntuple-el9" if "cmssw15-ntuple-el9" in available_profiles else "cmssw15-el9",
    }
    analyses = {str(task.get("parameters", {}).get("analysis", "JJP"))
                for task in handoff.get("tasks", []) if task.get("stage") != "combined"}
    analyses.update(str(task.get("subtasks", {}).get("merge", {}).get("parameters", {}).get("analysis", "JJP"))
                    for task in handoff.get("tasks", []) if task.get("stage") == "combined")
    if "JJP" in analyses:
        required.add("analyzer-el9")
    if not required <= set(runtime["profiles"]):
        raise ValueError("IHEP runtime lacks required merge/ntuple/analysis profiles")
    return Path(runtime_dir).resolve() / "controller/core"


def prepare_batch(handoff_path: str | Path, workspace: str | Path, *, proxy: str = None, core_root: str | Path = CORE_ROOT,
                  runtime_dir: str | Path | None = None, group: str = "cms", walltime: str = "mid", memory_mb: int = 0,
                  log_level: str = None) -> Dict[str, Any]:
    """Prepare one pinned combined allocation per merge group; do not submit."""
    handoff = _read(handoff_path)
    if handoff.get("schema") != "full-mc-handoff-v1":
        raise ValueError("unsupported handoff schema")
    pinned_core = _runtime_for_handoff(handoff, runtime_dir)
    effective_core = pinned_core or Path(core_root).resolve()
    root = Path(workspace).resolve()
    if (root / "batch_manifest.json").exists():
        raise FileExistsError("batch already exists; use its existing manifest")
    root.mkdir(parents=True, exist_ok=True)
    jobs: List[Dict[str, Any]] = []
    for index, combined in enumerate(_combined_tasks(handoff)):
        task_path = root / "tasks" / ("%04d.json" % index)
        combined["checkpoint_dir"] = str(root / "checkpoints" / ("%04d" % index))
        if pinned_core:
            combined["runtime_dir"] = str(Path(runtime_dir).resolve())
        level = _log_level(log_level or combined.get("log_level"))
        combined["log_level"] = level
        if not combined.get("runtime_release"):
            raise ValueError("combined tasks require a pinned runtime release")
        for subtask in combined["subtasks"].values():
            subtask["log_level"] = level
            subtask.setdefault("parameters", {})["core_root"] = str(effective_core)
        _atomic_json(task_path, combined)
        wrapper = root / "wrappers" / ("%04d.sh" % index)
        _write_worker(wrapper, task_path, effective_core, proxy)
        jobs.append({"job_id": None, "index": index, "state": "prepared", "task_path": str(task_path),
                     "wrapper": str(wrapper), "task_ids": combined.get("task_membership", [combined["task_id"]]),
                     "node": SAFE_NTUPLE_NODES[index % len(SAFE_NTUPLE_NODES)] + ".ihep.ac.cn"})
    run_id = str(handoff.get("run_id") or Path(handoff_path).resolve().parent.name)
    manifest = {"schema": SCHEMA, "created_at": datetime.now(timezone.utc).isoformat(), "run_id": run_id,
                "handoff": str(Path(handoff_path).resolve()), "workspace": str(root), "group": group,
                "walltime": walltime, "memory_mb": int(memory_mb), "safe_nodes": list(SAFE_NTUPLE_NODES),
                "runtime_dir": str(Path(runtime_dir).resolve()) if pinned_core else None,
                "core_revision": handoff.get("core_revision"), "jobs": jobs}
    manifest_path = root / "batch_manifest.json"
    _atomic_json(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def _submit_command(manifest: Dict[str, Any], job: Dict[str, Any]) -> List[str]:
    command = ["hep_sub", str(job["wrapper"]), "-g", str(manifest["group"]), "-gwn", "CMS", "-wt", str(manifest["walltime"]),
               "-n", "1", "-wn", str(job["node"])]
    if int(manifest.get("memory_mb", 0)) > 0:
        command.extend(["-mem", str(manifest["memory_mb"])])
    logs = Path(manifest["workspace"]) / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    command.extend(["-o", str(logs / ("%04d.out" % job["index"])), "-e", str(logs / ("%04d.err" % job["index"]))])
    return command


def _job_id(text: str) -> str | None:
    match = re.search(r"(?:cluster|job)\s*(?:id)?\s*[:#]?\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
    return match.group(1) if match else None


def _environment():
    environment = dict(os.environ)
    environment["PATH"] = "/cvmfs/common.ihep.ac.cn/software/hepjob/bin:" + environment.get("PATH", "")
    return environment


def _record_attempt(job, command):
    attempts = job.setdefault("attempts", [])
    attempt = {"number": len(attempts) + 1, "state": "submitting", "command": command,
               "submitted_at": datetime.now(timezone.utc).isoformat()}
    attempts.append(attempt)
    return attempt


def submit_batch(manifest_path: str | Path, runner: Callable[..., Any] = subprocess.run) -> Dict[str, Any]:
    """Submit prepared jobs serially.  Ambiguous attempts become ``unknown``."""
    path = Path(manifest_path)
    manifest = _read(path)
    if manifest.get("schema") not in _SCHEMAS:
        raise ValueError("unsupported batch manifest")
    for job in manifest["jobs"]:
        if job.get("state") != "prepared":
            continue
        command = _submit_command(manifest, job)
        attempt = _record_attempt(job, command)
        job.update(state="submitting", submit_command=command,
                   submitted_at=attempt["submitted_at"], reconciliation_required=False)
        _atomic_json(path, manifest)
        try:
            completed = runner(command, check=False, text=True, capture_output=True,
                               env=_environment())
        except OSError as exc:
            job.update(state="unknown", reconciliation_required=True, submit_output=str(exc))
            attempt.update(state="unknown", reconciliation_required=True, output=str(exc))
            _atomic_json(path, manifest)
            continue
        stdout = str(getattr(completed, "stdout", "")); stderr = str(getattr(completed, "stderr", ""))
        found = _job_id(stdout + "\n" + stderr)
        job["submit_command"] = command
        job["submitted_at"] = datetime.now(timezone.utc).isoformat()
        job["submit_output"] = (stdout + stderr)[-4000:]
        if getattr(completed, "returncode", 1) == 0 and found:
            job.update({"state": "submitted", "job_id": found})
            attempt.update(state="submitted", job_id=found, output=job["submit_output"])
        else:
            # Do not risk a duplicate allocation.  The operator must reconcile
            # this exact task membership/job attempt before another submission.
            job.update({"state": "unknown", "job_id": found, "reconciliation_required": True})
            attempt.update(state="unknown", job_id=found, reconciliation_required=True,
                           output=job["submit_output"])
        _atomic_json(path, manifest)
    return manifest


def _hep_q(job_id, runner):
    """Query one exact cluster and retry an empty response once.

    An empty response can be IHEP login-node fork exhaustion or a completed job
    that has left the queue. It is never completion evidence.
    """
    command = ["hep_q", "-i", str(job_id)]
    for attempt in range(2):
        try:
            completed = runner(command, check=False, text=True, capture_output=True,
                               env=_environment())
        except OSError as exc:
            return None, "error: %s" % exc
        stdout = str(getattr(completed, "stdout", "")).strip()
        stderr = str(getattr(completed, "stderr", "")).strip()
        if getattr(completed, "returncode", 1) != 0:
            return None, (stderr or stdout)[-4000:]
        # hep_q can print argparse diagnostics to stderr while returning 0.
        # Never treat that text as a scheduler state: doing so can mark a live
        # job failed and make explicit recovery duplicate its allocation.
        if stdout:
            return stdout[-4000:], None
        if stderr:
            return None, stderr[-4000:]
        if attempt:
            return "", None
    return "", None


def _hep_state(text, job_id):
    """Read only the requested job row, never hep_q's aggregate counters."""
    if not text or re.search(r"(?im)^\s*(?:usage:|.*\berror:)", text):
        return "unknown"
    requested = str(job_id)
    if not re.fullmatch(r"\d+(?:\.\d+)?", requested):
        return "unknown"
    rows = []
    in_table = False
    for line in text.splitlines():
        if re.match(r"^\s*JOBID\s+OWNER\s+SUBMITTED\s+RUN_TIME\s+ST\b", line):
            in_table = True
            continue
        if not in_table:
            continue
        # SUBMITTED occupies a date and time column in HepJob's table.
        match = re.match(
            r"^\s*(?P<id>\d+(?:\.\d+)?)\s+\S+\s+\S+\s+\S+\s+\S+\s+"
            r"(?P<state>[A-Z])(?:\s|$)", line
        )
        if not match:
            continue
        row_id = match.group("id")
        if row_id == requested or ("." not in requested and row_id.split(".")[0] == requested):
            rows.append(match.group("state"))
    if len(rows) != 1:
        return "unknown"
    if rows[0] in _ACTIVE_CODES:
        return "active"
    if rows[0] in _FAILED_CODES:
        return "failed"
    if rows[0] == "C":
        return "completed"
    return "unknown"


def status_batch(manifest_path: str | Path,
                 runner: Callable[..., Any] = subprocess.run) -> Dict[str, Any]:
    """Reconcile submitted jobs by their recorded HepJob IDs only."""
    path = Path(manifest_path)
    manifest = _read(path)
    if manifest.get("schema") not in _SCHEMAS:
        raise ValueError("unsupported batch manifest")
    for job in manifest.get("jobs", []):
        if job.get("state") != "submitted" or not job.get("job_id"):
            continue
        text, error = _hep_q(job["job_id"], runner)
        observed = _hep_state(text or "", job["job_id"]) if error is None else "unknown"
        job.update(scheduler_query=["hep_q", "-i", str(job["job_id"])],
                   scheduler_output=(text or error or "")[-4000:],
                   scheduler_state=observed,
                   reconciled_at=datetime.now(timezone.utc).isoformat())
        if observed == "failed":
            job.update(state="failed", recovery_eligible=True,
                       reconciliation_required=False)
        elif observed == "completed":
            job.update(state="completed", recovery_eligible=False,
                       reconciliation_required=False)
        elif observed == "active":
            job.update(recovery_eligible=False, reconciliation_required=False)
        elif observed == "unknown":
            job["reconciliation_required"] = True
    _atomic_json(path, manifest)
    return manifest


def resume_batch(manifest_path: str | Path,
                 runner: Callable[..., Any] = subprocess.run) -> Dict[str, Any]:
    """Recover only exact jobs confirmed failed; ambiguity remains blocked."""
    path = Path(manifest_path)
    manifest = status_batch(path, runner=runner)
    for job in manifest["jobs"]:
        if job.get("state") == "failed" and job.get("recovery_eligible"):
            previous = str(job.get("job_id"))
            job.update(state="prepared", recovery_of=previous, job_id=None,
                       recovery_eligible=False, reconciliation_required=False)
    _atomic_json(path, manifest)
    return submit_batch(path, runner=runner)
