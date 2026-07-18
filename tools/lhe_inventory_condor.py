#!/usr/bin/env python3
"""Prepare, run, submit, and summarize standalone LHE counting clusters."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple


SCHEMA_VERSION = 1
RESULT_NAME = "count_result.json"
MANIFEST_NAME = "run_manifest.json"
WORKLIST_NAME = "worklist.json"
SUBMIT_NAME = "count_lhe_inventory.sub"


def atomic_write_json(path: str, payload: object) -> None:
    destination = os.path.abspath(path)
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{os.path.basename(destination)}.",
        suffix=".tmp",
        dir=os.path.dirname(destination),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
        os.replace(temporary, destination)
    except Exception:
        try:
            os.remove(temporary)
        except FileNotFoundError:
            pass
        raise


def file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonicalize_lhe_path(path: str, config: Dict[str, object]) -> str:
    mount_prefix = str(config.get("cern_eos_mount_prefix", "/eos/user/"))
    xrootd_prefix = str(
        config.get("cern_eos_xrootd_prefix", "root://eosuser.cern.ch///eos/user/")
    )
    if mount_prefix and path.startswith(mount_prefix):
        return xrootd_prefix + path[len(mount_prefix):]
    return path


def count_lhe_events(path: str) -> int:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
        return sum(1 for line in handle if line.strip().startswith("<event>"))


def _load_json(path: str) -> object:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _render_submit_file(
    template_path: str,
    workdir: str,
    worker_path: str,
    proxy_path: str,
    task_count: int,
    max_materialize: int,
    requirements: str,
) -> str:
    proxy_suffix = f",{os.path.abspath(proxy_path)}" if proxy_path else ""
    proxy_name = os.path.basename(proxy_path) if proxy_path else "-"
    assignments = [
        f"work_dir = {os.path.abspath(workdir)}",
        f"worker_path = {os.path.abspath(worker_path)}",
        f"worker_name = {os.path.basename(worker_path)}",
        f"worklist_path = {os.path.join(os.path.abspath(workdir), WORKLIST_NAME)}",
        f"worklist_name = {WORKLIST_NAME}",
        f"proxy_transfer_suffix = {proxy_suffix}",
        f"proxy_name = {proxy_name}",
        f"task_count = {task_count}",
        f"max_materialize_count = {max_materialize}",
        f"count_requirements = {requirements or 'True'}",
        "",
    ]
    with open(template_path, "r", encoding="utf-8") as handle:
        return "\n".join(assignments) + handle.read()


def prepare_workspace(
    workdir: str,
    worklist: Dict[str, object],
    output_path: str,
    machine_env: Dict[str, object],
    proxy_path: str,
    max_materialize: int,
    worker_path: str,
    template_path: str,
) -> Dict[str, object]:
    destination = os.path.abspath(workdir)
    if os.path.exists(destination):
        raise FileExistsError(f"Condor inventory workspace already exists: {destination}")
    if max_materialize <= 0:
        raise ValueError("--condor-max-materialize must be positive")

    parent = os.path.dirname(destination)
    os.makedirs(parent, exist_ok=True)
    temporary = tempfile.mkdtemp(prefix=f".{os.path.basename(destination)}.", dir=parent)
    try:
        os.makedirs(os.path.join(temporary, "logs"))
        os.makedirs(os.path.join(temporary, "results"))
        worklist_path = os.path.join(temporary, WORKLIST_NAME)
        atomic_write_json(worklist_path, worklist)

        task_count = len(worklist.get("tasks", []))
        requirements = "True"
        target_machine = str(machine_env.get("target_machine", "") or "")
        if target_machine:
            escaped_machine = target_machine.replace('"', '\\"')
            requirements = f'(TARGET.Machine == "{escaped_machine}")'
        submit_text = _render_submit_file(
            template_path=template_path,
            workdir=destination,
            worker_path=worker_path,
            proxy_path=proxy_path,
            task_count=task_count,
            max_materialize=max_materialize,
            requirements=requirements,
        )
        submit_path = os.path.join(temporary, SUBMIT_NAME)
        with open(submit_path, "w", encoding="utf-8") as handle:
            handle.write(submit_text)

        manifest = OrderedDict(
            [
                ("schema_version", SCHEMA_VERSION),
                ("kind", "lhe_inventory_condor_run"),
                ("created_at", datetime.now().isoformat(timespec="seconds")),
                ("state", "prepared"),
                ("output_path", os.path.abspath(output_path)),
                ("machine_env", machine_env),
                ("proxy_path", os.path.abspath(proxy_path) if proxy_path else ""),
                ("task_count", task_count),
                ("max_materialize", max_materialize),
                ("worklist_sha256", file_sha256(worklist_path)),
                ("submit_file", SUBMIT_NAME),
                ("cluster_id", None),
                ("submit_output", ""),
            ]
        )
        atomic_write_json(os.path.join(temporary, MANIFEST_NAME), manifest)
        os.rename(temporary, destination)
        return manifest
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def load_workspace(workdir: str) -> Tuple[Dict[str, object], Dict[str, object]]:
    workdir = os.path.abspath(workdir)
    manifest_path = os.path.join(workdir, MANIFEST_NAME)
    worklist_path = os.path.join(workdir, WORKLIST_NAME)
    manifest = _load_json(manifest_path)
    worklist = _load_json(worklist_path)
    if not isinstance(manifest, dict) or manifest.get("kind") != "lhe_inventory_condor_run":
        raise ValueError(f"Invalid Condor inventory manifest: {manifest_path}")
    if not isinstance(worklist, dict) or not isinstance(worklist.get("tasks"), list):
        raise ValueError(f"Invalid Condor inventory worklist: {worklist_path}")
    if file_sha256(worklist_path) != manifest.get("worklist_sha256"):
        raise ValueError("Condor inventory worklist checksum does not match run manifest")
    if int(manifest.get("task_count", -1)) != len(worklist["tasks"]):
        raise ValueError("Condor inventory task count does not match run manifest")
    return manifest, worklist


def submit_workspace(workdir: str) -> int:
    workdir = os.path.abspath(workdir)
    manifest, worklist = load_workspace(workdir)
    if manifest.get("cluster_id") is not None:
        raise ValueError(f"Workspace already records cluster {manifest['cluster_id']}")
    if not worklist["tasks"]:
        print("No LHE files were discovered; there are no Condor jobs to submit.")
        return 0

    submit_file = os.path.join(workdir, str(manifest["submit_file"]))
    result = subprocess.run(
        ["condor_submit", "-terse", submit_file],
        cwd=workdir,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"condor_submit failed: {result.stderr.strip()}")
    match = re.search(r"(?:^|\s)(\d+)\.\d+", result.stdout)
    if not match:
        raise RuntimeError(f"Cannot parse cluster ID from condor_submit output: {result.stdout.strip()}")
    manifest["state"] = "submitted"
    manifest["cluster_id"] = int(match.group(1))
    manifest["submitted_at"] = datetime.now().isoformat(timespec="seconds")
    manifest["submit_output"] = result.stdout.strip()
    atomic_write_json(os.path.join(workdir, MANIFEST_NAME), manifest)
    print(f"Submitted LHE inventory counting cluster {manifest['cluster_id']}")
    return int(manifest["cluster_id"])


def _task_failure(task: Dict[str, object], status: str, error: str) -> Dict[str, object]:
    record = OrderedDict(
        [
            ("task_id", task.get("task_id")),
            ("path", task.get("path")),
            ("seed", task.get("seed")),
            ("status", status),
            ("error", error[:4096]),
        ]
    )
    if task.get("discovered_path") != task.get("path"):
        record["discovered_path"] = task.get("discovered_path")
    return record


def summarize_workspace(workdir: str) -> Tuple[Dict[str, object], bool]:
    workdir = os.path.abspath(workdir)
    manifest, worklist = load_workspace(workdir)
    tasks = worklist["tasks"]
    pools_source = worklist.get("pools", {})
    pools = OrderedDict()
    for pool_name, pool_info in pools_source.items():
        pools[pool_name] = OrderedDict(
            [
                ("path", pool_info.get("path", "")),
                ("discovered_count", int(pool_info.get("discovered_count", 0))),
                ("count", 0),
                ("counted_events", 0),
                ("files", []),
                ("failures", []),
            ]
        )

    successful = 0
    failed = 0
    missing = 0
    result_dir = os.path.join(workdir, "results")
    candidate_files = sorted(Path(result_dir).glob("count_*.json"))
    results_by_task: Dict[int, list] = {}
    malformed_files = []
    for result_path in candidate_files:
        try:
            raw = _load_json(str(result_path))
            if not isinstance(raw, dict):
                raise ValueError("result is not a JSON object")
            task_id = int(raw.get("task_id"))
            results_by_task.setdefault(task_id, []).append(raw)
        except Exception as exc:
            malformed_files.append(f"{result_path.name}: {exc}")

    for task in tasks:
        task_id = int(task["task_id"])
        pool_name = str(task["pool"])
        pool_payload = pools[pool_name]
        candidates = results_by_task.get(task_id, [])
        if not candidates:
            pool_payload["failures"].append(
                _task_failure(task, "missing_result", "No result fragment was found")
            )
            missing += 1
            continue
        if len(candidates) != 1:
            pool_payload["failures"].append(
                _task_failure(task, "duplicate_result", f"Found {len(candidates)} result fragments")
            )
            failed += 1
            continue
        result = candidates[0]
        identity_matches = all(
            result.get(key) == task.get(key) for key in ("pool", "path", "seed")
        )
        try:
            actual_events = int(result.get("actual_events", 0))
        except (TypeError, ValueError):
            actual_events = 0
        if result.get("status") != "ok" or actual_events <= 0 or not identity_matches:
            detail = str(result.get("error", "invalid or mismatched result"))
            if not identity_matches:
                detail = "Result identity does not match the authoritative worklist"
            pool_payload["failures"].append(_task_failure(task, "error", detail))
            failed += 1
            continue
        record = OrderedDict(
            [
                ("path", task["path"]),
                ("seed", task.get("seed")),
                ("actual_events", actual_events),
                ("status", "ok"),
            ]
        )
        if task.get("discovered_path") != task.get("path"):
            record["discovered_path"] = task.get("discovered_path")
        pool_payload["files"].append(record)
        pool_payload["count"] += 1
        pool_payload["counted_events"] += actual_events
        successful += 1

    unknown_ids = sorted(set(results_by_task) - {int(task["task_id"]) for task in tasks})
    complete = not (failed or missing or malformed_files or unknown_ids)
    payload = OrderedDict(
        [
            ("complete", complete),
            ("created_at", datetime.now().isoformat(timespec="seconds")),
            ("campaigns", worklist.get("campaigns", [])),
            ("existing_lhe_base", worklist.get("existing_lhe_base", "")),
            ("local_output_base", worklist.get("local_output_base", "")),
            (
                "counting_summary",
                OrderedDict(
                    [
                        ("mode", "condor"),
                        ("workspace", workdir),
                        ("cluster_id", manifest.get("cluster_id")),
                        ("expected", len(tasks)),
                        ("successful", successful),
                        ("failed", failed),
                        ("missing", missing),
                        ("malformed_fragments", malformed_files),
                        ("unknown_task_ids", unknown_ids),
                    ]
                ),
            ),
            ("pools", pools),
        ]
    )
    return payload, complete


def run_worker(worklist_path: str, task_index: int, proxy_name: str, output_path: str) -> int:
    try:
        worklist = _load_json(worklist_path)
        tasks = worklist["tasks"]
        task = tasks[task_index]
    except Exception as exc:
        atomic_write_json(output_path, {"schema_version": SCHEMA_VERSION, "task_id": task_index,
                                        "status": "error", "error": f"Cannot load task: {exc}"})
        return 1

    result = OrderedDict(
        [
            ("schema_version", SCHEMA_VERSION),
            ("task_id", task.get("task_id")),
            ("pool", task.get("pool")),
            ("path", task.get("path")),
            ("seed", task.get("seed")),
        ]
    )
    scratch_root = os.environ.get("_CONDOR_SCRATCH_DIR") or os.getcwd()
    try:
        input_path = str(task["path"])
        with tempfile.TemporaryDirectory(prefix="lhe_inventory_", dir=scratch_root) as temporary:
            local_path = input_path
            if input_path.startswith("root://"):
                if proxy_name == "-" or not os.path.isfile(proxy_name):
                    raise RuntimeError("Remote input requires a transferred X509 proxy")
                proxy_path = os.path.abspath(proxy_name)
                os.chmod(proxy_path, 0o600)
                env = os.environ.copy()
                env["X509_USER_PROXY"] = proxy_path
                basename = os.path.basename(input_path.rstrip("/")) or "input.lhe"
                local_path = os.path.join(temporary, basename)
                copy_result = subprocess.run(
                    ["xrdcp", "--nopbar", "--force", input_path, local_path],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=7200,
                    env=env,
                )
                if copy_result.returncode != 0:
                    raise RuntimeError(f"xrdcp failed: {copy_result.stderr.strip()[:3500]}")
            if not os.path.isfile(local_path):
                raise FileNotFoundError(f"LHE input is not visible on the execute node: {local_path}")
            actual_events = count_lhe_events(local_path)
            if actual_events <= 0:
                raise ValueError("Counted zero LHE events")
        result["actual_events"] = actual_events
        result["status"] = "ok"
        atomic_write_json(output_path, result)
        return 0
    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)[:4096]
        atomic_write_json(output_path, result)
        return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    worker = subparsers.add_parser("worker")
    worker.add_argument("worklist")
    worker.add_argument("task_index", type=int)
    worker.add_argument("proxy_name")
    worker.add_argument("output")
    args = parser.parse_args()
    if args.command == "worker":
        return run_worker(args.worklist, args.task_index, args.proxy_name, args.output)
    return 1


if __name__ == "__main__":
    sys.exit(main())
