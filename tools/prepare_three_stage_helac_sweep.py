#!/usr/bin/env python3
"""Prepare and summarize the finite stage-one HELAC sweep, without submission."""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import re
import sys
import tarfile

CORE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORE / "src"))
sys.path.insert(0, str(CORE))

from full_mc.domain.models import Artifact, task_dict
from full_mc.io import atomic_json, read_json
from full_mc.local_paths import LAYOUT_MARKER, log_path, record_path
from full_mc.planning.graph import path_text, token, write_submit_files, write_tasks_dag
from full_mc.profiles import ProfileCatalog, resolve_source
from full_mc.runtime.packages import validate_runtime_release
from full_mc.workers._common import count_lhe_events
from tools.helac_inspector import FINAL_RE, fortran_float


# Each row is a cell with two independently seeded source jobs.
CELLS = {
    "A": ("pool_2jpsi_g", 1_000_000, 100_000, [93000, 93001]),
    "B": ("pool_2jpsi_g", 1_000_000, 10_000_000, [93002, 93003]),
    "C": ("pool_2jpsi_g", 2_000_000, 100_000, [93004, 93005]),
    "D": ("pool_2jpsi_g", 2_000_000, 10_000_000, [93006, 93007]),
    "gg": ("pool_gg", 500_000, 10_000_000, [95000, 95001]),
}
SCHEDULER = {
    "max_jobs_submitted": "DAGMAN_MAX_JOBS_SUBMITTED",
    "max_jobs_idle": "DAGMAN_MAX_JOBS_IDLE",
    "max_submits_per_interval": "DAGMAN_MAX_SUBMITS_PER_INTERVAL",
    "submit_delay": "DAGMAN_SUBMIT_DELAY",
    "max_pre_scripts": "DAGMAN_MAX_PRE_SCRIPTS",
    "max_post_scripts": "DAGMAN_MAX_POST_SCRIPTS",
}
METRICS = ("cpu_seconds", "wall_seconds", "sigma_nb", "sigma_error_nb",
           "max_weight_warnings", "stop_reason")


def _integer(value, name, minimum=1):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _validate_profile(profile, cell):
    pool, nmc, unwevt, _ = CELLS[cell]
    for key, value in (("nmc", nmc), ("unwevt", unwevt)):
        if _integer(profile.get(key), key) != value:
            raise ValueError(f"cell {cell} requires {key}={value}")
    process = ("generate g g > g g" if cell == "gg"
               else "generate g g > cc~(3S11) cc~(3S11) g")
    if (profile.get("generator_path") != "main"
            or profile.get("unwevt_semantics") != "main-path-stop-target"
            or profile.get("process") != process):
        raise ValueError(f"cell {cell} requires its main-path process and stop target")
    values = (0, 0, 4, 0, 0) if pool == "pool_gg" else (6, 4, 0, 3.0969, 3.0969)
    keys = ("min_pt_conia", "min_pt_bonia", "min_pt_q", "min_m_qqp", "min_m_q_beam")
    if any(profile.get("cuts", {}).get(k) != v for k, v in zip(keys, values)):
        raise ValueError(f"cell {cell} has incorrect cuts")


def plan(spec_path, output_dir):
    spec = deepcopy(read_json(spec_path))
    required = {"schema", "run_id", "core_revision", "runtime_release", "runtime_dir",
                "schedd", "proxy_path", "output_root", "site_proof_path", "resources",
                "scheduler", "cells"}
    if required - spec.keys():
        raise ValueError("missing spec fields: " + ", ".join(sorted(required - spec.keys())))
    if spec["schema"] != "three-stage-helac-sweep-v1":
        raise ValueError("expected three-stage-helac-sweep-v1")
    run_id = token(spec["run_id"])
    if not run_id.endswith("_s1") or not run_id[:-3]:
        raise ValueError("run_id must be <experiment_id>_s1")
    if not re.fullmatch(r"[0-9a-f]{40}", spec["core_revision"]):
        raise ValueError("core_revision must be a full Git SHA")
    token(spec["runtime_release"])
    token(spec["schedd"])
    for key in ("runtime_dir", "proxy_path", "site_proof_path"):
        if not Path(spec[key]).is_absolute():
            raise ValueError(f"{key} must be absolute")
        spec[key] = path_text(spec[key])
    proof = read_json(spec["site_proof_path"])
    expected_checks = {"mutual_exclusion", "read_after_unlock", "crash_recovery",
                       "pre_defer", "pre_skip", "dynamic_subdag", "autorescue"}
    if (proof.get("schema") != "cern-afs-lock-proof-v1"
            or not {"bigbird11.cern.ch", "bigbird22.cern.ch"} <= set(proof.get("schedds", []))
            or any(proof.get("checks", {}).get(name) is not True for name in expected_checks)):
        raise ValueError("site proof does not establish every required CERN AFS lock check")
    output_root = spec["output_root"].rstrip("/")
    if not output_root.startswith("root://cceos.ihep.ac.cn:1094///store/"):
        raise ValueError("output_root requires the explicit IHEP triple-slash URL")
    if run_id[:-3] not in output_root.split("/"):
        raise ValueError("output_root must contain the experiment_id directory")
    resource = spec["resources"]["lhe"]
    if _integer(resource.get("cpus"), "lhe.cpus") != 1:
        raise ValueError("the existing HELAC submit format requires one CPU")
    for key in ("memory_mb", "disk_bytes"):
        _integer(resource.get(key), "lhe." + key)
    token(resource["flavour"])
    for key in SCHEDULER:
        _integer(spec["scheduler"].get(key), key, minimum=0 if key == "submit_delay" else 1)
    _integer(spec["scheduler"]["maxjobs"].get("lhe"), "maxjobs.lhe")
    runtime = validate_runtime_release(spec["runtime_dir"],
                                       expected_release=spec["runtime_release"],
                                       expected_site="cern")
    if runtime.get("source_dirty") is not False:
        raise ValueError("sweep requires a clean runtime (source_dirty=false)")
    if runtime["git_revision"] != spec["core_revision"]:
        raise ValueError("core_revision does not match runtime")
    if "helac-el7" not in runtime["profiles"] or "helac-el7" not in runtime["bundles"]:
        raise ValueError("runtime lacks the HELAC profile/bundle")
    controller = Path(spec["runtime_dir"]) / "controller/core"
    # Resolve the catalogue shipped with the pinned runtime, not the live checkout.
    catalogue = ProfileCatalog("generator", read_json(
        controller / "common/profiles/generator_profiles.json"))
    cells = spec["cells"]
    if (not isinstance(cells, list) or len(cells) != 5
            or {c["cell"] for c in cells} != set(CELLS)):
        raise ValueError("sweep requires exactly cells A, B, C, D, gg")
    by_cell = {c["cell"]: c for c in cells}
    tasks, entries, sources = [], [], []
    for cell, (pool, _, _, seeds) in CELLS.items():
        row = by_cell[cell]
        if row["pool"] != pool or row["seeds"] != seeds:
            raise ValueError(f"cell {cell} requires pool {pool} and seeds {seeds}")
        for replica, seed in enumerate(seeds, 1):
            source_id = f"{run_id[:-3]}_s1_{cell}_seed{seed}"
            source = resolve_source(dict(pool=pool, source_id=source_id, seed=seed,
                                         generator_profile=row["profile_id"]), catalogue)
            profile = source["resolved_generator_profile"]
            _validate_profile(profile, cell)
            identity = f"LHE_{len(tasks):06d}"
            url = f"{output_root}/{pool}/{source_id}/source.lhe.gz"
            task = task_dict(identity, "lhe", "helac-el7", [],
                             [Artifact(identity, url, "lhe")],
                             {"pool_name": pool, "source_id": source_id, "seed": seed,
                              "generator_profile": profile,
                              "retain_helac_diagnostics": True}, "info", spec["runtime_release"])
            tasks.append(task)
            sources.append(source)
            entries.append(dict(cell=cell, replica=replica, task_id=identity, pool=pool,
                                source_id=source_id, seed=seed, profile_id=row["profile_id"],
                                generator_profile=profile, url=url))
    root = Path(path_text(output_dir))
    if root.exists() and any(root.iterdir()):
        raise FileExistsError("run directory is not empty; reuse it for recovery")
    root.mkdir(parents=True, exist_ok=True)
    (root / LAYOUT_MARKER).touch()
    spec.update(core_root=str(controller), runtime_source_dirty=False, sources=sources)
    atomic_json(root / "run.json", spec)
    atomic_json(root / "sweep-manifest.json", {
        "schema": "three-stage-helac-sweep-manifest-v1", "run_id": run_id,
        "runtime": runtime, "entries": entries,
        "physics": {"beam_energy_gev_each": 6800, "effective_scale": 0,
                    "mu_r_gev": 12.8, "mu_f_gev": 12.8, "scale_factor": 1,
                    "muR_over_ref": 1, "muF_over_ref": 1,
                    "pdf": 10000, "pdf_name": "cteq6m", "lhapdf": "F",
                    "cmass_gev": 1.54845, "charm_pair_mass_gev": 3.0969,
                    "min_m_qqp": 3.0969, "min_m_q_beam": 3.0969,
                    "other_parameters": "HELAC-Onia 2.7.6 main template"},
        "assumptions": {"min_m_qqp": 3.0969, "min_m_q_beam": 3.0969,
                        "planning_events_hint_is_capacity": False,
                        "nmc_is_per_iteration": True, "detector_events": 0}})
    write_submit_files(root, controller, spec["runtime_dir"], spec["proxy_path"])
    settings = spec["scheduler"]
    (root / "dagman.config").write_text("".join(
        f"{name} = {settings[key]}\n" for key, name in SCHEDULER.items()))
    dag = write_tasks_dag(root, 0, tasks, [], spec)
    dag.replace(root / "run.dag")
    return {"run_dir": str(root), "dag": str(root / "run.dag"),
            "source_files": len(tasks), "submitted": False}


def _diagnostics(path):
    """Read compact returned evidence in place; never extract worker archives."""
    values, evidence, warnings, timings = {}, [], [], []
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            if not member.isfile():
                continue
            name = Path(member.name).name
            if not name.endswith((".log", ".out", ".stdout", ".stderr")):
                continue
            handle = archive.extractfile(member)
            if name.endswith(".timing.log"):
                timing = dict(line.decode().strip().split("=", 1) for line in handle if b"=" in line)
                if all(k in timing for k in ("wall_seconds", "cpu_user_seconds", "cpu_system_seconds")):
                    timings.append(timing)
                if "helac_ho_cluster" in name and timing.get("exit_code") not in (None, "0"):
                    values["stop_reason"] = "helac_ho_cluster exit_code=" + timing["exit_code"]
                evidence.append(f"{path}:{member.name}")
                continue
            for raw in handle:
                line = raw.decode("utf-8", errors="replace").strip()
                match = FINAL_RE.search(line)
                if match:
                    values.update(sigma_nb=fortran_float(match.group("sigma")),
                                  sigma_error_nb=fortran_float(match.group("error")))
                    evidence.append(f"{path}:{member.name}")
                if (re.search(r"max(?:imum)?.?weight", line, re.I)
                        and re.search(r"warn|exceed|overweight", line, re.I)):
                    warnings.append(line)
                    evidence.append(f"{path}:{member.name}")
    if warnings:
        values["max_weight_warnings"] = warnings
    if timings:
        values["wall_seconds"] = sum(float(t["wall_seconds"]) for t in timings)
        values["cpu_seconds"] = sum(float(t["cpu_user_seconds"]) + float(t["cpu_system_seconds"])
                                    for t in timings)
    return values, sorted(set(evidence))


def _count_local(root, entry, ref):
    """Reuse the worker's exact count, or cache one gzip-aware local count."""
    if ref.get("events") is not None:
        return _integer(ref["events"], "output.events", 0), ref.get("size_bytes"), None
    path = Path(ref.get("local_path", ""))
    if not path.is_absolute() or not path.is_file():
        return None, ref.get("size_bytes"), None
    stat = path.stat()
    signature = dict(path=str(path), size_bytes=stat.st_size, mtime_ns=stat.st_mtime_ns)
    receipt_path = record_path(root, "counts", entry["task_id"])
    receipt = read_json(receipt_path) if receipt_path.exists() else {}
    if receipt.get("file") != signature:
        receipt = dict(file=signature, actual_events=count_lhe_events(path),
                       count_source="full-mc-gzip-aware-counter")
        atomic_json(receipt_path, receipt)
    return receipt["actual_events"], stat.st_size, str(receipt_path)


def summarize(run_dir, output):
    root = Path(run_dir).resolve()
    spec = read_json(root / "run.json")
    manifest = read_json(root / "sweep-manifest.json")
    rows = []
    for entry in manifest["entries"]:
        row = {key: entry[key] for key in (
            "cell", "replica", "task_id", "pool", "source_id", "seed", "profile_id", "url")}
        row.update({key: None for key in METRICS})
        row.update(status="missing", actual_events=None, gzip_bytes=None,
                   events_per_cpu_hour=None, reached_cap=None,
                   evidence_paths=[], missing_reasons={})
        result_path = record_path(root, "results", entry["task_id"])
        try:
            if result_path.exists():
                result = read_json(result_path)
                if (result.get("task_id") != entry["task_id"] or result.get("stage") != "lhe"
                        or result.get("runtime_release", spec["runtime_release"])
                        != spec["runtime_release"]):
                    raise ValueError("result identity does not match frozen task")
                row["status"] = result.get("status", "unknown")
                row["evidence_paths"].append(str(result_path))
                row.update({key: result[key] for key in METRICS if key in result})
                refs = [ref for ref in result.get("outputs", [])
                        if ref.get("artifact_id") == entry["task_id"]
                        and ref.get("url") == entry["url"] and ref.get("kind") == "lhe"]
                if len(refs) == 1:
                    events, size, receipt = _count_local(root, entry, refs[0])
                    row.update(actual_events=events, gzip_bytes=size)
                    if receipt:
                        row["evidence_paths"].append(receipt)
                if result.get("error"):
                    row["result_error"] = result["error"]
        except (OSError, ValueError, TypeError) as exc:
            row["status"] = "invalid"
            row["result_error"] = str(exc)
        diagnostics = log_path(root, entry["task_id"], ".tar.gz")
        if diagnostics.exists():
            row["evidence_paths"].append(str(diagnostics))
            try:
                values, paths = _diagnostics(diagnostics)
                row.update({k: v for k, v in values.items() if row[k] is None})
                if "cpu_seconds" in values:
                    row["timing_scope"] = "sum of timed run_helac.sh commands; excludes wrapper overhead"
                row["evidence_paths"].extend(paths)
            except (OSError, ValueError, tarfile.TarError) as exc:
                row["diagnostics_error"] = str(exc)
        if row["actual_events"] is not None:
            row["reached_cap"] = row["actual_events"] == entry["generator_profile"]["unwevt"]
            if row["reached_cap"] and row["stop_reason"] is None:
                row["stop_reason"] = "accepted-event-cap (count equals frozen unwevt)"
            if row["cpu_seconds"] is not None and row["cpu_seconds"] > 0:
                row["events_per_cpu_hour"] = row["actual_events"] * 3600 / row["cpu_seconds"]
        for key, value in row.items():
            if value is None:
                row["missing_reasons"][key] = "not recorded in available result/diagnostics evidence"
        rows.append(row)
    payload = {"schema": "three-stage-helac-sweep-summary-v1", "run_id": spec["run_id"],
               "core_revision": spec["core_revision"], "runtime_release": spec["runtime_release"],
               "all_results_present": all(r["status"] in {"ok", "failed"} for r in rows),
               "all_sources_counted": all(r["actual_events"] is not None for r in rows),
               "entries": rows, "manifest_path": str(root / "sweep-manifest.json")}
    atomic_json(output, payload)
    return payload


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("plan")
    command.add_argument("--spec", required=True, type=Path)
    command.add_argument("--output-dir", required=True, type=Path)
    command = commands.add_parser("summarize")
    command.add_argument("--run", required=True, type=Path)
    command.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            result = plan(args.spec, args.output_dir)
        else:
            payload = summarize(args.run, args.output)
            result = {"output": str(args.output), "all_results_present": payload["all_results_present"]}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
