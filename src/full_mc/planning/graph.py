"""Lazy SubDAGs and profile-specific transfer descriptions."""
import json
from pathlib import Path
import re

from full_mc.io import atomic_json
from full_mc.runtime.profiles import STAGE_RUNTIME

CATEGORIES = {"lhe": "lhe", "plan": "lhe_planning", "mix": "block_processing",
              "miniaod": "processing", "archive": "lhe_coordination"}


def token(value):
    value = str(value)
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ValueError(f"not a DAG token: {value}")
    return value


def path_text(path):
    value = str(Path(path).resolve())
    if any(character.isspace() or character in '",;' for character in value):
        raise ValueError(f"unsupported submit path: {value}")
    return value


def write_submit_files(run_dir, core_root, runtime_dir):
    root, core, runtime = map(Path, (run_dir, core_root, runtime_dir))
    descriptor = json.loads((runtime / "runtime.json").read_text())
    submit = root / "submit"
    submit.mkdir(parents=True, exist_ok=True)
    for directory in ("tasks", "results", "logs", "steps"):
        (root / directory).mkdir(exist_ok=True)
    bootstrap = core / "src/full_mc/runtime/bootstrap.py"
    for stage, category in CATEGORIES.items():
        profile = STAGE_RUNTIME[stage]
        if profile not in descriptor["profiles"]:
            continue
        assets = [runtime / descriptor["bundles"]["core"], runtime / "runtime.json",
                  bootstrap]
        if profile in descriptor["bundles"]:
            assets.append(runtime / descriptor["bundles"][profile])
        cpus = 2 if stage == "miniaod" else 1
        lines = [
            "universe = vanilla", "executable = /usr/bin/python3",
            "arguments = bootstrap.py --task $(task).json",
            "should_transfer_files = YES", "when_to_transfer_output = ON_EXIT",
            "transfer_executable = False",
            "transfer_input_files = " + ",".join(map(path_text, assets))
                + "," + path_text(root / "tasks") + "/$(task).json",
            "transfer_output_files = result.json,diagnostics.tar.gz",
            'transfer_output_remaps = "result.json=' + path_text(root / "results")
                + '/$(task).json;diagnostics.tar.gz=' + path_text(root / "logs")
                + '/$(task).tar.gz"',
            "use_x509userproxy = True", "getenv = False",
            f"request_cpus = {cpus}", "request_memory = $(memory)",
            "request_disk = $(disk)",
            '+JobFlavour = "$(flavour)"',
            "output = " + path_text(root / "logs") + "/$(task).out",
            "error = " + path_text(root / "logs") + "/$(task).err",
            "log = " + path_text(root / "logs") + "/events.log", "queue",
        ]
        (submit / (stage + ".sub")).write_text("\n".join(lines) + "\n")
    return submit


def write_tasks_dag(root, step, tasks, dependencies, spec):
    root = Path(root)
    # Steps are created by the root DAGMan PRE script, after the root DAG has
    # already been submitted. A root-level CONFIG is not inherited reliably by
    # their independently spawned DAGMan processes.
    lines = [f"CONFIG {path_text(root / 'dagman.config')}"]
    limits = spec.get("scheduler", {}).get("maxjobs", {})
    for category in sorted(set(CATEGORIES.values())):
        lines.append(f"MAXJOBS {category} {int(limits.get(category, 20))}")
    for task in tasks:
        name = token(task["task_id"])
        stage = task["stage"]
        atomic_json(root / "tasks" / (name + ".json"), task)
        resource = spec.get("resources", {}).get(stage, {})
        # Explicit byte ceilings drive scratch needs; never event-count inference.
        default_disk = max(4_000_000_000, int(task.get("parameters", {}).get("scratch_bytes", 0)))
        disk_kb = (int(resource.get("disk_bytes", default_disk)) + 1023) // 1024
        memory_mb = int(resource.get("memory_mb", 6000))
        flavour = token(resource.get("flavour", "workday"))
        lines += [f"JOB {name} {path_text(root / 'submit' / (stage + '.sub'))}",
                  f"CATEGORY {name} {CATEGORIES[stage]}",
                  f'VARS {name} task="{name}" disk="{disk_kb}" memory="{memory_mb}" flavour="{flavour}"',
                  f"RETRY {name} 1"]
    for parents, child in dependencies:
        if parents:
            lines.append("PARENT " + " ".join(map(token, parents)) + " CHILD " + token(child))
    if not tasks:
        raise ValueError("a nonterminal wave must contain worker tasks")
    path = root / "steps" / f"{step:06d}.dag"
    path.write_text("\n".join(lines) + "\n")
    return path


def write_root_dag(root, spec, steps):
    root = Path(root)
    lines = [f"CONFIG {path_text(root / 'dagman.config')}"]
    cli = path_text(Path(spec["core_root"]) / "mcprod")
    pre = root / "advance.sh"
    pre.write_text("#!/bin/bash\nset -euo pipefail\n"
                   + f"if [[ -f {path_text(root / '.complete')} ]]; then exit 99; fi\n"
                   + f'exec {cli} internal advance-wave --run-dir {path_text(root)} --step "$1"\n')
    pre.chmod(0o755)
    for index in range(steps):
        name = f"WAVE_{index:06d}"
        lines += [f"SUBDAG EXTERNAL {name} {path_text(root / 'steps' / f'{index:06d}.dag')}",
                  f"SCRIPT PRE {name} {path_text(pre)} {index}",
                  f"PRE_SKIP {name} 99"]
        if index:
            lines.append(f"PARENT WAVE_{index-1:06d} CHILD {name}")
    (root / "run.dag").write_text("\n".join(lines) + "\n")
    config = spec.get("scheduler", {})
    settings = {"DAGMAN_MAX_JOBS_SUBMITTED": int(config.get("max_jobs_submitted", 200)),
                "DAGMAN_MAX_JOBS_IDLE": int(config.get("max_jobs_idle", 100)),
                "DAGMAN_MAX_SUBMITS_PER_INTERVAL": int(config.get("max_submits_per_interval", 20)),
                "DAGMAN_SUBMIT_DELAY": int(config.get("submit_delay", 0))}
    (root / "dagman.config").write_text("".join(f"{k} = {v}\n" for k, v in settings.items()))
    return root / "run.dag"
