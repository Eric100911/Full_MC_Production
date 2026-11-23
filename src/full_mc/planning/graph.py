"""Lazy SubDAGs and profile-specific transfer descriptions."""
import json
from pathlib import Path
import re

from full_mc.local_paths import bucketed, task_variables
from full_mc.runtime.profiles import STAGE_RUNTIME

CATEGORIES = {"lhe": "lhe", "plan": "lhe_planning",
              "processing": "processing", "archive": "lhe_coordination",
              "reclaim": "lhe_coordination"}


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


def write_submit_files(run_dir, core_root, runtime_dir, proxy_path, *, processing_profiles=()):
    root, core, runtime = map(Path, (run_dir, core_root, runtime_dir))
    descriptor = json.loads((runtime / "runtime.json").read_text())
    submit = root / "submit"
    submit.mkdir(parents=True, exist_ok=True)
    for directory in ("tasks", "results", "logs"):
        (root / directory).mkdir(exist_ok=True)
    task_macro = "$(task_path)" if bucketed(root) else "$(task)"
    log_macro = "$(log_path)" if bucketed(root) else "$(task)"
    bootstrap = core / "src/full_mc/runtime/bootstrap.py"
    for stage in CATEGORIES:
        profile = STAGE_RUNTIME[stage]
        if profile not in descriptor["profiles"]:
            continue
        assets = [runtime / descriptor["bundles"]["core"], runtime / "runtime.json",
                  bootstrap]
        profiles = {profile, *processing_profiles} if stage == "processing" else {profile}
        assets.extend(runtime / name for name in sorted({descriptor["bundles"][p]
                     for p in profiles if p in descriptor["bundles"]}))
        cpus = 2 if stage == "processing" else 1
        lines = [
            "universe = vanilla", "executable = /usr/bin/python3",
            "arguments = bootstrap.py --task $(task).json",
            "should_transfer_files = YES", "when_to_transfer_output = ON_EXIT",
            "transfer_executable = False",
            "transfer_input_files = " + ",".join(map(path_text, assets))
                + "," + path_text(root / "tasks") + "/" + task_macro + ".json",
            "transfer_output_files = result.json,diagnostics.tar.gz",
            'transfer_output_remaps = "result.json=' + path_text(root / "results")
                + '/' + task_macro + '.json;diagnostics.tar.gz=' + path_text(root / "logs")
                + '/' + log_macro + '.tar.gz"',
            "use_x509userproxy = True", "x509userproxy = " + path_text(proxy_path),
            "getenv = False",
            f"request_cpus = {cpus}", "request_memory = $(memory)",
            "request_disk = $(disk)",
            '+JobFlavour = "$(flavour)"',
            "output = " + path_text(root / "logs") + "/" + log_macro + ".out",
            "error = " + path_text(root / "logs") + "/" + log_macro + ".err",
            "log = " + path_text(root / "logs") + "/events.log", "queue",
        ]
        (submit / (stage + ".sub")).write_text("\n".join(lines) + "\n")
    return submit


def worker_job_lines(root, task, spec):
    """Render one worker job with the requested resources and retry policy."""
    identity = token(task["task_id"])
    stage = task["stage"]
    resource = spec.get("resources", {}).get(stage, {})
    disk = max(4_000_000_000, int(task.get("parameters", {}).get("scratch_bytes", 0)))
    requested_disk = int(resource.get("disk_bytes", disk))
    if stage == "processing" and requested_disk < task["parameters"]["scratch_bytes"]:
        raise ValueError("processing scratch exceeds its requested disk budget")
    disk = (requested_disk + 1023) // 1024
    memory_default = 3072 if stage == "lhe" else 3000 if stage == "plan" else 6000
    memory = int(resource.get("memory_mb", memory_default))
    flavour_default = (
        "espresso" if stage in {"plan", "archive", "reclaim"}
        else "workday"
    )
    flavour = token(resource.get("flavour", flavour_default))
    return [f"JOB {identity} {path_text(root / 'submit' / (stage + '.sub'))}",
              f"CATEGORY {identity} {CATEGORIES[stage]}",
              f'VARS {identity} task="{identity}" disk="{disk}" memory="{memory}" flavour="{flavour}"{task_variables(root, identity)}',
              f"RETRY {identity} 1"]
