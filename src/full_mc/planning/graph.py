"""Condor descriptions for the root DAG and the flat processing DAG."""
import base64
import json
from pathlib import Path
import re
import zlib

from full_mc.credentials import requires_proxy
from full_mc.local_paths import FILES_PER_BUCKET, LOG_FILES_PER_TASK, identity_bucket
from full_mc.runtime.profiles import STAGE_RUNTIME

PROC_DIR = "proc"
PROC_DAG = "processing.dag"
PLAN_RECORD = "plan.json"
RESULTS_BUNDLE = "results_bundle.tar.gz"
CATEGORIES = {"source": "lhe", "plan": "lhe_planning",
              "processing": "processing", "reclaim": "lhe_coordination"}


def token(value):
    value = str(value)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value):
        raise ValueError(f"not a DAG token: {value}")
    return value


def path_text(path):
    value = str(path)
    if any(char.isspace() or char in '",;\\$' for char in value):
        raise ValueError(f"unsupported submit path: {value}")
    return value


def node_stem(identity, width=FILES_PER_BUCKET):
    if identity in {"PLAN", "RECLAIM"}:
        return identity
    proc = re.fullmatch(r"(PROC_.+)_JOB[0-9]+_BLOCK([0-9]+)", identity)
    indexed = f"{proc[1]}_{proc[2]}" if proc else identity
    return str(identity_bucket(indexed, width) / identity)


def log_stem(identity):
    return node_stem(identity, FILES_PER_BUCKET // LOG_FILES_PER_TASK)


def _task_payload(task):
    encoded = json.dumps(task, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(zlib.compress(encoded, level=1)).decode("ascii")


def stage_requires_proxy(spec, stage):
    resource = "lhe" if stage == "source" else stage
    return requires_proxy(spec.get("resources", {}).get(resource, {}))


def worker_submit(root, core, runtime, descriptor, proxy, stage, extra_profiles=()):
    root, core, runtime = map(Path, (root, core, runtime))
    profiles = {STAGE_RUNTIME[stage], *extra_profiles}
    assets = [runtime / descriptor["bundles"]["core"], runtime / "runtime.json",
              core / "src/full_mc/runtime/bootstrap.py"]
    assets += [runtime / name for name in sorted({descriptor["bundles"][p]
               for p in profiles if p in descriptor["bundles"]})]
    base = root / PROC_DIR if stage == "processing" else root
    inputs = assets
    outputs = ["result.json", "diagnostics.tar.gz"]
    remaps = [f"result.json={path_text(base / 'results')}/$(stem).json",
              f"diagnostics.tar.gz={path_text(base / 'logs')}/$(log_stem).tar.gz"]
    extra_args = " --publication-id $(Cluster).$(Process)"
    if stage == "plan":
        outputs.append(PROC_DIR)
        extra_args += f" --products {PROC_DIR}"
    if stage == "reclaim":
        inputs.extend((root / PROC_DIR / PLAN_RECORD, root / PROC_DIR / RESULTS_BUNDLE))
        for filename in ("handoff.json", "merge_groups.json"):
            outputs.append(filename)
            remaps.append(f"{filename}={path_text(root)}/{filename}")
        extra_args += " --products handoff.json"
    transfer_inputs = ",".join(map(path_text, inputs))
    if stage == "processing":
        task_arg = "--task-payload $(payload)"
    else:
        task_arg = "--task $(task).json"
        transfer_inputs += f",{path_text(base / 'tasks')}/$(stem).json"
    lines = [
        "universe = vanilla", "executable = /usr/bin/python3",
        f"initialdir = {path_text(root)}",
        f"arguments = bootstrap.py {task_arg}{extra_args}",
        "should_transfer_files = YES", "when_to_transfer_output = ON_EXIT",
        "transfer_executable = False", f"transfer_input_files = {transfer_inputs}",
        "transfer_output_files = " + ",".join(outputs),
        'transfer_output_remaps = "' + ";".join(remaps) + '"',
        "use_x509userproxy = False" if proxy is None else "use_x509userproxy = True",
        *([] if proxy is None else ["x509userproxy = " + path_text(proxy)]),
        "getenv = False", "request_cpus = $(cpus)", "request_memory = $(memory)",
        "request_disk = $(disk)", '+JobFlavour = "$(flavour)"',
        f"output = {path_text(base / 'logs')}/$(log_stem).out",
        f"error = {path_text(base / 'logs')}/$(log_stem).err",
        f"log = {path_text(base / 'logs' / 'events.log')}", "queue",
    ]
    return "\n".join(lines) + "\n"


def worker_job_lines(root, task, spec):
    identity = token(task["task_id"])
    stage = task["stage"]
    category = "lhe" if stage == "lhe" else CATEGORIES[stage]
    resource = spec["resources"].get("lhe" if stage == "source" else stage, {})
    disk = (resource.get("disk_bytes", task["parameters"].get("scratch_bytes", 1_000_000_000))
            + 1023) // 1024
    cpus = resource.get("cpus", 1)
    memory = resource.get("memory_mb", 3072 if stage == "source" else
                          6000 if stage == "processing" else 3000)
    flavour = token(resource.get("flavour", "workday"))
    submit = root / (PROC_DIR if stage == "processing" else "submit") / f"{stage}.sub"
    variables = (f'task="{identity}" stem="{node_stem(identity)}" log_stem="{log_stem(identity)}" '
                 f'cpus="{cpus}" disk="{disk}" memory="{memory}" flavour="{flavour}"')
    if stage == "processing":
        variables += f' payload="{_task_payload(task)}"'
    return [
        f"JOB {identity} {path_text(submit)}",
        f"CATEGORY {identity} {category}",
        f"VARS {identity} {variables}",
        f"RETRY {identity} 1",
    ]


def write_dagman_config(root, scheduler):
    options = {
        "DAGMAN_MAX_JOBS_SUBMITTED": 0, "DAGMAN_MAX_JOBS_IDLE": 0,
        "DAGMAN_MAX_SUBMITS_PER_INTERVAL": 20, "DAGMAN_SUBMIT_DELAY": 0,
    }
    for key in options:
        options[key] = scheduler.get(key.removeprefix("DAGMAN_").lower(), options[key])
    options.update(DAGMAN_GENERATE_SUBDAG_SUBMITS="TRUE", DAGMAN_AUTO_RESCUE="TRUE")
    (root / "dagman.config").write_text("".join(f"{key} = {value}\n" for key, value in options.items()))


def root_dag(root, spec, sources, plan, reclaim):
    lines = [f"CONFIG {path_text(root / 'dagman.config')}"]
    for task in (*sources, plan, reclaim):
        lines.extend(worker_job_lines(root, task, spec))
    lines.append(f"SUBDAG EXTERNAL PROC_SUBDAG {path_text(root / PROC_DIR / PROC_DAG)}")
    # DAGMan limits the length of an individual line. Each dependency can be
    # stated independently, and PLAN still waits for all SOURCE jobs.
    lines.extend(f"PARENT {task['task_id']} CHILD PLAN" for task in sources)
    lines.extend(("PARENT PLAN CHILD PROC_SUBDAG", "PARENT PROC_SUBDAG CHILD RECLAIM"))
    python_path = path_text(Path(spec["core_root"]) / "src")
    proc_dir = path_text(root / PROC_DIR)
    lines.append(f"SCRIPT PRE RECLAIM /usr/bin/env PYTHONPATH={python_path} "
                 f"/usr/bin/python3 -m full_mc.runtime.reclaim_bundle {proc_dir}")
    return "\n".join(lines) + "\n"
