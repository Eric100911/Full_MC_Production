"""Finite, independent source and campaign SubDAGs for shared staging runs."""
from pathlib import Path
import shlex
import os
import tempfile

from full_mc.io import write_immutable_json
from full_mc.planning.graph import CATEGORIES, path_text, token

DEFER = 75
SKIP = 99


def _atomic_text(path, text):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def source_order(spec):
    pools = {}
    for index, source in enumerate(spec["sources"]):
        pools.setdefault(source["pool"], []).append(index)
    return [values[offset] for offset in range(max(map(len, pools.values()), default=0))
            for values in pools.values() if offset < len(values)]


def source_name(index):
    return f"SOURCE_{int(index):06d}"


def shard_name(campaign, index):
    return f"SHARD_{token(campaign)}_{int(index):06d}"


def cleanup_name(pool, slot):
    return f"CLEANUP_{token(pool)}_{int(slot):06d}"


def child_path(root, name):
    return Path(root) / "subdags" / (token(name) + ".dag")


def shard_ranges(spec):
    width = int(spec.get("limits", {}).get("shard_nodes", 10))
    if width <= 0:
        raise ValueError("limits.shard_nodes must be positive")
    return [(c["name"], i, start, min(width, int(c["max_processing_nodes"]) - start))
            for c in spec["campaigns"]
            for i, start in enumerate(range(0, int(c["max_processing_nodes"]), width))]


def _script(root, phase, name, arguments, post=False):
    command = "SCRIPT DEFER 75 60 " + ("POST" if post else "PRE")
    return f"{command} {name} {path_text(Path(root) / 'controller.sh')} {phase} {arguments}"


def write_config(root, spec):
    options = spec.get("scheduler", {})
    settings = {
        "DAGMAN_MAX_JOBS_SUBMITTED": options.get("max_jobs_submitted", 200),
        "DAGMAN_MAX_JOBS_IDLE": options.get("max_jobs_idle", 100),
        "DAGMAN_MAX_SUBMITS_PER_INTERVAL": options.get("max_submits_per_interval", 20),
        "DAGMAN_SUBMIT_DELAY": options.get("submit_delay", 0),
        "DAGMAN_MAX_PRE_SCRIPTS": options.get("max_pre_scripts", 20),
        "DAGMAN_MAX_POST_SCRIPTS": options.get("max_post_scripts", 20),
        "DAGMAN_GENERATE_SUBDAG_SUBMITS": True,
        "DAGMAN_AUTO_RESCUE": True,
        "DAGMAN_ALWAYS_RUN_POST": False,
    }
    path = Path(root) / "dagman.config"
    path.write_text("".join(f"{key} = {str(value).upper() if isinstance(value, bool) else int(value)}\n"
                            for key, value in settings.items()))


def write_root_dag(root, spec):
    root = Path(root)
    (root / "subdags").mkdir(exist_ok=True)
    (root / "logs").mkdir(exist_ok=True)
    write_config(root, spec)
    cli = shlex.quote(path_text(Path(spec["core_root"]) / "mcprod"))
    launcher = root / "controller.sh"
    launcher.write_text(
        "#!/bin/bash\nset -euo pipefail\nexport PATH=/usr/bin:/bin\n"
        'phase="${1:?missing controller phase}"\nshift\n'
        + f"exec >>{shlex.quote(path_text(root / 'logs' / 'controller.log'))} 2>&1\n"
        + f'exec {cli} internal "$phase" --run-dir {shlex.quote(path_text(root))} "$@"\n')
    launcher.chmod(0o755)
    lines = [f"CONFIG {path_text(root / 'dagman.config')}",
             "MAXJOBS lhe_planning 2", "MAXJOBS block_processing 6",
             "MAXJOBS lhe_coordination 20"]
    pool_parents = {s["pool"]: [] for s in spec["sources"]}
    work = []
    for index in source_order(spec):
        source = spec["sources"][index]
        name = source_name(index)
        work.append(name)
        pool_parents[source["pool"]].append(name)
        args = f"--source-index {index}"
        lines += [f"SUBDAG EXTERNAL {name} {path_text(child_path(root, name))}",
                  f"CATEGORY {name} lhe_planning",
                  _script(root, "prepare-source", name, args), f"PRE_SKIP {name} 99",
                  _script(root, "complete-source", name, args + " --return-code $RETURN", True)]
    campaigns = {c["name"]: c for c in spec["campaigns"]}
    for campaign, index, _, _ in shard_ranges(spec):
        name = shard_name(campaign, index)
        work.append(name)
        for pool in {s["pool"] for s in campaigns[campaign]["sources"]}:
            pool_parents[pool].append(name)
        args = f"--campaign {campaign} --shard-index {index}"
        lines += [f"SUBDAG EXTERNAL {name} {path_text(child_path(root, name))}",
                  f"CATEGORY {name} block_processing",
                  _script(root, "prepare-shard", name, args), f"PRE_SKIP {name} 99",
                  _script(root, "complete-shard", name, args + " --return-code $RETURN", True)]
    def cleanup_job(name, pool, slot):
        resource = spec.get("resources", {}).get("reclaim", {})
        disk = (int(resource.get("disk_bytes", 4_000_000_000)) + 1023) // 1024
        memory = int(resource.get("memory_mb", 6000))
        flavour = token(resource.get("flavour", "workday"))
        return [f"JOB {name} {path_text(root / 'submit' / 'reclaim.sub')}",
                f"CATEGORY {name} lhe_coordination",
                f'VARS {name} task="{name}" disk="{disk}" memory="{memory}" flavour="{flavour}"',
                f"RETRY {name} 1",
                _script(root, "prepare-cleanup", name, f"--pool {pool} --slot {slot}"),
                f"PRE_SKIP {name} 99",
                _script(root, "accept-result", name, f"--task-id {name} --return-code $RETURN", True)]

    final_cleanups = []
    for pool, parents in sorted(pool_parents.items()):
        earlier = []
        for slot, parent in enumerate(parents):
            name = cleanup_name(pool, slot)
            earlier.append(name)
            lines += cleanup_job(name, pool, slot)
            lines.append(f"PARENT {parent} CHILD {name}")
        name = cleanup_name(pool, len(parents))
        final_cleanups.append(name)
        lines += cleanup_job(name, pool, len(parents))
        if work or earlier:
            lines.append("PARENT " + " ".join(work + earlier) + " CHILD " + name)
    (root / "submit" / "noop.sub").write_text("universe = local\nexecutable = /bin/true\nqueue\n")
    lines += [f"JOB FINALIZE {path_text(root / 'submit' / 'noop.sub')} NOOP",
              _script(root, "finalize-shards", "FINALIZE", "")]
    if final_cleanups:
        lines.append("PARENT " + " ".join(final_cleanups) + " CHILD FINALIZE")
    path = root / "run.dag"
    _atomic_text(path, "\n".join(lines) + "\n")
    return path


def write_child_dag(root, name, tasks, dependencies, spec, claims=()):
    """Render after allocation commit; retries repair missing DAG/task files."""
    root = Path(root)
    lines = [f"CONFIG {path_text(root / 'dagman.config')}"]
    limits = spec.get("scheduler", {}).get("maxjobs", {})
    for category in sorted(set(CATEGORIES.values())):
        lines.append(f"MAXJOBS {category} {int(limits.get(category, 20))}")
    for task in tasks:
        identity = token(task["task_id"])
        stage = task["stage"]
        if not task.get("parameters", {}).get("source_plan_task"):
            write_immutable_json(root / "tasks" / (identity + ".json"), task)
        resource = spec.get("resources", {}).get(stage, {})
        disk = max(4_000_000_000, int(task.get("parameters", {}).get("scratch_bytes", 0)))
        disk = (int(resource.get("disk_bytes", disk)) + 1023) // 1024
        memory = int(resource.get("memory_mb", 3072 if stage == "lhe" else 6000))
        flavour = token(resource.get("flavour", "workday"))
        lines += [f"JOB {identity} {path_text(root / 'submit' / (stage + '.sub'))}",
                  f"CATEGORY {identity} {CATEGORIES[stage]}",
                  f'VARS {identity} task="{identity}" disk="{disk}" memory="{memory}" flavour="{flavour}"',
                  f"RETRY {identity} 1"]
        if identity in claims:
            lines.append(_script(root, "admit-producer", identity, f"--task-id {identity}"))
        lines.append(_script(root, "accept-result", identity,
                             f"--task-id {identity} --return-code $RETURN", True))
    for parents, child in dependencies:
        if parents:
            lines.append("PARENT " + " ".join(map(token, parents)) + " CHILD " + token(child))
    if not tasks:
        raise ValueError("a prepared child must contain worker tasks")
    path = child_path(root, name)
    path.parent.mkdir(exist_ok=True)
    _atomic_text(path, "\n".join(lines) + "\n")
    return path
