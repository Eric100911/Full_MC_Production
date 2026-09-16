"""Finite HELAC generation with local native output and one explicit stageout."""
from __future__ import annotations

import shutil
from ._common import core_path, count_lhe_events, parameters, prepare, publish, result, run


def build_command(task, work_dir):
    p = parameters(task)
    maximum = int(p["max_events"])
    if maximum <= 0:
        raise ValueError("HELAC max_events must be positive and finite")
    command = ["bash", str(core_path(task, "lhe_generation/run_helac.sh")),
               "--pool", str(p["pool_name"]), "--seed", str(p["seed"]),
               "--unwevt", str(maximum),
               "--output-dir", "lhe", "--compress-lhe",
               "--helac-diagnostics", "none", "--log-level",
               "info" if task.get("log_level") == "debug" else "slim"]
    process = p.get("process") or "\n".join(p.get("process_lines", []))
    if process:
        command += ["--process", process]
    for key, flag in (
        ("charm_state", "--charm-state"), ("bottom_state", "--bottom-state"),
        ("extra_gluon", "--extra-gluon"), ("min_pt_conia", "--min-pt-conia"),
        ("min_pt_bonia", "--min-pt-bonia"), ("min_pt_q", "--min-pt-q"),
        ("min_m_qqp", "--min-m-qqp"), ("min_m_q_beam", "--min-m-q-beam"),
        ("nmc", "--nmc")):
        if key in p and p[key] is not None:
            value = str(p[key]).lower() if isinstance(p[key], bool) else str(p[key])
            command += [flag, value]
    return command


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "helac-el7")
    source = core_path(task, "common/packages/helac_package.tar.gz")
    shutil.copy2(source, work_dir / "helac_package.tar.gz")
    run(task, "helac-el7", build_command(task, work_dir), work_dir,
        {"LOCAL_OUTPUT_BASE": str(work_dir / "products"), "SKIP_STAGEOUT": "1"})
    p = parameters(task)
    output = work_dir / "products/lhe" / f"sample_{p['pool_name']}_{p['seed']}.lhe.gz"
    events = count_lhe_events(output)
    if not 0 < events <= int(p["max_events"]):
        raise RuntimeError("HELAC output violates its finite event cap")
    return result(task, [publish(output, task, events=events)])
