"""Finite HELAC generation with local native output and one explicit stageout."""
from __future__ import annotations

import shutil
from ._common import core_path, count_lhe_events, parameters, prepare, publish, result, run


def build_command(task, work_dir):
    p = parameters(task)
    profile = p.get("generator_profile")
    if not isinstance(profile, dict):
        raise ValueError("HELAC task must carry its resolved generator_profile")
    nmc = int(profile.get("nmc", 0))
    unwevt = int(profile.get("unwevt", 0))
    if nmc <= 0 or unwevt <= 0:
        raise ValueError("resolved HELAC profile requires positive nmc and unwevt")
    command = ["bash", str(core_path(task, "lhe_generation/run_helac.sh")),
               "--pool", str(p["pool_name"]), "--seed", str(p["seed"]),
               "--nmc", str(nmc), "--unwevt", str(unwevt),
               "--output-dir", "lhe", "--compress-lhe",
               "--helac-diagnostics", "none", "--log-level",
               "info" if task.get("log_level") == "debug" else "slim"]
    process = profile.get("process") or "\n".join(profile.get("process_lines", []))
    if process:
        command += ["--process", process]
    cuts = profile.get("cuts", {})
    for key, flag in (("min_pt_conia", "--min-pt-conia"),
                      ("min_pt_bonia", "--min-pt-bonia"),
                      ("min_pt_q", "--min-pt-q"),
                      ("min_m_qqp", "--min-m-qqp"),
                      ("min_m_q_beam", "--min-m-q-beam")):
        if key in cuts and cuts[key] is not None:
            command += [flag, str(cuts[key])]
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
    profile = p.get("generator_profile")
    if not isinstance(profile, dict):
        raise ValueError("HELAC task must carry its resolved generator_profile")
    if events <= 0:
        raise RuntimeError("HELAC produced no LHE events")
    if (profile.get("unwevt_semantics") == "main-path-stop-target"
            and events > int(profile["unwevt"])):
        raise RuntimeError("main-path output exceeds its accepted-event stop target")
    return result(task, [publish(output, task, events=events)])
