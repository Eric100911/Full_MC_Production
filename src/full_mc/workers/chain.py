"""Narrow stage adapters around the maintained native CMSSW/shower commands."""
from __future__ import annotations

import gzip
from pathlib import Path
import shutil

from full_mc.io import read_json, atomic_json
from full_mc.storage.transfer import materialize_input
from ._common import core_path, parameters, run


def native_tree(task, work_dir, shower=False):
    """Only mutable build inputs go into scratch; never compile in the release."""
    root = work_dir / "native"
    processing = root / "processing"
    processing.mkdir(parents=True, exist_ok=True)
    shutil.copy2(core_path(task, "processing/run_chain.sh"), processing / "run_chain.sh")
    shutil.copytree(core_path(task, "processing/cmssw_helpers"),
                    processing / "cmssw_helpers", dirs_exist_ok=True)
    if shower:
        shutil.copytree(core_path(task, "processing/pythia_shower"),
                        processing / "pythia_shower", dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns("*.o", "__pycache__"))
    if not (root / "common").exists():
        (root / "common").symlink_to(core_path(task, "common"), target_is_directory=True)
    return processing / "run_chain.sh"


def build_command(task, work_dir, script, config):
    p = parameters(task)
    stage = task["stage"]
    command = ["bash", str(script), "--config", str(config),
               "--inputs", "file:staged", "--modes", "normal",
               "--analysis", str(p["analysis"]), "--campaign", str(p["campaign"]),
               "--job-id", str(p.get("job_id", task["task_id"])),
               "--workdir", str(work_dir), "--cleanup", "false",
               "--max-events", str(p.get("target_mixed_events", -1)),
               "--enable-ntuple", "true" if stage == "ntuple" else "false",
               "--efficiency-ntuple", str(stage == "ntuple" and p["analysis"] == "JJP").lower()]
    if stage == "mix":
        command += ["--stop-at", "mix", "--shuffle-mixing",
                    str(p.get("shuffle_mixing", True)).lower()]
    elif stage == "miniaod":
        command += ["--skip-to", "gensim", "--stop-at", "miniaod"]
    elif stage == "ntuple":
        command += ["--skip-to", "ntuple", "--stop-at", "ntuple",
                    "--miniaod-input", str(work_dir / "input_MINIAOD.root")]
    else:
        raise ValueError(f"not a native chain stage: {stage}")
    for key, flag in (("first_run", "--first-run"),
                      ("first_luminosity_block", "--first-luminosity-block"),
                      ("first_event", "--first-event"),
                      ("num_events_in_lumi", "--number-events-in-lumi")):
        if key in p.get("edm_event_id", {}):
            command += [flag, str(p["edm_event_id"][key])]
    profile = p.get("detector_profile")
    if isinstance(profile, dict):
        command += ["--detector-profile-id", str(profile["profile_id"])]
        if stage == "ntuple":
            command += ["--ntuple-era", str(profile.get("ntuple_era", profile.get("era", "Run3"))),
                        "--ntuple-global-tag", str(profile["mc_global_tag"])]
    return command


def execute_chain(task, work_dir):
    p = parameters(task)
    config = read_json(core_path(task, "common/node_config_defaults.json"))
    # Only task-local source slots, never a complete production inventory.
    for key in ("target_mixed_events", "normal_shortfall_policy",
                "unused_hepmc_warning_fraction", "mixing_rng_seed",
                "phi_consumption_mode", "edm_event_id", "processing_environment"):
        if key in p:
            config[key] = p[key]
    if isinstance(p.get("detector_profile"), dict):
        profile = p["detector_profile"]
        config["detector_profile"] = profile
    if task["stage"] == "mix":
        references = {ref["artifact_id"]: ref for ref in task["inputs"]}
        if len(references) != len(task["inputs"]):
            raise ValueError("duplicate mix input artifact IDs")
        local = {}
        slots = []
        used = set()
        for slot in p["sources"]:
            entry = dict(slot)
            identities = entry.get("inputs", [])
            if not identities:
                raise ValueError("every shower slot must contain input artifact IDs")
            paths = []
            for identity in identities:
                if identity not in references:
                    raise ValueError(f"undeclared source input: {identity}")
                if identity in used:
                    raise ValueError(f"LHE block reused across source slots: {identity}")
                used.add(identity)
                local[identity] = materialize_input(references[identity], work_dir)
                paths.append("file:" + str(local[identity]))
            if int(entry.get("rng_seed", 0)) <= 0:
                raise ValueError("every source slot requires its frozen RNG seed")
            entry["inputs"] = paths
            slots.append(entry)
        if used != set(references):
            raise ValueError("mix contains unused input references")
        if int(p.get("mixing_rng_seed", 0)) <= 0:
            raise ValueError("mix requires its frozen RNG seed")
        config["sources"] = slots
    else:
        if len(task["inputs"]) != 1:
            raise ValueError(f"{task['stage']} requires exactly one input")
        source = materialize_input(task["inputs"][0], work_dir)
        if task["stage"] == "miniaod":
            destination = work_dir / "mixed.hepmc"
            opener = gzip.open if source.name.endswith(".gz") else open
            with opener(source, "rb") as incoming, destination.open("wb") as outgoing:
                shutil.copyfileobj(incoming, outgoing)
        else:
            destination = work_dir / "input_MINIAOD.root"
            if source.resolve() != destination.resolve():
                shutil.copy2(source, destination)
    config_path = work_dir / "chain.json"
    atomic_json(config_path, config)
    script = native_tree(task, work_dir, shower=task["stage"] == "mix")
    run(task, task["runtime"], build_command(task, work_dir, script, config_path),
        work_dir)
