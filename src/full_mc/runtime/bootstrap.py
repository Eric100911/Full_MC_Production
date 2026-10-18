"""Standalone transfer bootstrap; the child imports code from its runtime bundle."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile


def _extract(archive_path, destination, prefix):
    destination = Path(destination)
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        seen = set()
        for member in members:
            name = PurePosixPath(member.name)
            if (name.is_absolute() or ".." in name.parts or not name.parts
                    or name.parts[0] != prefix or member.name in seen
                    or not (member.isfile() or member.isdir())):
                raise ValueError(f"invalid runtime member: {member.name}")
            seen.add(member.name)
        for member in members:
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as incoming, target.open("xb") as output:
                shutil.copyfileobj(incoming, output)
            target.chmod(member.mode & 0o777)


def unpack_runtime(release_dir, destination, profile, release):
    """Unpack only this stage's payload; do not hash or copy whole run inventories."""
    source, destination = Path(release_dir).resolve(), Path(destination).resolve()
    descriptor = json.loads((source / "runtime.json").read_text())
    if descriptor["runtime_release"] != release or profile not in descriptor["profiles"]:
        raise ValueError("task does not match the transferred runtime release/profile")
    if destination.exists():
        raise FileExistsError(destination)
    destination.mkdir(parents=True)

    def bundle(key):
        name = descriptor["bundles"][key]
        if Path(name).name != name:
            raise ValueError("runtime bundle names must be basenames")
        return source / name

    _extract(bundle("core"), destination, "core")
    core = destination / "core"
    if profile in ("helac-el7", "cmssw15-el9", "cmssw15-sim-el9", "cmssw15-ntuple-el9"):
        asset = bundle(profile)
        if not asset.is_file() and profile == "helac-el7":
            # Some Condor schedds transfer the package under its source
            # basename even when the release descriptor uses the stable
            # runtime basename.  Accept that transfer spelling, but always
            # install the package under the descriptor name expected by the
            # worker.
            source_alias = source / "helac_package_cbseed_v1.tar.gz"
            if source_alias.is_file():
                asset = source_alias
        packages = core / "common/packages"
        packages.mkdir(parents=True, exist_ok=True)
        target = packages / Path(descriptor["bundles"][profile]).name
        try:
            os.link(asset, target)
        except OSError:
            shutil.copy2(asset, target)
    elif profile == "analyzer-el9":
        _extract(bundle(profile), core / "external", "NtupleAnalyzer")
    return core


def write_diagnostics(task, stage_work, destination):
    """Keep native HELAC evidence only for tasks explicitly requesting it.

    Preserve paths so helac_inspector.py can parse the extracted HELAC root.
    Native streams/results contain warning and stopping evidence; do not infer
    missing physics measurements or bundle LHE, executable trees or full traces.
    """
    stage_work, destination = Path(stage_work), Path(destination)
    paths = set(stage_work.rglob("*.log"))
    if (task.get("stage") == "lhe"
            and task.get("parameters", {}).get("retain_helac_diagnostics") is True):
        patterns = (
            "command_logs/*.stdout", "command_logs/*.stderr",
            "py8_onia_user.inp", "helac_forbidden_*.txt",
            "HELAC-Onia-2.7.6/run_config.ho",
            "HELAC-Onia-2.7.6/input/*.inp",
            "HELAC-Onia-2.7.6/input/seed.input",
            "HELAC-Onia-2.7.6/PROC_HO_*/input/*.inp",
            "HELAC-Onia-2.7.6/PROC_HO_*/input/seed.input",
            "HELAC-Onia-2.7.6/PROC_HO_*/P[0-9]*_calc_*/input/*.inp",
            "HELAC-Onia-2.7.6/PROC_HO_*/P[0-9]*_calc_*/input/seed.input",
            "HELAC-Onia-2.7.6/PROC_HO_*/P[0-9]*_calc_*/output/RESULT_*.out",
            "HELAC-Onia-2.7.6/PROC_HO_*/P[0-9]*_calc_*/output/screen_output.txt",
            "HELAC-Onia-2.7.6/PROC_HO_*/P[0-9]*_calc_*/output/screen_error.txt",
            "HELAC-Onia-2.7.6/PROC_HO_*/results/*.out",
        )
        for pattern in patterns:
            paths.update(stage_work.glob(pattern))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(destination, "w:gz", compresslevel=1) as archive:
        for path in sorted(paths):
            relative = path.relative_to(stage_work)
            if (path.is_file() and not path.is_symlink()
                    and not any((stage_work / parent).is_symlink()
                                for parent in relative.parents)):
                archive.add(path, arcname=str(relative), recursive=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", type=Path, required=True)
    parser.add_argument("--release-dir", type=Path, default=Path.cwd())
    parser.add_argument("--work-dir", type=Path, default=Path.cwd() / "work")
    parser.add_argument("--result", type=Path, default=Path.cwd() / "result.json")
    parser.add_argument("--diagnostics", type=Path, default=Path.cwd() / "diagnostics.tar.gz")
    args = parser.parse_args(argv)
    if sys.version_info < (3, 9):
        parser.error("the worker bootstrap requires Python 3.9 or newer")
    task = json.loads(args.task.read_text())
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    output = args.result.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"task_id": task["task_id"], "stage": task["stage"],
                                 "status": "failed", "outputs": []}) + "\n")
    stage_work = work / "stage"
    stage_work.mkdir()
    try:
        core = unpack_runtime(args.release_dir, work / "runtime", task["runtime"],
                              task["runtime_release"])
        task.setdefault("parameters", {})["core_root"] = str(core)
        if task["runtime"] == "analyzer-el9":
            task["parameters"]["analyzer_path"] = str(core / "external/NtupleAnalyzer")
        local_task = work / "task.json"
        local_task.write_text(json.dumps(task) + "\n")
        outcome = subprocess.run([sys.executable, str(core / "mcprod"), "worker",
                                  "--task", str(local_task), "--work-dir", str(stage_work),
                                  "--result", str(output)], check=False)
        return outcome.returncode
    finally:
        # Return diagnostics, not scratch LHE/HepMC/ROOT payloads or package trees.
        write_diagnostics(task, stage_work, args.diagnostics)


if __name__ == "__main__":
    raise SystemExit(main())
