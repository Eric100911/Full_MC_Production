"""Transfer the exact PROC result records selected by PLAN."""
from pathlib import Path
import shutil
import sys
import tarfile
from tempfile import NamedTemporaryFile

from full_mc.io import read_json
from full_mc.planning.graph import PLAN_RECORD, RESULTS_BUNDLE, node_stem, token


def _result_names(plan):
    return [f"results/{node_stem(token(job['task_id']))}.json"
            for job in plan["processing"]]


def pack(proc_dir):
    root = Path(proc_dir)
    names = _result_names(read_json(root / PLAN_RECORD))
    with NamedTemporaryFile(dir=root, prefix=".reclaim-", delete=False) as temporary:
        candidate = Path(temporary.name)
    try:
        with tarfile.open(candidate, "w:gz") as archive:
            # Keep the directory member even when PLAN allocated no PROC jobs.
            directory = tarfile.TarInfo("results")
            directory.type = tarfile.DIRTYPE
            directory.mode = 0o755
            archive.addfile(directory)
            for name in names:
                path = root / name
                if not path.is_file() or path.is_symlink() or path.stat().st_size == 0:
                    raise ValueError(f"missing or empty PROC result: {path}")
                archive.add(path, arcname=name, recursive=False)
        candidate.replace(root / RESULTS_BUNDLE)
    finally:
        candidate.unlink(missing_ok=True)


def unpack(bundle, directory, plan):
    """Read only regular JSON members named by PLAN."""
    destination = Path(directory)
    names = _result_names(plan)
    with tarfile.open(bundle, "r:gz") as archive:
        members = archive.getmembers()
        actual = {member.name for member in members}
        missing = set(names) - actual
        if missing:
            raise FileNotFoundError(f"missing PROC result from {bundle}: {min(missing)}")
        if actual != {"results", *names} or len(members) != len(actual):
            raise ValueError(f"unexpected or duplicate PROC result bundle members: {bundle}")
        for member in members:
            if member.name == "results":
                if not member.isdir():
                    raise ValueError(f"invalid results directory member: {bundle}")
                continue
            if not member.isfile() or member.size == 0:
                raise ValueError(f"invalid PROC result bundle member: {member.name}")
        destination.mkdir(parents=True, exist_ok=True)
        for name in names:
            target = destination / name.removeprefix("results/")
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(name) as incoming, target.open("xb") as outgoing:
                shutil.copyfileobj(incoming, outgoing)
    return destination


if __name__ == "__main__":
    try:
        pack(sys.argv[1])
    except (OSError, ValueError, tarfile.TarError) as exc:
        sys.exit(f"could not pack RECLAIM results: {exc}")
