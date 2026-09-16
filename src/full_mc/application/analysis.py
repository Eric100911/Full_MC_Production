"""Explicit selection/efficiency aggregation from validated IHEP group products."""
from pathlib import Path, PurePosixPath
import re
import tarfile

from full_mc.io import atomic_json, read_json
from full_mc.settings import CORE_ROOT, LOG_LEVELS
from full_mc.storage.transfer import artifact_ready, materialize_input
from full_mc.workers._common import publish, result, run


def _log_level(value):
    value = str(value or "info")
    if value not in LOG_LEVELS:
        raise ValueError("log_level must be one of %s" % (LOG_LEVELS,))
    return value


def prepare_aggregate(handoff_path, output_path, campaign="JJP", log_level=None):
    if campaign != "JJP":
        raise ValueError("full efficiency aggregation currently supports JJP only")
    handoff = read_json(handoff_path)
    if handoff.get("schema") != "full-mc-handoff-v1":
        raise ValueError("unsupported handoff schema")
    groups, seen = {}, set()
    for task in handoff["tasks"]:
        subtasks = task["subtasks"]
        task_analysis = subtasks.get("merge", {}).get("parameters", {}).get("analysis")
        if task_analysis != campaign:
            continue
        selection, efficiency = subtasks["selection"], subtasks["efficiency"]
        parameters = efficiency["parameters"]
        sample = parameters["campaign"]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", sample):
            raise ValueError("sample must be a path-safe campaign identifier")
        pair = {}
        for name, stage, kind in (("selection", selection, "selected-ntuple"),
                                   ("efficiency", efficiency, "efficiency-shard")):
            outputs = stage["outputs"]
            if len(outputs) != 1 or outputs[0]["kind"] != kind:
                raise ValueError(f"{name} must declare exactly one {kind}")
            ref = outputs[0]
            if ref["artifact_id"] in seen:
                raise ValueError(f"duplicate aggregation artifact: {ref['artifact_id']}")
            if not artifact_ready(ref):
                raise ValueError(f"unverified aggregation input: {ref['artifact_id']}")
            seen.add(ref["artifact_id"])
            pair[name] = ref
        groups.setdefault(sample, []).append(pair)
    if not groups:
        raise ValueError("no validated selection/efficiency pairs to aggregate")
    output_path = Path(output_path).resolve()
    base = str(handoff.get("analysis_output", output_path.parent / "analysis-products")).rstrip("/")
    outputs = []
    for sample in groups:
        outputs.extend([
            {"artifact_id": f"aggregate:{sample}:selected", "kind": "selected-ntuple",
             "url": f"{base}/{sample}/selected.root"},
            {"artifact_id": f"aggregate:{sample}:efficiency", "kind": "efficiency-maps",
             "url": f"{base}/{sample}/efficiency.tar"},
        ])
    task = {"schema": "full-mc-aggregate-v1", "task_id": handoff["run_id"] + ":aggregate",
            "stage": "aggregate", "runtime": "analyzer-el9",
            "runtime_release": handoff["runtime_release"], "log_level": _log_level(log_level),
            "groups": groups, "outputs": outputs, "parameters": {}}
    atomic_json(output_path, task)
    return {"task_path": str(output_path), "samples": list(groups),
            "input_count": len(seen), "output_count": len(outputs)}


def _extract_shard(archive_path, destination, sample, used_names):
    """Extract only distinct regular shard members under their declared sample."""
    destination = Path(destination)
    with tarfile.open(archive_path, "r:") as archive:
        members = archive.getmembers()
        for member in members:
            name = PurePosixPath(member.name)
            if (not member.isfile() or name.is_absolute() or ".." in name.parts
                    or len(name.parts) < 3 or not name.parts[0].startswith("shard_")
                    or name.parts[1] != sample or member.name in used_names):
                raise ValueError(f"invalid or overlapping efficiency shard member: {member.name}")
            used_names.add(member.name)
        for member in members:
            target = destination / member.name
            if target.exists():
                raise FileExistsError(target)
            target.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            with archive.extractfile(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)


def run_aggregate_task(prepared_path, work_dir, log_level=None):
    task = read_json(prepared_path)
    if task.get("schema") != "full-mc-aggregate-v1":
        raise ValueError("unsupported aggregate task")
    task["log_level"] = _log_level(log_level or task.get("log_level"))
    root = Path(work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    analyzer = CORE_ROOT / "external/NtupleAnalyzer"
    products = []
    for sample, pairs in task["groups"].items():
        sample_root = root / sample
        # A fresh attempt avoids accepting files left by a failed aggregation.
        import tempfile
        sample_root.mkdir(parents=True, exist_ok=True)
        attempt = Path(tempfile.mkdtemp(prefix="attempt-", dir=sample_root))
        selected, used = [], set()
        for pair in pairs:
            for ref in pair.values():
                if not artifact_ready(ref):
                    raise RuntimeError(f"aggregation input is no longer valid: {ref['artifact_id']}")
            selected.append(str(materialize_input(pair["selection"], attempt)))
            shard = materialize_input(pair["efficiency"], attempt)
            _extract_shard(shard, attempt / "shards", sample, used)
        input_list = attempt / "selected-inputs.json"
        atomic_json(input_list, selected)
        selected_output = attempt / "selected.root"
        # Keep command arguments short regardless of the number of merge groups.
        merge_script = (
            "import ROOT,json; merger=ROOT.TFileMerger(False); "
            "assert merger.OutputFile(" + repr(str(selected_output)) + ",'RECREATE'); "
            "files=json.load(open(" + repr(str(input_list)) + ")); "
            "assert all(merger.AddFile(p) for p in files); assert merger.Merge()"
        )
        run(task, "analyzer-el9", ["python3", "-c", merge_script], attempt)
        maps = attempt / "efficiency"
        run(task, "analyzer-el9", [
            "python3", analyzer / "merge_efficiency_shards.py", "--sample", sample,
            "--shards-dir", attempt / "shards", "--output-dir", maps], attempt)
        run(task, "analyzer-el9", [
            "python3", analyzer / "build_derived_efficiency.py",
            "--input-dir", maps, "--skip-plots"], attempt)
        if not (maps / sample / "efficiency_maps.parquet").is_file():
            raise RuntimeError("efficiency aggregation did not produce its maps")
        archive_path = attempt / "efficiency.tar"
        with tarfile.open(archive_path, "w") as archive:
            for path in sorted(maps.rglob("*")):
                if path.is_file():
                    archive.add(path, arcname=str(path.relative_to(maps)), recursive=False)
        products.append(publish(selected_output, task, f"aggregate:{sample}:selected"))
        products.append(publish(archive_path, task, f"aggregate:{sample}:efficiency"))
    outcome = result(task, products)
    atomic_json(root / "result.json", outcome)
    return {"task_id": task["task_id"], "status": "ok",
            "result_path": str(root / "result.json"), "output_count": len(products)}
