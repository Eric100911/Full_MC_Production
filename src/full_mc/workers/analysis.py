"""Pinned NtupleAnalyzer commands; both stages consume the full raw ntuple."""
from __future__ import annotations

from pathlib import Path
import shutil
import tarfile
from full_mc.runtime.bootstrap import _extract
from full_mc.storage.transfer import materialize_input
from ._common import batch_runtime_asset, core_path, parameters, prepare, publish, result, run


def build_command(task, raw_ntuple, work_dir, analyzer_path=None):
    p = parameters(task)
    analyzer = Path(analyzer_path or p.get("analyzer_path", core_path(task, "external/NtupleAnalyzer")))
    if p.get("analysis") != "JJP":
        raise ValueError("the full efficiency workflow currently supports JJP only")
    if task["stage"] == "selection":
        sample = p.get("sample")
        if not sample:
            suffix = str(p["campaign"]).removeprefix("JJP_")
            sample = suffix.replace("DPS1", "DPS_1").replace("DPS2", "DPS_2")
        return ["python3", str(analyzer / "merge_apply_cuts.py"),
                "--channel", "JJP", "--dataset", "mc", "--sample", sample,
                "--input-dir", str(raw_ntuple), "--output", str(work_dir / "selected.root"),
                "--jobs", "1", "--stage-mode", "never"]
    return ["python3", str(analyzer / "run_efficiency.py"),
            "--analysis-mode", "JpsiJpsiPhi", "--input-files", str(raw_ntuple),
            "--sample-name", str(p["campaign"]),
            "--output-dir", str(work_dir / "shards" / ("shard_" + p["merge_group"])),
            "--tree-path", "mkcands/X_data", "--remote-access-mode", "direct",
            "--efficiency-backend", "vectorized", "--step-size", "100 MB",
            "--skip-plots"]


def analyzer_tree(task, work_dir):
    if parameters(task).get("analyzer_path"):
        return Path(parameters(task)["analyzer_path"])
    if not task.get("runtime_dir"):
        return core_path(task, "external/NtupleAnalyzer")
    asset = batch_runtime_asset(task, "analyzer-el9")
    external = work_dir / "external"
    analyzer = external / "NtupleAnalyzer"
    if analyzer.is_symlink():
        analyzer.unlink()
    elif analyzer.exists():
        shutil.rmtree(analyzer)
    _extract(asset, external, "NtupleAnalyzer")
    return analyzer


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "analyzer-el9")
    if len(task.get("inputs", [])) != 1 or task["inputs"][0]["kind"] != "raw-ntuple":
        raise ValueError("analysis requires exactly one full raw-ntuple input")
    raw = materialize_input(task["inputs"][0], work_dir)
    analyzer = analyzer_tree(task, work_dir)
    run(task, "analyzer-el9", build_command(task, raw, work_dir, analyzer), work_dir)
    if task["stage"] == "selection":
        output = work_dir / "selected.root"
    else:
        shards = work_dir / "shards"
        sample = shards / ("shard_" + parameters(task)["merge_group"]) / parameters(task)["campaign"]
        for name in ("gen_systems.parquet", "event_step_flags.parquet"):
            if not (sample / name).is_file():
                raise RuntimeError(f"efficiency did not produce {sample / name}")
        output = work_dir / "efficiency-shard.tar"
        with tarfile.open(output, "w") as archive:
            for path in sorted(shards.rglob("*")):
                if path.is_file():
                    archive.add(path, arcname=str(path.relative_to(shards)), recursive=False)
    return result(task, [publish(output, task)])
