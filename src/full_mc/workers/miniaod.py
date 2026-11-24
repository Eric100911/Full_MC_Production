"""CMSSW12 processing from an validated mixed-HepMC checkpoint."""
from ._common import parameters, prepare
from .chain import execute_chain
from .edm import count_events


def produce(task, work_dir):
    work_dir = prepare(task, work_dir, task["runtime"])
    expected = int(task["inputs"][0]["events"])
    if expected <= 0 or not parameters(task).get("edm_event_id"):
        raise ValueError("MiniAOD requires positive HepMC count and frozen EDM IDs")
    execute_chain(task, work_dir)
    path = work_dir / "output_MINIAOD.root"
    events = count_events(task, path, work_dir)
    if not 0 < events <= expected:
        raise RuntimeError("MiniAOD has zero events or exceeds its mixed-HepMC count")
    partial = events < expected
    if partial and parameters(task).get("normal_shortfall_policy") != "report-and-truncate":
        raise RuntimeError("MiniAOD shortfall is not permitted by this task")
    return path, events, {"count_source": "edmFileUtil", "expected_events": expected,
                          "partial": partial,
                          "global_job_index": parameters(task).get("global_job_index"),
                          "edm_event_id": parameters(task).get("edm_event_id")}
