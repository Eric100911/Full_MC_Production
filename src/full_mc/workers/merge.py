"""Merge exact validated MiniAOD components locally under CMSSW12/EL8."""
from full_mc.storage.transfer import artifact_ready, materialize_input
from ._common import prepare, publish, result, run
from .edm import count_events


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, task["runtime"])
    inputs = task.get("inputs", [])
    if not inputs or len({x["artifact_id"] for x in inputs}) != len(inputs):
        raise ValueError("merge requires distinct input artifacts")
    if not all(artifact_ready(item) for item in inputs):
        raise RuntimeError("merge component validation failed")
    expected = sum(int(item["events"]) for item in inputs)
    if any(int(item["events"]) <= 0 for item in inputs):
        raise ValueError("merge needs exact positive component counts")
    paths = [materialize_input(item, work_dir) for item in inputs]
    input_list = work_dir / "inputs.txt"
    input_list.write_text("".join("file:" + str(path) + "\n" for path in paths),
                          encoding="utf-8")
    output = work_dir / "merged_MINIAOD.root"
    run(task, task["runtime"], [
        "edmCopyPickMerge", "inputFiles_load=" + str(input_list),
        "outputFile=" + str(output), "maxSize=0"], work_dir)
    events = count_events(task, output, work_dir)
    if events != expected:
        raise RuntimeError(f"merged event count {events} differs from component sum {expected}")
    return result(task, [publish(output, task, events=events,
                                 metadata={"count_source": "edmFileUtil"})])
