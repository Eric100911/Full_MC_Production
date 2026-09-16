"""Publish only the accepted mixed HepMC; source-slot HepMC stays in scratch."""
from __future__ import annotations

import gzip
import shutil

from full_mc.io import read_json
from ._common import prepare, publish, result
from .chain import execute_chain


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "pythia-el8")
    execute_chain(task, work_dir)
    manifest = read_json(work_dir / "mixed_manifest.json")
    events = int(manifest.get("actual_mixed_hepmc_events",
                              manifest.get("events_written", 0)))
    mixed = work_dir / "mixed.hepmc"
    with mixed.open(encoding="utf-8") as stream:
        counted = sum(line.startswith("E ") for line in stream)
    if events <= 0 or events != counted:
        raise RuntimeError("mixed HepMC count disagrees with mixer result")
    compressed = work_dir / "mixed.hepmc.gz"
    with mixed.open("rb") as source, gzip.open(compressed, "wb", compresslevel=1) as target:
        shutil.copyfileobj(source, target)
    output = publish(compressed, task, events=events)
    source_counts = [
        {"slot": index,
         "actual_hepmc_events": read_json(work_dir / f"shower_{slot.get('slot', index)}_manifest.json")["actual_hepmc_events"]}
        for index, slot in enumerate(task["parameters"]["sources"])
    ]
    return result(task, [output], source_counts=source_counts)
