"""Count the actual EDM product; never substitute planned or HepMC counts."""
from __future__ import annotations
import json
from pathlib import Path
import subprocess

from full_mc.runtime.profiles import run_profile


def parse_event_count(payload):
    entries = payload if isinstance(payload, list) else [payload]
    # Each invocation asks about precisely one file.
    if len(entries) != 1 or not isinstance(entries[0], dict):
        raise ValueError("edmFileUtil must describe exactly one file")
    events = entries[0].get("events")
    if type(events) is not int or events < 0:
        raise ValueError("edmFileUtil returned no exact event count")
    return events


def count_events(task, path, work_dir):
    with (Path(work_dir) / "commands.log").open("a", encoding="utf-8") as log:
        completed = run_profile(task["runtime"], ["edmFileUtil", "-j", str(path)],
                                cwd=work_dir, stdout=subprocess.PIPE, stderr=log)
    return parse_event_count(json.loads(completed.stdout))
