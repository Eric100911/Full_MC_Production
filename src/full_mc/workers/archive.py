"""Archive compressed upstream products after component validation; no deletion."""
from pathlib import Path
import shutil

from full_mc.storage.archive import create_archive
from full_mc.storage.transfer import artifact_ready, materialize_input
from ._common import parameters, prepare, publish, result


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "utilities-el9")
    p = parameters(task)
    references = task.get("inputs", [])
    if p.get("archive_role") == "source":
        if not isinstance(p.get("source_plan_task"), str) or not p["source_plan_task"]:
            raise ValueError("source archive requires its frozen source PLAN identity")
        # Only exact, frozen originals may bypass the allocated MiniAOD gate.
        if (not references or any(ref.get("kind") != "lhe" or
                not str(ref.get("url", "")).endswith(".lhe.gz") or
                isinstance(ref.get("size_bytes"), bool) or
                not isinstance(ref.get("size_bytes"), int) or
                ref["size_bytes"] <= 0 or
                isinstance(ref.get("events"), bool) or
                not isinstance(ref.get("events"), int) or ref["events"] <= 0
                for ref in references)):
            raise ValueError("source archive requires frozen original LHE references")
    else:
        gates = p.get("component_miniaods", [])
        if not gates or not all(artifact_ready(ref) for ref in gates):
            raise RuntimeError("archive requires all allocated component MiniAODs to be valid")
    if not references or len({ref["artifact_id"] for ref in references}) != len(references):
        raise ValueError("archive requires distinct upstream artifacts")
    members = []
    identities = []
    for index, ref in enumerate(references):
        source = materialize_input(ref, work_dir)
        if not source.name.endswith((".lhe.gz", ".hepmc.gz")):
            raise ValueError("archive members must already be .lhe.gz or .hepmc.gz")
        # Prefixes disambiguate identical basenames without changing gzip members.
        member = work_dir / f"{index:06d}_{source.name}"
        shutil.copy2(source, member)
        members.append(member)
        identity = {"artifact_id": ref["artifact_id"], "member": member.name,
                    "url": ref["url"], "kind": ref["kind"],
                    "size_bytes": source.stat().st_size}
        if "events" in ref:
            identity["events"] = int(ref["events"])
        identities.append(identity)
    output = create_archive(members, work_dir / "upstream.tar")
    return result(task, [publish(output, task)], members=identities)
