"""Reclaim run-owned staged products; no discovery and no alternate paths."""
from full_mc.storage.reclaim import remove_many
from full_mc.storage.transfer import artifact_ready
from ._common import parameters, prepare, result


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "utilities-el9")
    staging_prefix = str(parameters(task)["staging_prefix"]).rstrip("/") + "/"
    references = task.get("inputs", [])
    if parameters(task).get("source_archive_task"):
        archive = parameters(task)["verified_archive"]
        if not artifact_ready(archive):
            raise RuntimeError("source archive cannot be verified before reclamation")
    if not references and not parameters(task).get("terminal_blocks"):
        raise ValueError("reclaim requires explicit artifact references")
    plans = []
    targets = []
    seen = set()
    for ref in references:
        if parameters(task).get("terminal_blocks") and ref["kind"] != "lhe-block":
            raise ValueError("terminal cleanup requires exact LHE block references")
        url = str(ref["url"])
        if not url.startswith(staging_prefix):
            raise ValueError("refusing to reclaim outside run staging: " + url)
        objects = [("payload", url)]
        # Only an explicitly declared sidecar is deleted.  PLAN blocks publish
        # no EOS sidecar, so manufacturing url + ".json" would delete an object
        # this run never staged.
        if "sidecar_url" in ref:
            sidecar = str(ref["sidecar_url"])
            if not sidecar.startswith(staging_prefix):
                raise ValueError("refusing to reclaim outside run staging: " + sidecar)
            objects.append(("sidecar", sidecar))
        for _kind, target in objects:
            if target not in seen:
                seen.add(target)
                targets.append(target)
        plans.append((ref["artifact_id"], objects))
    dispositions = remove_many(targets, work_dir=work_dir)
    reclaimed = [{"artifact_id": identity,
                  "objects": [{"kind": kind, "url": target,
                               "disposition": dispositions[target]}
                              for kind, target in objects]}
                 for identity, objects in plans]
    return result(task, [], reclaimed=reclaimed)
