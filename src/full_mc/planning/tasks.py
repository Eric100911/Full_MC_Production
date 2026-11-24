"""Frozen task manifests and exact artifact references."""
from full_mc.planning.graph import token
from full_mc.runtime.profiles import STAGE_RUNTIME, runtime_for_profile


def task_manifest(spec, identity, stage, inputs, outputs, parameters):
    profile = parameters.get("detector_profile")
    if profile and stage in {"miniaod", "merge", "ntuple"}:
        role = {"miniaod": "simulation", "merge": "merge", "ntuple": "ntuple"}[stage]
        runtime = runtime_for_profile(profile, role)
    else:
        runtime = STAGE_RUNTIME[stage]
    return {"task_id": token(identity), "stage": stage, "runtime": runtime,
            "runtime_release": spec["runtime_release"], "log_level": spec["log_level"],
            "inputs": inputs, "outputs": outputs, "parameters": parameters}


def artifact_reference(identity, url, kind, **extra):
    return dict(artifact_id=identity, url=url, kind=kind, **extra)


def staging_root(spec):
    return spec["storage"]["cern_staging"].rstrip("/") + "/" + spec["run_id"]


def reclaim_references(refs):
    """Exact staged objects a reclaim task may delete, deduplicated by identity.

    Only an explicitly recorded ``sidecar_url`` travels with a reference: PLAN
    blocks publish no EOS sidecar, so manufacturing ``url + ".json"`` would
    delete an object this run never staged.
    """
    seen, values = set(), []
    for ref in refs:
        identity = ref["artifact_id"]
        if identity in seen:
            continue
        seen.add(identity)
        item = {key: ref[key] for key in ("artifact_id", "url", "kind") if key in ref}
        if "sidecar_url" in ref:
            item["sidecar_url"] = ref["sidecar_url"]
        values.append(item)
    return values
