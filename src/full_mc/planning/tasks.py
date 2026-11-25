"""Frozen task manifests and exact artifact references."""
from copy import deepcopy

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


def processing_tasks(spec, campaign, nodes, refs, source_indices):
    cpus = spec["resources"]["processing"]["cpus"]
    for node in nodes:
        first_block = node["sources"][0]["block_refs"][0]
        source_index = source_indices[first_block]
        job_id = f"JOB{source_index:06d}_BLOCK{node['job_index']:06d}"
        identity = campaign["name"] + "_" + job_id
        inputs, slots = [], []
        for slot in node["sources"]:
            inputs.extend(deepcopy(refs[key]) for key in slot["block_refs"])
            slots.append({"slot": slot["slot"], "mode": slot["mode"],
                "inputs": slot["block_refs"], "rng_seed": slot["source_rng_seed"],
                "max_lhe_events": slot["lhe_events"], "target_hepmc_events": node["target_mixed_events"],
                "max_hadronization_retries": int(campaign["sources"][slot["slot"]].get("max_hadronization_retries", 5000))})
        parameters = {"campaign": campaign["name"], "analysis": campaign["analysis"],
            "job_id": job_id, "target_mixed_events": node["target_mixed_events"],
            "global_job_index": node.get("global_job_index", node["job_index"]),
            "edm_event_id": node["edm_event_id"], "normal_shortfall_policy": spec["normal_shortfall_policy"],
            "unused_hepmc_warning_fraction": spec["unused_hepmc_warning_fraction"],
            "processing_environment": spec["processing_environment"], "processing_cpus": cpus}
        mixed_base = spec["storage"]["ihep_archive"].rstrip("/") + "/" + spec["run_id"]
        hepmc = artifact_reference("HEPMC_" + identity, mixed_base + "/mixed/" + identity + ".hepmc.gz", "hepmc")
        hepmc["sidecar_url"] = hepmc["url"] + ".json"
        mix = task_manifest(spec, "MIX_" + identity, "mix", inputs, [hepmc],
            dict(parameters, sources=slots, mixing_rng_seed=node["mixing_rng_seed"]))
        variants = []
        for profile in campaign["resolved_detector_variants"]:
            variant = profile["profile_id"]
            detector_identity = f"{campaign['name']}_{variant}_{job_id}"
            detector_parameters = dict(parameters, detector_profile_id=variant,
                detector_profile=deepcopy(profile), simulation_runtime=runtime_for_profile(profile, "simulation"),
                merge_runtime=runtime_for_profile(profile, "merge"), ntuple_runtime=runtime_for_profile(profile, "ntuple"))
            product = artifact_reference("MINIAOD_" + detector_identity,
                spec["storage"]["ihep_products"].rstrip("/") + "/" + spec["run_id"] + "/"
                + campaign["name"] + "/" + variant + "/" + job_id + ".root", "miniaod",
                detector_profile_id=variant, detector_profile=deepcopy(profile))
            mini = task_manifest(spec, "MINIAOD_" + detector_identity, "miniaod", [hepmc], [product], detector_parameters)
            product["sidecar_url"] = product["url"] + ".json"
            variants.append(mini)
        outputs = [hepmc] + [mini["outputs"][0] for mini in variants]
        processing = task_manifest(spec, "PROC_" + identity, "processing", inputs, outputs, {})
        processing.update(mix=mix, variants=variants, core_revision=spec["core_revision"])
        yield processing
