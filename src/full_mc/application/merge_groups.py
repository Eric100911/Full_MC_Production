"""Build the IHEP combined tasks from ordered MiniAOD references."""
from copy import deepcopy

from full_mc.planning.tasks import artifact_reference, task_manifest
from full_mc.runtime.profiles import runtime_for_profile


def sample_key(campaign, profile):
    return f"{campaign}::{profile}"


def _combined(spec, campaign, detector_profile, refs, ordinal):
    name = campaign["name"]
    variant = detector_profile["profile_id"]
    external_ordinal = int(campaign.get("merge_group_index_start", 0)) + int(ordinal)
    identity = f"{name}_{variant}_MERGE{external_ordinal:06d}"
    base = (spec["storage"]["ihep_products"].rstrip("/") + "/" + spec["run_id"]
            + "/" + name + "/" + variant)
    parameters = {"campaign": name, "analysis": campaign["analysis"],
                  "merge_group": identity, "sample_key": sample_key(name, variant),
                  "detector_profile_id": variant, "detector_profile": deepcopy(detector_profile),
                  "simulation_runtime": runtime_for_profile(detector_profile, "simulation"),
                  "merge_runtime": runtime_for_profile(detector_profile, "merge"),
                  "ntuple_runtime": runtime_for_profile(detector_profile, "ntuple"),
                  "mc_global_tag": detector_profile.get("mc_global_tag"),
                  "era": detector_profile.get("era")}
    provenance = {"detector_profile_id": variant,
                  "detector_profile": deepcopy(detector_profile),
                  "mc_global_tag": detector_profile.get("mc_global_tag"),
                  "era": detector_profile.get("era"),
                  "simulation_runtime": runtime_for_profile(detector_profile, "simulation"),
                  "merge_runtime": runtime_for_profile(detector_profile, "merge"),
                  "ntuple_runtime": runtime_for_profile(detector_profile, "ntuple")}
    outputs = {
        "merge": artifact_reference(identity + "_merged", base + "/merged/" + identity + ".root", "miniaod", **provenance),
        "ntuple": artifact_reference(identity + "_raw", base + "/ntuples/" + identity + ".root", "raw-ntuple", **provenance),
        "selection": artifact_reference(identity + "_selected", base + "/selected/" + identity + ".root", "selected-ntuple", **provenance),
        "efficiency": artifact_reference(identity + "_efficiency", base + "/efficiency/" + identity + ".tar", "efficiency-shard", **provenance)}
    if campaign["analysis"] == "JUP":
        outputs = {key: value for key, value in outputs.items() if key in ("merge", "ntuple")}
    subtasks = {stage: task_manifest(spec, identity + "_" + stage, stage, refs if stage == "merge" else [],
                            [output], dict(parameters)) for stage, output in outputs.items()}
    return {"task_id": identity, "stage": "combined", "runtime_release": spec["runtime_release"],
            "log_level": spec["log_level"], "subtasks": subtasks}



def merge_groups(spec, samples):
    """Build downstream tasks in campaign, detector variant and PROC order."""
    groups = []
    for campaign in spec["campaigns"]:
        event_limit = campaign.get("merge_events")
        byte_limit = campaign.get("merge_bytes")
        for profile in campaign["resolved_detector_variants"]:
            refs = samples.get(sample_key(campaign["name"], profile["profile_id"]), [])
            start, ordinal, events, size = 0, 0, 0, 0
            for end, ref in enumerate(refs, 1):
                events += ref["events"]
                size += ref["size_bytes"]
                if ((event_limit and events >= event_limit) or (byte_limit and size >= byte_limit)
                        or end == len(refs)):
                    groups.append(_combined(spec, campaign, profile, refs[start:end], ordinal))
                    start, ordinal, events, size = end, ordinal + 1, 0, 0
    return groups
