"""Resolve a run's settings once, before any jobs are generated."""
from __future__ import annotations

import copy
from importlib import resources
import json
import os
import re
from pathlib import Path

from .profiles import load_catalogues, resolve_detector_variants, resolve_source
from .storage.layout import (
    canonical_storage_location,
    storage_location_is_within,
    storage_location_parent,
)

CORE_ROOT = Path(__file__).resolve().parents[2]
LOG_LEVELS = ("debug", "info", "warning", "error")


def read_json(path):
    with Path(path).open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return value


def _resource_json(name):
    """Use checkout records when present, otherwise installed package data."""
    checkout = CORE_ROOT / "common" / ("campaigns/catalog.json" if name == "catalog.json"
                                         else "node_config_defaults.json")
    if checkout.is_file():
        return read_json(checkout)
    with resources.files("full_mc.resources").joinpath(name).open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("installed resource %s: expected a JSON object" % name)
    return value


def catalog():
    value = _resource_json("catalog.json")
    profiles = load_catalogues()
    value["generator_profiles"] = profiles["generator"].as_dict()["profiles"]
    value["detector_profiles"] = profiles["detector"].as_dict()["profiles"]
    return value


def profile_catalogs():
    """Return the settings-boundary profile catalogues."""
    return load_catalogues()


def defaults():
    return _resource_json("node_config_defaults.json")


def _path(value, parent):
    path = Path(os.path.expandvars(str(value))).expanduser()
    return str((parent / path).resolve() if not path.is_absolute() else path.resolve())


def _integer(value, name, *, minimum=0):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be an integer")
    if number < minimum:
        qualifier = "nonnegative" if minimum == 0 else "positive"
        raise ValueError(f"{name} must be {qualifier}")
    return number


def _validate_scheduler_and_resources(spec):
    scheduler = spec.get("scheduler", {})
    if scheduler is None:
        scheduler = {}
    if not isinstance(scheduler, dict):
        raise ValueError("scheduler must be an object")
    maxjobs = scheduler.get("maxjobs", {})
    if not isinstance(maxjobs, dict):
        raise ValueError("scheduler.maxjobs must be an object")
    for category, value in maxjobs.items():
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", str(category)):
            raise ValueError("scheduler.maxjobs has an invalid category")
        maxjobs[category] = _integer(value, f"scheduler.maxjobs.{category}", minimum=1)
    for key in ("max_jobs_submitted", "max_jobs_idle", "max_submits_per_interval"):
        if key in scheduler:
            scheduler[key] = _integer(scheduler[key], f"scheduler.{key}", minimum=1)
    if "submit_delay" in scheduler:
        scheduler["submit_delay"] = _integer(scheduler["submit_delay"], "scheduler.submit_delay")
    spec["scheduler"] = scheduler

    resources = spec.get("resources", {})
    if resources is None:
        resources = {}
    if not isinstance(resources, dict):
        raise ValueError("resources must be an object")
    expected_cpus = {"lhe": 1, "plan": 1, "mix": 1, "miniaod": 2, "archive": 1}
    for stage, resource in resources.items():
        if stage not in expected_cpus or not isinstance(resource, dict):
            raise ValueError("resources must use known worker-stage objects")
        for key in ("memory_mb", "disk_bytes"):
            if key in resource:
                resource[key] = _integer(resource[key], f"resources.{stage}.{key}", minimum=1)
        if "cpus" in resource:
            cpus = _integer(resource["cpus"], f"resources.{stage}.cpus", minimum=1)
            if cpus != expected_cpus[stage]:
                raise ValueError(f"resources.{stage}.cpus disagrees with the worker runtime contract")
        if "flavour" in resource and not re.fullmatch(r"[A-Za-z0-9_.-]+", str(resource["flavour"])):
            raise ValueError(f"resources.{stage}.flavour is not a DAG token")
    spec["resources"] = resources


def _validate_generated_staging(spec):
    """Fail closed when run-scoped generated output overlaps immutable LHE."""
    generated_root = (
        str(spec["storage"]["cern_staging"]).rstrip("/")
        + "/"
        + spec["run_id"]
    )
    immutable_locations = []
    configured = defaults().get("lhe_pool_directories", {})
    if isinstance(configured, dict):
        for entry in configured.values():
            if not isinstance(entry, dict) or not entry.get("path"):
                continue
            immutable_locations.append(canonical_storage_location(entry["path"]))
    for source in spec.get("sources", []):
        if source.get("url"):
            immutable_locations.append(storage_location_parent(source["url"]))

    candidate = canonical_storage_location(generated_root)
    for inventory_location in immutable_locations:
        if storage_location_is_within(candidate, inventory_location):
            raise ValueError(
                "generated staging overlaps immutable LHE inventory: "
                + generated_root
            )


def load_run_spec(path):
    path = Path(path).resolve()
    spec = copy.deepcopy(read_json(path))
    if spec.get("schema_version") != 2:
        raise ValueError("unsupported run schema_version")
    spec["schema_version"] = 2
    run_id = str(spec.get("run_id", ""))
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id):
        raise ValueError("run_id must be a nonempty path-safe identifier")
    spec["core_root"] = _path(spec.get("core_root", str(CORE_ROOT)), path.parent)
    if "site_config" in spec:
        site = read_json(_path(spec["site_config"], path.parent))
    else:
        site = {}
    storage = dict(defaults()["production_storage"])
    storage.update(site.get("storage", {}))
    storage.update(spec.get("storage", {}))
    for key in ("cern_staging", "ihep_products", "ihep_archive"):
        value = str(storage[key])
        if not value:
            raise ValueError(f"missing storage.{key}")
        if not value.startswith("root://"):
            storage[key] = _path(value, path.parent)
    storage["staging_bytes"] = int(storage.get("staging_bytes", 100_000_000_000))
    storage["reserve_bytes"] = int(storage.get("reserve_bytes", 10_000_000_000))
    if not 0 <= storage["reserve_bytes"] < storage["staging_bytes"]:
        raise ValueError("staging reserve must be nonnegative and smaller than budget")
    spec["storage"] = storage
    spec["log_level"] = str(spec.get("log_level", "info"))
    if spec["log_level"] not in LOG_LEVELS:
        raise ValueError(f"log_level must be one of {LOG_LEVELS}")
    spec["block_events"] = int(spec.get("block_events", 1000))
    if spec["block_events"] <= 0:
        raise ValueError("block_events must be positive")
    spec.setdefault("runtime_release", "production-v2")
    spec.setdefault("core_revision", "")
    spec.setdefault("normal_shortfall_policy", "report-and-truncate")
    spec.setdefault("unused_hepmc_warning_fraction", 0.15)
    spec.setdefault("processing_environment", defaults()["processing_environment"])
    limits = spec.setdefault("limits", {})
    for key, value in (("source_lhe_bytes", 2_000_000_000),
                       ("source_block_bytes", 2_000_000_000),
                       ("mixed_hepmc_bytes", 2_000_000_000),
                       ("source_files_per_step", 4),
                       ("processing_nodes_per_wave", 10)):
        limits.setdefault(key, value)
    _validate_scheduler_and_resources(spec)
    for key in ("runtime_dir", "analyzer_path", "inventory_path"):
        if spec.get(key):
            spec[key] = _path(spec[key], path.parent)
    if not isinstance(spec.get("campaigns"), list) or not spec["campaigns"]:
        raise ValueError("campaigns must be a nonempty list")
    known = catalog()
    profile_catalog = profile_catalogs()
    names = set()
    for campaign in spec["campaigns"]:
        name = campaign.get("name", "")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) or name in names:
            raise ValueError(f"invalid or duplicate campaign: {name}")
        names.add(name)
        base = known["campaigns"].get(name, {})
        campaign.setdefault("analysis", base.get("analysis_type", "JJP"))
        if campaign["analysis"] not in ("JJP", "JUP"):
            raise ValueError(f"unsupported analysis: {campaign['analysis']}")
        if not campaign.get("sources"):
            budgets = campaign.get("source_lhe_budgets", [])
            efficiencies = campaign.get("source_efficiencies", [])
            if len(budgets) != len(base.get("inputs", [])) or not budgets:
                raise ValueError(f"{name}: supply source slots and LHE budgets")
            if efficiencies and len(efficiencies) != len(budgets):
                raise ValueError(f"{name}: source efficiency count mismatch")
            campaign["sources"] = [
                {"pool": pool, "mode": mode, "lhe_budget": budget,
                 "efficiency": efficiencies[i] if efficiencies else 1.0}
                for i, (pool, mode, budget) in enumerate(zip(
                    base["inputs"], base["shower_modes"], budgets))
            ]
        for slot in campaign["sources"]:
            slot["lhe_budget"] = _integer(slot.get("lhe_budget", 0),
                                           f"{name}: source LHE budget", minimum=1)
            if not slot.get("pool") or not slot.get("mode"):
                raise ValueError(f"{name}: every source needs pool and mode")
            efficiency = float(slot.get("efficiency", 1.0))
            if not 0 < efficiency <= 1:
                raise ValueError(f"{name}: efficiency must be in (0, 1]")
            slot["efficiency"] = efficiency
        for field in ("target_mixed_events", "max_processing_nodes", "merge_events"):
            campaign[field] = _integer(campaign.get(field, 0), f"{name}: {field}", minimum=1)
        resolved = resolve_detector_variants(campaign, profile_catalog["detector"])
        campaign["resolved_detector_variants"] = resolved
    if not isinstance(spec.get("sources"), list) or not spec["sources"]:
        raise ValueError("sources must declare an exact finite source inventory")
    ids = set()
    for source_index, source in enumerate(spec["sources"]):
        key = (source.get("pool"), source.get("source_id"))
        if not all(key) or key in ids:
            raise ValueError(f"invalid or duplicate source identity: {key}")
        ids.add(key)
        source["seed"] = _integer(source.get("seed", 0), f"{key}: seed", minimum=1)
        source = resolve_source(source, profile_catalog["generator"])
        # Replace the caller's mutable input with the immutable resolved record.
        spec["sources"][source_index] = source
        if source.get("generator_profile"):
            available = int(source["resolved_generator_profile"]["planning_events_hint"])
        else:
            source["actual_events"] = _integer(source.get("actual_events", 0),
                                                 f"{key}: actual_events", minimum=1)
            available = source["actual_events"]
            if not source.get("url"):
                raise ValueError(f"{key}: existing input requires URL and exact counted events")
            source["size_bytes"] = _integer(source.get("size_bytes", 0),
                                             f"{key}: size_bytes", minimum=0)
        if "planned_events" in source and source["planned_events"] is not None:
            source["planned_events"] = _integer(source["planned_events"],
                                                  f"{key}: planned_events", minimum=1)
            if source["planned_events"] > available:
                raise ValueError(f"{key}: planned_events exceeds exact source capacity")
        if source.get("url") and not str(source["url"]).startswith("root://"):
            source["url"] = _path(str(source["url"]).removeprefix("file:"), path.parent)
    # Inline controls are intentionally not accepted even if no source loop
    # happened to reference them (for example an empty malformed source list).
    if any("generation" in source for source in spec["sources"]):
        raise ValueError("inline generation settings are not supported; use generator_profile")
    declared_pools = {source["pool"] for source in spec["sources"]}
    referenced_pools = {slot["pool"] for campaign in spec["campaigns"]
                        for slot in campaign["sources"]}
    unknown_pools = referenced_pools - declared_pools
    if unknown_pools:
        raise ValueError("campaign references undeclared source pool(s): "
                         + ", ".join(sorted(unknown_pools)))
    _validate_generated_staging(spec)
    return spec
