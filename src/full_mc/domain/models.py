"""Validated, stdlib-only objects at the JSON boundary.

The planner deliberately keeps URLs opaque.  A source's event count comes from
the authoritative inventory supplied in the run spec; it is never inferred
from its filename or byte size.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional


def _positive(value: Any, name: str, allow_zero: bool = False) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if number < 0 or (number == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be {qualifier}")
    return number


@dataclass(frozen=True)
class Artifact:
    artifact_id: str
    url: str
    kind: str
    events: Optional[int] = None
    size_bytes: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.artifact_id or not self.url or not self.kind:
            raise ValueError("artifact_id, url, and kind are required")
        if self.events is not None:
            _positive(self.events, "artifact.events", allow_zero=True)
        if self.size_bytes is not None:
            _positive(self.size_bytes, "artifact.size_bytes", allow_zero=True)

    def to_dict(self) -> Dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass(frozen=True)
class Source:
    pool: str
    source_id: str
    seed: int
    url: str = ""
    actual_events: Optional[int] = None
    size_bytes: Optional[int] = None
    planned_events: Optional[int] = None
    generator_profile: Optional[str] = None
    resolved_generator_profile: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if not self.pool or not self.source_id:
            raise ValueError("source pool and source_id are required")
        if bool(self.url) == bool(self.generator_profile):
            raise ValueError("source must select exactly one URL or generator_profile")
        if self.url:
            _positive(self.actual_events, "source.actual_events")
            _positive(self.size_bytes, "source.size_bytes", allow_zero=True)
        elif not isinstance(self.resolved_generator_profile, dict):
            raise ValueError("generated source must carry its resolved profile")
        if self.planned_events is not None:
            _positive(self.planned_events, "source.planned_events", allow_zero=True)
            if self.actual_events is not None and self.planned_events > self.actual_events:
                raise ValueError("source.planned_events exceeds actual_events")

    @property
    def available_events(self) -> int:
        available = self.actual_events
        if available is None:
            available = int(self.resolved_generator_profile["planning_events_hint"])
        return available if self.planned_events is None else self.planned_events

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Source":
        return cls(**{name: raw.get(name) for name in (
            "pool", "source_id", "seed", "url", "actual_events", "size_bytes", "planned_events",
            "generator_profile", "resolved_generator_profile")})

    def to_dict(self) -> Dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass(frozen=True)
class CampaignSource:
    pool: str
    mode: str
    lhe_budget: int
    efficiency: float

    def __post_init__(self) -> None:
        if not self.pool or not self.mode:
            raise ValueError("campaign source pool and mode are required")
        _positive(self.lhe_budget, "campaign source lhe_budget")
        if not 0 < float(self.efficiency) <= 1:
            raise ValueError("campaign source efficiency must be in (0, 1]")


@dataclass(frozen=True)
class Campaign:
    name: str
    analysis: str
    sources: List[CampaignSource]
    target_mixed_events: int
    max_processing_nodes: int
    merge_events: int
    detector_variants: List[str] = field(default_factory=list)
    resolved_detector_variants: List[Dict[str, Any]] = field(default_factory=list)
    global_job_index_start: int = 0
    merge_group_index_start: int = 0

    def __post_init__(self) -> None:
        if not self.name or self.analysis not in {"JJP", "JUP"}:
            raise ValueError("campaign requires a name and JJP or JUP analysis")
        if not self.sources:
            raise ValueError("campaign requires at least one source slot")
        _positive(self.target_mixed_events, "campaign.target_mixed_events")
        _positive(self.max_processing_nodes, "campaign.max_processing_nodes")
        _positive(self.merge_events, "campaign.merge_events")
        _positive(self.global_job_index_start, "campaign.global_job_index_start", allow_zero=True)
        _positive(self.merge_group_index_start, "campaign.merge_group_index_start", allow_zero=True)
        if not self.detector_variants or len(self.detector_variants) != len(set(self.detector_variants)):
            raise ValueError("campaign requires distinct detector_variants")
        if len(self.resolved_detector_variants) != len(self.detector_variants):
            raise ValueError("campaign detector variants must be resolved")

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Campaign":
        return cls(
            name=str(raw.get("name", "")), analysis=str(raw.get("analysis", "")),
            sources=[CampaignSource(**item) for item in raw.get("sources", [])],
            target_mixed_events=raw.get("target_mixed_events"),
            max_processing_nodes=raw.get("max_processing_nodes"),
            merge_events=raw.get("merge_events"),
            detector_variants=list(raw.get("detector_variants", [])),
            resolved_detector_variants=list(raw.get("resolved_detector_variants", [])),
            global_job_index_start=int(raw.get("global_job_index_start", 0)),
            merge_group_index_start=int(raw.get("merge_group_index_start", 0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        raw = asdict(self)
        return raw


@dataclass(frozen=True)
class RunSpec:
    schema_version: int
    run_id: str
    core_revision: str
    runtime_release: str
    storage: Dict[str, Any]
    sources: List[Source]
    campaigns: List[Campaign]
    block_events: int
    log_level: str = "info"
    profiles: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != 2:
            raise ValueError("only run-spec schema_version 2 is supported")
        if not self.run_id or not self.runtime_release:
            raise ValueError("run_id and runtime_release are required")
        _positive(self.block_events, "block_events")
        required_storage = {"cern_staging", "ihep_products", "ihep_archive", "staging_bytes", "reserve_bytes"}
        missing = required_storage - set(self.storage)
        if missing:
            raise ValueError(f"storage misses: {', '.join(sorted(missing))}")
        staging = _positive(self.storage["staging_bytes"], "storage.staging_bytes")
        reserve = _positive(self.storage["reserve_bytes"], "storage.reserve_bytes", allow_zero=True)
        if reserve >= staging:
            raise ValueError("storage.reserve_bytes must be lower than staging_bytes")
        source_keys = [(source.pool, source.source_id) for source in self.sources]
        if len(set(source_keys)) != len(source_keys):
            raise ValueError("source (pool, source_id) pairs must be unique")
        if len({campaign.name for campaign in self.campaigns}) != len(self.campaigns):
            raise ValueError("campaign names must be unique")
        known_pools = {source.pool for source in self.sources}
        unknown = {slot.pool for campaign in self.campaigns for slot in campaign.sources} - known_pools
        if unknown:
            raise ValueError(f"campaign references unknown pools: {', '.join(sorted(unknown))}")

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "RunSpec":
        return cls(
            schema_version=int(raw.get("schema_version", 0)), run_id=str(raw.get("run_id", "")),
            core_revision=str(raw.get("core_revision", "")), runtime_release=str(raw.get("runtime_release", "")),
            storage=dict(raw.get("storage", {})),
            sources=[Source.from_dict(item) for item in raw.get("sources", [])],
            campaigns=[Campaign.from_dict(item) for item in raw.get("campaigns", [])],
            block_events=raw.get("block_events"), log_level=str(raw.get("log_level", "info")),
            profiles=dict(raw.get("profiles", {})),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version, "run_id": self.run_id,
            "core_revision": self.core_revision, "runtime_release": self.runtime_release,
            "storage": dict(self.storage), "sources": [item.to_dict() for item in self.sources],
            "campaigns": [item.to_dict() for item in self.campaigns], "block_events": self.block_events,
            "log_level": self.log_level, "profiles": dict(self.profiles),
        }


def task_dict(task_id: str, stage: str, runtime: str, inputs: Iterable[Artifact],
              outputs: Iterable[Artifact], parameters: Mapping[str, Any], log_level: str,
              runtime_release: str) -> Dict[str, Any]:
    """Produce the public task JSON schema, without hidden mutable state."""
    if not task_id or not stage or not runtime or not runtime_release:
        raise ValueError("task_id, stage, runtime, and runtime_release are required")
    return {"task_id": task_id, "stage": stage, "runtime": runtime,
            "inputs": [item.to_dict() for item in inputs],
            "outputs": [item.to_dict() for item in outputs], "parameters": dict(parameters),
            "log_level": log_level, "runtime_release": runtime_release}
