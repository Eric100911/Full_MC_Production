"""Immutable generator and detector profile catalogues.

The catalogue is a settings-boundary input.  Callers receive deep copies so a
worker cannot accidentally modify the catalogue that was used to resolve a
run.  Workers are expected to consume the resolved record embedded in their
task JSON instead of reopening these files.
"""
from __future__ import annotations

from copy import deepcopy
import json
from importlib import resources
from pathlib import Path
from typing import Any, Mapping


_CATALOGUE_FILES = {
    "generator": "generator_profiles.json",
    "detector": "detector_profiles.json",
}


def _catalogue_path(kind: str) -> Path:
    if kind not in _CATALOGUE_FILES:
        raise ValueError(f"unknown profile catalogue: {kind}")
    return Path(__file__).resolve().parents[2] / "common" / "profiles" / _CATALOGUE_FILES[kind]


def load_catalogue(kind: str) -> dict[str, Any]:
    """Load one catalogue from checkout resources or installed package data."""
    path = _catalogue_path(kind)
    if path.is_file():
        value = json.loads(path.read_text(encoding="utf-8"))
    else:
        resource = resources.files("full_mc.resources").joinpath(_CATALOGUE_FILES[kind])
        with resource.open(encoding="utf-8") as handle:
            value = json.load(handle)
    if not isinstance(value, dict) or not isinstance(value.get("profiles"), dict):
        raise ValueError(f"invalid {kind} profile catalogue")
    return value


class ProfileCatalog:
    """A validated lookup table for one profile kind."""

    def __init__(self, kind: str, payload: Mapping[str, Any] | None = None):
        self.kind = kind
        payload = payload if payload is not None else load_catalogue(kind)
        profiles = payload.get("profiles")
        if not isinstance(profiles, Mapping):
            raise ValueError(f"{kind} profile catalogue must contain profiles")
        self._profiles = {str(name): deepcopy(dict(profile))
                         for name, profile in profiles.items()}
        for name, profile in self._profiles.items():
            if profile.get("profile_id") != name:
                raise ValueError(f"{kind} profile ID mismatch: {name}")
            if profile.get("availability") not in {"ready", "unavailable"}:
                raise ValueError(f"{kind} profile {name} has invalid availability")
        if kind == "detector":
            for profile in self._profiles.values():
                validate_pileup(profile)

    def names(self) -> tuple[str, ...]:
        return tuple(self._profiles)

    def get(self, name: str) -> dict[str, Any] | None:
        profile = self._profiles.get(str(name))
        return deepcopy(profile) if profile is not None else None

    def require_ready(self, name: str) -> dict[str, Any]:
        profile = self._profiles.get(str(name))
        if profile is None:
            raise ValueError(f"unknown {self.kind} profile: {name}")
        if profile.get("availability") != "ready":
            reasons = profile.get("blocked_reasons") or ["profile is unavailable"]
            raise ValueError(f"{self.kind} profile {name} is unavailable: "
                             + "; ".join(map(str, reasons)))
        return deepcopy(profile)

    def as_dict(self) -> dict[str, Any]:
        return {"profiles": deepcopy(self._profiles)}


def load_catalogues() -> dict[str, ProfileCatalog]:
    return {kind: ProfileCatalog(kind) for kind in _CATALOGUE_FILES}


def validate_pileup(profile: Mapping[str, Any]) -> None:
    """Enforce the semantic distinction between NoPileUp, mixing, and PreMix."""
    pileup = profile.get("pileup")
    if not isinstance(pileup, Mapping):
        # Unavailable year manifests may intentionally omit implementation
        # details, but they still need a declared mode for catalogue display.
        if profile.get("availability") == "unavailable":
            return
        raise ValueError(f"{profile.get('profile_id', 'profile')} lacks pileup")
    mode = pileup.get("mode")
    secondary = pileup.get("secondary_kind")
    if mode == "none" and secondary is not None:
        raise ValueError("NoPileUp must not declare secondary input")
    if mode == "mixing" and secondary != "minbias-gensim":
        raise ValueError("ordinary mixing requires min-bias GEN-SIM input")
    if mode == "premix" and secondary != "premix":
        raise ValueError("PreMix stage 2 requires PRemix input")
    if mode not in {"none", "mixing", "premix"}:
        raise ValueError(f"unsupported pileup mode: {mode}")


def resolve_source(raw: Mapping[str, Any], generator_profiles: ProfileCatalog) -> dict[str, Any]:
    """Resolve exactly one existing URL or ready immutable generator profile."""
    source = deepcopy(dict(raw))
    if "generation" in source:
        raise ValueError("inline generation settings are not supported; use generator_profile")
    has_existing = bool(source.get("url"))
    has_generation = bool(source.get("generator_profile"))
    if has_existing == has_generation:
        raise ValueError("source must select exactly one of an existing URL or generator_profile")

    if has_generation:
        forbidden = set(source) - {"pool", "source_id", "seed", "generator_profile",
                                   "resolved_generator_profile"}
        if forbidden:
            raise ValueError("fresh source supplies only pool, source_id, seed, and generator_profile: "
                             + ", ".join(sorted(forbidden)))
        profile = generator_profiles.require_ready(source["generator_profile"])
        if source.get("pool") not in profile.get("compatible_pools", []):
            raise ValueError("generator profile is incompatible with source pool")
        source["resolved_generator_profile"] = profile
        source.pop("actual_events", None)
        source.pop("size_bytes", None)
    else:
        if "actual_events" not in source:
            raise ValueError("existing source requires exact actual_events")
        if "size_bytes" not in source:
            raise ValueError("existing source requires exact size_bytes")
        source.pop("generator_profile", None)
    return source


def resolve_detector_variants(campaign: Mapping[str, Any],
                              detector_profiles: ProfileCatalog) -> list[dict[str, Any]]:
    names = campaign.get("detector_variants")
    if (not isinstance(names, list) or not names
            or any(not isinstance(name, str) or not name for name in names)
            or len(names) != len(set(names))):
        raise ValueError("campaign requires distinct detector_variants")
    return [detector_profiles.require_ready(name) for name in names]
