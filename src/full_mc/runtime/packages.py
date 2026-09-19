"""Small, reusable runtime releases, built from explicit payload allowlists."""
from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile

from full_mc.io import atomic_json
from .profiles import PROFILES

CMSSW15_RUNTIME = "cmssw15_tpsonia2mumu_runtime.tar.gz"
CMSSW15_SOURCE_PACKAGE = "tpsonia2mumu_code.tar.gz"
HELAC_SOURCE_PACKAGE = "helac_package_cbseed_v1.tar.gz"
HELAC_RUNTIME_PACKAGE = "helac_package.tar.gz"
HELAC_PACKAGE_SHA256 = (
    "407a62f3bdc29774f9537a8326a69e6665dd1c17a02d5c952e242a7d3cd55760"
)
CMSSW15_REQUIRED = (
    "CMSSW_15_0_15/src/",
    "CMSSW_15_0_15/src/HeavyFlavorAnalysis/TPS-Onia2MuMu/",
)
SITE_PROFILES = {
    "cern": ("helac-el7", "utilities-el9", "pythia-el8", "cmssw12-sim-el8"),
    "ihep": ("utilities-el9", "cmssw12-sim-el8", "cmssw15-sim-el9",
             "cmssw15-ntuple-el9", "analyzer-el9"),
}
# Never recursively bundle generated/, tests/, inventories, Git data, or packages/.
CORE_PAYLOAD = (
    "mcprod", "src/full_mc", "common/node_config_defaults.json",
    "common/campaigns/catalog.json", "common/profiles", "common/cmssw_configs",
    "common/compression_helpers.sh", "common/compression_util.py",
    "common/octet_pdg.py", "processing/run_chain.sh",
    "processing/cmssw_helpers", "processing/pythia_shower",
    "lhe_generation/run_helac.sh", "lhe_generation/input_templates",
    "tools/plan_lhe_blocks.py", "lhe_generation/lhe_shuffle_split.cc",
)


def validate_cmssw15_runtime(path):
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(path)
    with tarfile.open(path, "r:gz") as archive:
        names = tuple(m.name.lstrip("./").rstrip("/") + "/" for m in archive)
    missing = [entry for entry in CMSSW15_REQUIRED
               if not any(name.startswith(entry) for name in names)]
    if missing:
        raise ValueError("CMSSW15 runtime missing required paths: " + ", ".join(missing))


def validate_cmssw15_source_package(path):
    """Check the cheap source fallback contract without attempting a build."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(path)
    with tarfile.open(path, "r:gz") as archive:
        names = tuple(member.name.lstrip("./").rstrip("/") + "/" for member in archive)
    required = "HeavyFlavorAnalysis/TPS-Onia2MuMu/"
    if not any(name.startswith(required) for name in names):
        raise ValueError("CMSSW15 source package lacks " + required)


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def runtime_identity(release, git_revision):
    return {"runtime_release": release, "git_revision": git_revision}


def _files(root, paths):
    for relative in paths:
        path = root / relative
        if not path.exists():
            raise FileNotFoundError(path)
        candidates = sorted(path.rglob("*")) if path.is_dir() else [path]
        for candidate in candidates:
            parts = candidate.relative_to(root).parts
            if any(p in (".git", "__pycache__", ".pytest_cache") for p in parts):
                continue
            if candidate.is_symlink():
                raise ValueError(f"source payload contains a symlink: {candidate}")
            if candidate.is_file() and candidate.suffix not in (".pyc", ".o"):
                yield candidate


def _bundle(root, paths, destination, prefix="core"):
    with tarfile.open(destination, "w:gz", compresslevel=1) as archive:
        for path in _files(root, paths):
            archive.add(path, arcname=str(Path(prefix) / path.relative_to(root)),
                        recursive=False)


def _package_tpsonia2mumu_source(core, destination):
    """Create the documented source fallback used by ``run_chain.sh``."""
    source = Path(core) / "external/TPS-Onia2MuMu"
    if not source.is_dir():
        raise FileNotFoundError("TPS-Onia2MuMu submodule is not initialized: %s" % source)
    try:
        files = subprocess.check_output(["git", "-C", str(source), "ls-files"], text=True).splitlines()
    except subprocess.CalledProcessError as exc:
        raise ValueError("TPS-Onia2MuMu must be an initialized git submodule") from exc
    if not files:
        raise ValueError("TPS-Onia2MuMu submodule is not initialized")
    destination = Path(destination)
    with tarfile.open(destination, "w:gz", compresslevel=1) as archive:
        for relative in files:
            path = source / relative
            if not path.is_file() or path.is_symlink():
                raise ValueError("invalid TPS-Onia2MuMu source member: %s" % path)
            archive.add(path, arcname=str(Path("HeavyFlavorAnalysis/TPS-Onia2MuMu") / relative),
                        recursive=False)
    validate_cmssw15_source_package(destination)


def validate_runtime_release(release_dir, *, expected_release=None, expected_site=None):
    """Validate every descriptor-referenced release asset before use.

    This is existence and metadata validation, not per-worker content hashing.
    It catches moved, incomplete, or incorrectly paired releases before a DAG
    or IHEP batch can reference them.
    """
    root = Path(release_dir).resolve()
    descriptor_path = root / "runtime.json"
    try:
        descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid runtime descriptor %s: %s" % (descriptor_path, exc)) from exc
    if not isinstance(descriptor, dict):
        raise ValueError("runtime descriptor must be an object")
    if expected_release and descriptor.get("runtime_release") != expected_release:
        raise ValueError("runtime release does not match the requested release")
    if expected_site and descriptor.get("site") != expected_site:
        raise ValueError("runtime site does not match the requested site")
    profiles, bundles = descriptor.get("profiles"), descriptor.get("bundles")
    if (not isinstance(profiles, list) or not profiles or not isinstance(bundles, dict)
            or not isinstance(descriptor.get("git_revision"), str) or not descriptor["git_revision"]):
        raise ValueError("runtime descriptor lacks profiles, bundles, or git revision")
    if "core" not in bundles:
        raise ValueError("runtime descriptor lacks the core bundle")
    recorded_sizes = descriptor.get("bundle_bytes", {})
    if recorded_sizes is not None and not isinstance(recorded_sizes, dict):
        raise ValueError("runtime bundle_bytes must be an object")
    for key, name in bundles.items():
        if not isinstance(key, str) or not isinstance(name, str) or Path(name).name != name:
            raise ValueError("runtime bundle names must be safe basenames")
        asset = root / name
        if not asset.is_file() or asset.stat().st_size <= 0:
            raise FileNotFoundError("missing runtime asset: %s" % asset)
        if name in recorded_sizes and int(recorded_sizes[name]) != asset.stat().st_size:
            raise ValueError("runtime asset size does not match descriptor: %s" % name)
    controller = root / "controller/core"
    if not (controller / "mcprod").is_file() or not (controller / "src/full_mc/runtime/bootstrap.py").is_file():
        raise ValueError("runtime release lacks its frozen submit-host controller")
    cmssw_asset = bundles.get("cmssw15-ntuple-el9") or bundles.get("cmssw15-sim-el9") \
        or bundles.get("cmssw15-el9")
    if cmssw_asset:
        asset = root / cmssw_asset
        if cmssw_asset == CMSSW15_RUNTIME:
            validate_cmssw15_runtime(asset)
        elif cmssw_asset == CMSSW15_SOURCE_PACKAGE:
            validate_cmssw15_source_package(asset)
        else:
            raise ValueError("unrecognized CMSSW15 runtime asset: %s" % cmssw_asset)
    if "helac-el7" in bundles:
        if bundles["helac-el7"] != HELAC_RUNTIME_PACKAGE:
            raise ValueError("helac-el7 must use the stable runtime package basename")
        metadata = descriptor.get("helac_package")
        expected = {
            "source": HELAC_SOURCE_PACKAGE,
            "runtime_name": HELAC_RUNTIME_PACKAGE,
            "sha256": HELAC_PACKAGE_SHA256,
        }
        if metadata != expected:
            raise ValueError("runtime descriptor has invalid HELAC package provenance")
        asset = root / HELAC_RUNTIME_PACKAGE
        if _sha256(asset) != HELAC_PACKAGE_SHA256:
            raise ValueError("HELAC runtime package sha256 does not match published artifact")
    return descriptor


def prepare_runtime(core, destination, release, site, profiles=None):
    """Publish a new immutable release directory; no per-job package copies."""
    core, destination = Path(core).resolve(), Path(destination).resolve()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", release):
        raise ValueError("runtime release must be a path-safe identifier")
    if site not in SITE_PROFILES:
        raise ValueError(f"unknown runtime site: {site}")
    selected = list(dict.fromkeys(profiles or SITE_PROFILES[site]))
    if any(p not in PROFILES or p not in SITE_PROFILES[site] for p in selected):
        raise ValueError(f"profiles must belong to the {site} runtime set")
    if destination.exists():
        raise FileExistsError(f"runtime release already exists: {destination}")
    revision = subprocess.check_output(
        ["git", "-C", str(core), "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(
        ["git", "-C", str(core), "status", "--porcelain",
         "--untracked-files=normal"], text=True).strip())
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".runtime-", dir=destination.parent) as temporary:
        staging = Path(temporary) / "release"
        staging.mkdir()
        _bundle(core, CORE_PAYLOAD, staging / "core.tar.gz")
        from .bootstrap import _extract
        _extract(staging / "core.tar.gz", staging / "controller", "core")
        bundles = {"core": "core.tar.gz"}
        if "helac-el7" in selected:
            source = core / "common/packages" / HELAC_SOURCE_PACKAGE
            if not source.is_file() or source.stat().st_size <= 0:
                raise FileNotFoundError(source)
            if _sha256(source) != HELAC_PACKAGE_SHA256:
                raise ValueError("HELAC source package sha256 does not match published artifact")
            runtime_asset = staging / HELAC_RUNTIME_PACKAGE
            shutil.copy2(source, runtime_asset)
            if _sha256(runtime_asset) != HELAC_PACKAGE_SHA256:
                raise ValueError("copied HELAC runtime package sha256 does not match published artifact")
            bundles["helac-el7"] = HELAC_RUNTIME_PACKAGE
        cmssw15_profiles = [name for name in
                            ("cmssw15-ntuple-el9", "cmssw15-sim-el9", "cmssw15-el9")
                            if name in selected]
        if cmssw15_profiles:
            source = core / "common/packages" / CMSSW15_RUNTIME
            if source.is_file():
                validate_cmssw15_runtime(source)
                shutil.copy2(source, staging / source.name)
                bundles.update({profile: source.name for profile in cmssw15_profiles})
            else:
                fallback = staging / CMSSW15_SOURCE_PACKAGE
                _package_tpsonia2mumu_source(core, fallback)
                bundles.update({profile: fallback.name for profile in cmssw15_profiles})
        analyzer_revision = None
        if "analyzer-el9" in selected:
            analyzer = core / "external/NtupleAnalyzer"
            files = subprocess.check_output(
                ["git", "-C", str(analyzer), "ls-files"], text=True).splitlines()
            if not files:
                raise ValueError("NtupleAnalyzer submodule is not initialized")
            _bundle(analyzer, files, staging / "analyzer.tar.gz", "NtupleAnalyzer")
            analyzer_revision = subprocess.check_output(
                ["git", "-C", str(analyzer), "rev-parse", "HEAD"], text=True).strip()
            bundles["analyzer-el9"] = "analyzer.tar.gz"
        record = dict(runtime_identity(release, revision), site=site,
                      source_dirty=dirty, profiles=selected, bundles=bundles,
                      bundle_bytes={name: (staging / name).stat().st_size
                                    for name in bundles.values()})
        if "helac-el7" in selected:
            record["helac_package"] = {
                "source": HELAC_SOURCE_PACKAGE,
                "runtime_name": HELAC_RUNTIME_PACKAGE,
                "sha256": HELAC_PACKAGE_SHA256,
            }
        if analyzer_revision:
            record["analyzer_revision"] = analyzer_revision
        atomic_json(staging / "runtime.json", record)
        validate_runtime_release(staging, expected_release=release, expected_site=site)
        os.rename(staging, destination)
    return dict(record, directory=str(destination))
