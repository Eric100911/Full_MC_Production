"""Disposable run specs and frozen runtime fixtures."""
from contextlib import ExitStack
from unittest.mock import patch

from full_mc.application.staging import FUSED_STATE_SCHEMA
from full_mc.io import atomic_json
from full_mc.storage.transfer import TransferClient
from tests.generate_synthetic_lhe import generate


REMOTE_METHODS = ("size", "mkdir_parent", "mkdir", "read_json_if_exists",
                  "directory_has_entries", "copy_json_if_absent", "copy_out",
                  "copy_in", "read_json", "remove")
def remote_io_blocked():
    """Every TransferClient operation raises; the seam must not call any of them."""
    stack = ExitStack()
    for name in REMOTE_METHODS:
        stack.enter_context(patch.object(
            TransferClient, name,
            side_effect=AssertionError("seam touched remote storage")))
    return stack


def run_spec_fixture(root):
    source = root / "input" / "input.lhe"
    source.parent.mkdir(parents=True)
    generate(str(source), 15)
    runtime = root / "runtime"
    (runtime / "controller/core/src/full_mc/runtime").mkdir(parents=True)
    (runtime / "controller/core/mcprod").write_text("#!/bin/sh\n", encoding="utf-8")
    (runtime / "controller/core/src/full_mc/runtime/bootstrap.py").write_text("# bootstrap\n", encoding="utf-8")
    for name in ("core.tgz", "utilities.tgz", "pythia.tgz", "cmssw12.tgz"):
        (runtime / name).write_bytes(b"bundle")
    atomic_json(runtime / "runtime.json", {
        "runtime_release": "test-release", "site": "cern", "git_revision": "deadbeef",
        "source_dirty": False, "cern_state_schema": FUSED_STATE_SCHEMA,
        "profiles": ["utilities-el9", "pythia-el8", "cmssw12-sim-el8"],
        "bundles": {"core": "core.tgz", "utilities-el9": "utilities.tgz",
                    "pythia-el8": "pythia.tgz", "cmssw12-sim-el8": "cmssw12.tgz"}})
    spec = {
        "schema_version": 2, "orchestration": "campaign_shards", "run_id": "offline", "runtime_release": "test-release",
        "runtime_dir": str(runtime),
        "block_events": 5,
        "storage": {"cern_staging": "root://eosuser.cern.ch///eos/user/c/chiw/staging",
                    "ihep_products": str(root / "ihep/products"),
                    "ihep_archive": str(root / "ihep/archive")},
        "limits": {"source_lhe_bytes": 1000, "source_block_bytes": 1000,
                   "mixed_hepmc_bytes": 1000},
        "scheduler": {"maxjobs": {"lhe": 3, "lhe_planning": 3,
                                    "block_processing": 2, "processing": 2,
                                    "lhe_coordination": 1}},
        "sources": [{"pool": "pool", "source_id": "s1", "seed": 19,
                     "url": str(source), "actual_events": 15,
                     "size_bytes": source.stat().st_size}],
        "campaigns": [{"name": "C", "analysis": "JJP", "target_mixed_events": 1,
                       "max_processing_nodes": 1, "merge_events": 1,
                       "detector_variants": ["run3_2022_legacy_premix_v1"],
                       "sources": [{"pool": "pool", "mode": "normal", "lhe_budget": 5},
                                   {"pool": "pool", "mode": "normal", "lhe_budget": 5}]}]}
    spec_path = root / "spec.json"
    atomic_json(spec_path, spec)
    return spec_path, root / "run"
