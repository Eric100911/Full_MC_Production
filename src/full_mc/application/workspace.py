"""Create the small, operator-owned repository around a production core."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _git(*args, cwd=None):
    command = ["git", *map(str, args)]
    try:
        result = subprocess.run(command, cwd=cwd, env=dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull), check=True, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        raise RuntimeError(f"git command failed: {' '.join(command)}: {detail.strip()}") from exc
    return result.stdout.strip()


def _selected_revision(core: Path, revision: str | None) -> str:
    selected = revision or "HEAD"
    commit = _git("-C", core, "rev-parse", "--verify", f"{selected}^{{commit}}")
    try:
        _git("-C", core, "cat-file", "-e", f"{commit}:mcprod")
    except RuntimeError as exc:
        raise ValueError(
            f"core revision {commit} does not contain the new mcprod entry point"
        ) from exc
    return commit


def _write_initial_files(root: Path):
    (root / "campaigns").mkdir()
    (root / "history").mkdir()
    (root / ".gitignore").write_text(
        "work/\nruntimes/\ncredentials/\n", encoding="utf-8"
    )
    (root / "README.md").write_text(
        "# Full MC production workspace\n\n"
        "This repository records operator-owned campaign specifications and history.\n"
        "The production implementation is pinned in the `core` submodule.\n",
        encoding="utf-8",
    )
    (root / "campaigns" / "README.md").write_text(
        "# Campaigns\n\nPlace reviewed campaign specifications here.\n",
        encoding="utf-8",
    )
    (root / "campaigns" / "example.json").write_text(
        '{\n  "schema_version": 1,\n  "run_id": "example",\n  "runtime_release": "production-v1",\n  "runtime_dir": "../runtimes/production-v1/cern",\n  "block_events": 1000,\n  "limits": {\n    "source_lhe_bytes": 2000000000,\n    "source_block_bytes": 2000000000,\n    "mixed_hepmc_bytes": 2000000000,\n    "source_files_per_step": 1,\n    "processing_nodes_per_wave": 1\n  },\n  "sources": [\n    {\n      "pool": "pool_2jpsi_cs",\n      "source_id": "example-source",\n      "seed": 1,\n      "generation": {"max_events": 10000}\n    }\n  ],\n  "campaigns": [\n    {\n      "name": "JJP_SPS_CS",\n      "target_mixed_events": 1000,\n      "max_processing_nodes": 1,\n      "merge_events": 1000,\n      "sources": [\n        {"pool": "pool_2jpsi_cs", "mode": "phi_mpi_off", "lhe_budget": 1000}\n      ]\n    }\n  ]\n}\n', encoding="utf-8"
    )
    (root / "history" / "README.md").write_text(
        "# History\n\nRecord production decisions, handoffs, and completed runs here.\n",
        encoding="utf-8",
    )


def initialize(directory, core, revision=None):
    """Initialize *directory* as an uncommitted wrapper around a core revision."""
    target = Path(directory).expanduser().resolve()
    if target.exists() and (not target.is_dir() or any(target.iterdir())):
        raise ValueError("workspace target must be absent or empty")
    core_path = Path(core).expanduser().resolve()
    if not core_path.is_dir() or not (core_path / ".git").exists():
        raise ValueError("core must be a local Git working tree")
    commit = _selected_revision(core_path, revision)

    target.mkdir(parents=True, exist_ok=True)
    _git("init", target)
    _write_initial_files(target)
    # Restrict the file protocol allowance to this one submodule operation.
    _git("-c", "protocol.file.allow=always", "submodule", "add", str(core_path), "core",
         cwd=target)
    _git("-C", target / "core", "checkout", "--detach", commit)
    _git("add", ".", cwd=target)
    return {"workspace": str(target), "core": str(target / "core"),
            "core_revision": commit, "status": "initialized"}
