"""Run-namespace ownership markers for fresh CERN submissions."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from .transfer import TransferClient


MARKER_SCHEMA = "full-mc-storage-reservation-v1"


def reserve_run_namespace(client: TransferClient, root_url: str,
                          marker: Mapping[str, Any]) -> str:
    """Reserve one ``root_url/run_id`` directory idempotently."""
    run_id = str(marker.get("run_id", ""))
    if not run_id or "/" in run_id or ".." in run_id:
        raise ValueError("reservation marker requires a path-safe run_id")
    if marker.get("schema") != MARKER_SCHEMA:
        raise ValueError("unsupported storage reservation marker schema")
    run_root = str(root_url).rstrip("/") + "/" + run_id
    marker_url = run_root + "/.mcprod-run.json"
    existing = client.read_json_if_exists(marker_url)
    if existing is not None:
        if existing != dict(marker):
            raise RuntimeError("run_id is already reserved by a different run")
        return marker_url
    if client.directory_has_entries(run_root):
        raise RuntimeError("remote run directory is nonempty and has no owner marker")
    client.mkdir(run_root, parents=True)
    client.copy_json_if_absent(dict(marker), marker_url)
    if client.read_json_if_exists(marker_url) != dict(marker):
        raise RuntimeError("remote namespace reservation raced or was replaced")
    return marker_url


def reservation_marker(spec: Mapping[str, Any], spec_bytes: bytes) -> dict[str, Any]:
    """Build the marker from the frozen run specification and core revision."""
    return {
        "schema": MARKER_SCHEMA,
        "run_id": str(spec["run_id"]),
        "run_spec_sha256": hashlib.sha256(spec_bytes).hexdigest(),
        "core_revision": str(spec.get("core_revision", "")),
        "runtime_release": str(spec["runtime_release"]),
    }


def reserve_run_namespaces(spec: Mapping[str, Any], *, client: TransferClient | None = None,
                           spec_bytes: bytes | None = None) -> list[str]:
    """Reserve CERN staging, IHEP products, and IHEP archive roots."""
    storage = spec.get("storage", {})
    roots = [storage.get(name) for name in ("cern_staging", "ihep_products", "ihep_archive")]
    if any(not root for root in roots):
        raise ValueError("run reservation requires all three configured storage roots")
    if spec_bytes is None:
        spec_bytes = json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
    marker = reservation_marker(spec, spec_bytes)
    client = client or TransferClient()
    return [reserve_run_namespace(client, str(root), marker) for root in roots]
