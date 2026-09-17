"""Submit-host X.509 credential resolution for CERN DAGMan runs."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
from typing import Mapping


AFS_ROOT = Path("/afs")


def _persistent_afs_path(path: Path) -> bool:
    """Return whether ``path`` is on the submit-visible persistent AFS mount."""
    return path == AFS_ROOT or AFS_ROOT in path.parents


def _validate_proxy_path(candidate: str) -> tuple[Path | None, str | None]:
    try:
        path = Path(candidate).expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        return None, f"cannot resolve {candidate!r}: {exc}"
    if not path.is_file():
        return None, f"not a regular file: {path}"
    if not os.access(path, os.R_OK):
        return None, f"not readable: {path}"
    if path.stat().st_size <= 0:
        return None, f"empty: {path}"
    if not _persistent_afs_path(path):
        return None, f"not a persistent AFS path (refusing /tmp or /eos credential): {path}"
    return path, None


def _voms_proxy_path(runner) -> str | None:
    try:
        result = runner(["voms-proxy-info", "--path"], check=False, text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode:
        return None
    value = result.stdout.strip()
    return value or None


def resolve_proxy_path(*, environ: Mapping[str, str] | None = None,
                       runner=subprocess.run) -> str:
    """Resolve a usable persistent proxy, preferring ``X509_USER_PROXY``.

    DAGMan PRE scripts run on the submit host for the lifetime of the DAG.  Their
    credential must therefore be an existing readable, nonempty AFS file rather
    than the host-local ``/tmp`` proxy or an EOS path.
    """
    environment = os.environ if environ is None else environ
    failures = []
    configured = environment.get("X509_USER_PROXY")
    if configured:
        path, error = _validate_proxy_path(configured)
        if path is not None:
            return str(path)
        failures.append(f"X509_USER_PROXY: {error}")

    discovered = _voms_proxy_path(runner)
    if discovered and discovered != configured:
        path, error = _validate_proxy_path(discovered)
        if path is not None:
            return str(path)
        failures.append(f"voms-proxy-info --path: {error}")
    detail = "; ".join(failures) if failures else "no X509_USER_PROXY or voms-proxy-info proxy"
    raise RuntimeError("CERN DAGMan requires an existing readable nonempty persistent AFS X.509 proxy: "
                       + detail)
