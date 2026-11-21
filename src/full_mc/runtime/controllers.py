"""Immutable submit-host controller builds and reversible run launchers.

Controller activation changes only ``controller.sh`` and files beneath the
run's private ``.controller-switch`` directory. It does not change the worker
runtime identity, run manifest, DAGs, or state ledger.
"""
from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import tempfile

from . import packages
from .bootstrap import _extract
from full_mc.planning.graph import path_text
from full_mc.planning.script_defer import validate_script_defer_seconds

BUILD_SCHEMA = "mcprod-controller-build-v1"
SWITCH_MARKER = "# MCPROD_CONTROLLER_SWITCH_V1 "
LOCK_NAME = ".controller-switch.lock"
BACKUP_DIR = ".controller-switch/backups"
_BUILD_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_json_sync(path: Path, payload: dict) -> None:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    with path.open("x", encoding="utf-8") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _git_snapshot(core: Path) -> tuple[str, list[str]]:
    if not core.is_dir():
        raise ValueError(f"controller source core is not a directory: {core}")
    env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
    try:
        revision = subprocess.check_output(
            ["git", "-C", str(core), "rev-parse", "--verify", "HEAD^{commit}"],
            text=True, env=env, stderr=subprocess.PIPE,
        ).strip()
        status = subprocess.check_output(
            ["git", "-C", str(core), "status", "--porcelain", "--untracked-files=normal"],
            text=True, env=env, stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        raise ValueError(f"cannot identify controller source revision: {detail.strip()}") from exc
    if not re.fullmatch(r"[0-9a-fA-F]{40,64}", revision):
        raise ValueError("controller source HEAD is not a full commit ID")
    return revision, status.splitlines()


def _sync_tree(root: Path) -> None:
    directories = []
    for current, dirs, files in os.walk(root):
        current_path = Path(current)
        directories.append(current_path)
        for name in files:
            path = current_path / name
            if path.is_symlink():
                raise ValueError(f"controller build contains a symlink: {path}")
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
    for directory in reversed(directories):
        _fsync_dir(directory)


def prepare_controller(core, destination, build_id):
    """Build an immutable controller release from the shared core allowlist.

    ``destination`` is the exact, previously absent build directory. The
    existing worker-runtime builder and payload allowlist define what code is
    included; runtime packages and run data are never copied.
    """
    core = Path(core).expanduser().resolve()
    destination = Path(destination).expanduser().resolve()
    if not _BUILD_ID_RE.fullmatch(str(build_id)):
        raise ValueError("controller build ID must be a path-safe identifier")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"controller build already exists: {destination}")
    if not (core / "mcprod").is_file():
        raise FileNotFoundError(core / "mcprod")
    revision, source_status = _git_snapshot(core)
    destination.parent.mkdir(parents=True, exist_ok=True)

    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.build-",
                                    dir=destination.parent))
    published = False
    try:
        bundle = staging / "controller-core.tar.gz"
        packages._bundle(core, packages.CORE_PAYLOAD, bundle)
        payload_sha256 = _sha256_file(bundle)
        with bundle.open("rb") as handle:
            os.fsync(handle.fileno())
        _extract(bundle, staging / "controller", "core")
        build = {
            "schema": BUILD_SCHEMA,
            "build_id": str(build_id),
            "git_revision": revision,
            "source_core": str(core),
            "source_dirty": bool(source_status),
            "source_status": source_status,
            "payload": list(packages.CORE_PAYLOAD),
            "payload_sha256": payload_sha256,
            "entry_point": "controller/core/mcprod",
            "worker_runtime_release_policy": "preserve-run-json-runtime-release",
        }
        _write_json_sync(staging / "controller.json", build)
        _sync_tree(staging)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f"controller build already exists: {destination}")
        os.rename(staging, destination)
        published = True
        try:
            _fsync_dir(destination.parent)
        except OSError:
            # rename may be visible even when the directory sync reports an
            # error. Resolve the outcome from the published build record.
            try:
                current = _read_build(destination)
            except (OSError, ValueError) as inspect_error:
                raise OSError(
                    f"controller build may already be visible at {destination}; "
                    "inspect the destination and retry preparation"
                ) from inspect_error
            if current["build_id"] != str(build_id) or current["git_revision"] != revision:
                raise OSError(
                    f"controller build may already be visible at {destination}, but its "
                    "record differs from this preparation; inspect before retrying"
                )
            try:
                _fsync_dir(destination.parent)
            except OSError as retry_error:
                raise OSError(
                    f"controller build is visible at {destination}, but its parent "
                    "directory fsync failed; inspect the build and retry the operation"
                ) from retry_error
            build["directory_sync_retried"] = True
        return dict(build, directory=str(destination))
    finally:
        if not published and staging.exists():
            import shutil
            shutil.rmtree(staging)


def _read_build(build_dir) -> dict:
    build_dir = Path(build_dir).expanduser().resolve()
    if not build_dir.is_dir() or build_dir.is_symlink():
        raise ValueError(f"controller build directory is absent or unsafe: {build_dir}")
    try:
        record = json.loads((build_dir / "controller.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid controller build record at {build_dir}: {exc}") from exc
    if (not isinstance(record, dict) or record.get("schema") != BUILD_SCHEMA
            or not isinstance(record.get("build_id"), str)
            or not _BUILD_ID_RE.fullmatch(record["build_id"])
            or not isinstance(record.get("git_revision"), str)
            or not isinstance(record.get("source_dirty"), bool)
            or record.get("payload") != list(packages.CORE_PAYLOAD)
            or not isinstance(record.get("payload_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", record["payload_sha256"])
            or record.get("entry_point") != "controller/core/mcprod"):
        raise ValueError(f"invalid controller build metadata at {build_dir}")
    bundle = build_dir / "controller-core.tar.gz"
    if bundle.is_symlink() or not bundle.is_file():
        raise ValueError(f"controller build payload bundle is missing or unsafe: {bundle}")
    if _sha256_file(bundle) != record["payload_sha256"]:
        raise ValueError(f"controller build payload checksum does not match: {bundle}")
    core = build_dir / "controller/core"
    entry = core / "mcprod"
    if not entry.is_file() or not os.access(entry, os.X_OK):
        raise ValueError(f"controller build entry point is missing or not executable: {entry}")
    if not (core / "src/full_mc/cli.py").is_file():
        raise ValueError(f"controller build lacks full_mc CLI source: {core}")
    return dict(record, directory=str(build_dir), core_root=str(core))


def _run_metadata(run_dir) -> tuple[Path, dict]:
    root = Path(run_dir).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"run directory does not exist: {root}")
    try:
        metadata = json.loads((root / "run.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read run metadata in {root}: {exc}") from exc
    if not isinstance(metadata, dict) or metadata.get("orchestration") != "campaign_shards":
        raise ValueError("controller switching requires a campaign_shards run.json")
    from full_mc.application.staging import FUSED_STATE_SCHEMA
    if metadata.get("state_schema") == FUSED_STATE_SCHEMA:
        raise ValueError("fused campaign shards use their pinned runtime controller")
    for key in ("runtime_release", "core_revision", "core_root"):
        if not isinstance(metadata.get(key), str) or not metadata[key]:
            raise ValueError(f"campaign_shards run.json lacks {key}")
    if not (root / "run.dag").is_file():
        raise ValueError(f"run directory lacks its frozen root DAG: {root / 'run.dag'}")
    return root, metadata


def _launcher_path(root: Path) -> Path:
    path = root / "controller.sh"
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"controller launcher is absent or not a regular file: {path}")
    mode = stat.S_IMODE(path.stat().st_mode)
    if not mode & 0o111:
        raise ValueError(f"controller launcher is not executable: {path}")
    return path


def _original_launcher(root: Path, run_metadata: dict) -> bytes:
    core = Path(run_metadata["core_root"]).expanduser().resolve()
    entry = core / "mcprod"
    if not entry.is_file():
        raise ValueError(f"pinned original controller entry is missing: {entry}")
    try:
        root_text = path_text(root)
        log_text = path_text(root / "logs" / "controller.log")
        entry_text = shlex.quote(path_text(entry))
    except ValueError as exc:
        raise ValueError(f"run paths are not compatible with the generated launcher: {exc}") from exc
    text = (
        "#!/bin/bash\nset -euo pipefail\nexport PATH=/usr/bin:/bin\n"
        'phase="${1:?missing controller phase}"\nshift\n'
        f"exec >>{shlex.quote(log_text)} 2>&1\n"
        f'exec {entry_text} internal "$phase" --run-dir {shlex.quote(root_text)} "$@"\n'
    )
    return text.encode("utf-8")


def _metadata_comment(metadata: dict) -> str:
    raw = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw).decode("ascii")
    return SWITCH_MARKER + encoded


def _managed_launcher(root: Path, metadata: dict) -> bytes:
    build = Path(metadata["build_dir"]).resolve()
    entry = build / "controller/core/mcprod"
    root_text = str(root)
    log_text = str(root / "logs" / "controller.log")
    lines = [
        "#!/bin/bash",
        _metadata_comment(metadata),
        "set -euo pipefail",
        "export PATH=/usr/bin:/bin",
        f"export MCPROD_CONTROLLER_BUILD={shlex.quote(metadata['build_id'])}",
        "unset MCPROD_CONTROL_EVENTS MCPROD_ADMISSION_HINTS MCPROD_PRODUCER_INDEX",
    ]
    if "plan_accept_post_seconds" in metadata:
        lines.append(
            "unset MCPROD_PLAN_ACCEPT_POST_SECONDS MCPROD_PLAN_ACCEPT_POST_RUN_DIR"
        )
        if metadata["plan_accept_post_seconds"] is not None:
            lines.append(
                "export MCPROD_PLAN_ACCEPT_POST_SECONDS="
                + str(metadata["plan_accept_post_seconds"])
            )
            lines.append(
                "export MCPROD_PLAN_ACCEPT_POST_RUN_DIR="
                + shlex.quote(metadata["run_dir"])
            )
    if metadata["control_events"]:
        lines.append("export MCPROD_CONTROL_EVENTS=1")
    if metadata["admission_hints"]:
        lines.append("export MCPROD_ADMISSION_HINTS=1")
    if metadata["producer_index"]:
        lines.append("export MCPROD_PRODUCER_INDEX=1")
    lines += [
        'phase="${1:?missing controller phase}"',
        "shift",
        f"exec >>{shlex.quote(log_text)} 2>&1",
    ]
    if metadata.get("maintenance_pause", False):
        lines += [
            'printf "controller maintenance pause: phase=%s; deferring with exit 75\\n" "$phase" >&2',
            "exit 75",
        ]
    else:
        lines.append(
            f'exec {shlex.quote(str(entry))} internal "$phase" --run-dir '
            f"{shlex.quote(root_text)} \"$@\""
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _managed_metadata(data: bytes) -> dict | None:
    try:
        lines = data.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        return None
    if len(lines) < 2 or not lines[1].startswith(SWITCH_MARKER):
        return None
    try:
        raw = base64.b64decode(lines[1][len(SWITCH_MARKER):], altchars=b"-_", validate=True)
        value = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("controller launcher has a malformed switch marker")
    if not isinstance(value, dict):
        raise ValueError("controller launcher switch marker is not an object")
    return value


def _backup_path_valid(root: Path, path_value: str) -> Path:
    if not isinstance(path_value, str) or not path_value:
        raise ValueError("managed launcher backup path is invalid")
    base = (root / BACKUP_DIR).resolve()
    path = Path(path_value)
    if not path.is_absolute() or path.parent.resolve() != base:
        raise ValueError("managed launcher backup path is outside its private backup directory")
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"managed launcher backup is missing or unsafe: {path}")
    return path


def _validate_managed(root: Path, run_metadata: dict, data: bytes) -> dict:
    metadata = _managed_metadata(data)
    if metadata is None:
        raise ValueError("controller launcher is not a managed controller release")
    required = {
        "schema", "run_dir", "runtime_release", "original_core_root", "build_id",
        "build_dir", "control_events", "previous_launcher", "previous_sha256",
        "previous_mode", "admission_hints", "producer_index",
    }
    if not required.issubset(metadata) or metadata.get("schema") != "mcprod-controller-switch-v1":
        raise ValueError("managed controller launcher metadata is incomplete or unsupported")
    for key in ("run_dir", "runtime_release", "original_core_root", "build_id",
                "build_dir", "previous_launcher", "previous_sha256"):
        if not isinstance(metadata[key], str) or not metadata[key]:
            raise ValueError(f"managed controller launcher has invalid {key}")
    if (metadata["run_dir"] != str(root)
            or metadata["runtime_release"] != run_metadata["runtime_release"]
            or metadata["original_core_root"] != run_metadata["core_root"]
            or not isinstance(metadata["control_events"], bool)
            or not isinstance(metadata["admission_hints"], bool)
            or not isinstance(metadata["producer_index"], bool)
            or ("maintenance_pause" in metadata
                and not isinstance(metadata["maintenance_pause"], bool))
            or type(metadata["previous_mode"]) is not int
            or not 0 <= metadata["previous_mode"] <= 0o777
            or not re.fullmatch(r"[0-9a-f]{64}", metadata["previous_sha256"])):
        raise ValueError("managed launcher does not match the frozen run identity")
    if "plan_accept_post_seconds" in metadata:
        value = metadata["plan_accept_post_seconds"]
        if value is not None:
            validate_script_defer_seconds(value, "managed launcher PLAN POST interval")
    build = _read_build(metadata["build_dir"])
    if metadata["build_id"] != build["build_id"]:
        raise ValueError("managed launcher build identity disagrees with its build record")
    backup = _backup_path_valid(root, metadata["previous_launcher"])
    if _sha256(backup.read_bytes()) != metadata["previous_sha256"]:
        raise ValueError("managed launcher backup checksum does not match")
    if _managed_launcher(root, metadata) != data:
        raise ValueError("managed controller launcher was edited after activation; refusing to replace it")
    return dict(metadata, build=build, backup=str(backup))


def _inspect(root: Path, run_metadata: dict) -> dict:
    path = _launcher_path(root)
    data = path.read_bytes()
    managed_marker = _managed_metadata(data)
    if managed_marker is not None:
        current = _validate_managed(root, run_metadata, data)
        return {
            "run_dir": str(root), "launcher": str(path), "managed": True,
            "controller_build_id": current["build_id"],
            "controller_build_dir": current["build_dir"],
            "git_revision": current["build"]["git_revision"],
            "source_dirty": current["build"]["source_dirty"],
            "control_events": current["control_events"],
            "admission_hints": current["admission_hints"],
            "producer_index": current["producer_index"],
            "maintenance_pause": current.get("maintenance_pause", False),
            "plan_accept_post_seconds": current.get("plan_accept_post_seconds"),
            "previous_launcher_backup": current["backup"],
            "runtime_release": run_metadata["runtime_release"],
            "live_processes_verified": False,
        }
    expected = _original_launcher(root, run_metadata)
    if data != expected:
        raise ValueError(
            f"unrecognized controller launcher at {path}; expected the generated "
            "campaign_shards launcher or an unmodified mcprod-managed launcher"
        )
    return {
        "run_dir": str(root), "launcher": str(path), "managed": False,
        "controller_build_id": None, "controller_build_dir": run_metadata["core_root"],
        "git_revision": run_metadata["core_revision"], "source_dirty": None,
        "control_events": False, "admission_hints": False, "producer_index": False,
        "maintenance_pause": False,
        "plan_accept_post_seconds": None,
        "previous_launcher_backup": None,
        "runtime_release": run_metadata["runtime_release"],
        "live_processes_verified": False,
    }


def _write_backup(root: Path, previous: bytes, mode: int) -> Path:
    backup_dir = root / BACKUP_DIR
    backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    _fsync_dir(root)
    _fsync_dir(backup_dir.parent)
    digest = _sha256(previous)
    base = backup_dir / f"launcher-{digest[:20]}.sh"
    for suffix in range(1000):
        candidate = base if suffix == 0 else backup_dir / f"launcher-{digest[:20]}-{suffix}.sh"
        try:
            fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode & 0o777)
        except FileExistsError:
            if candidate.is_file() and not candidate.is_symlink():
                if candidate.read_bytes() == previous and stat.S_IMODE(candidate.stat().st_mode) == mode:
                    with candidate.open("rb") as handle:
                        os.fsync(handle.fileno())
                    _fsync_dir(backup_dir)
                    return candidate
                continue
            continue
        with os.fdopen(fd, "wb") as handle:
            handle.write(previous)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_dir(backup_dir)
        return candidate
    raise FileExistsError("could not allocate a unique controller launcher backup")


def _atomic_launcher_replace(path: Path, data: bytes, mode: int) -> dict:
    fd, temporary = tempfile.mkstemp(prefix=".controller.sh.switch-", dir=path.parent)
    temporary = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fchmod(handle.fileno(), mode & 0o777)
            os.fsync(handle.fileno())
        replace_returned = False
        try:
            os.replace(temporary, path)
            replace_returned = True
            _fsync_dir(path.parent)
            return {"recovered_after_replace": False, "durability_warning": False}
        except OSError as exc:
            try:
                visible = path.read_bytes() == data
            except OSError as inspect_error:
                if replace_returned or not temporary.exists():
                    raise OSError(
                        f"controller launcher replacement may already be visible at {path}; "
                        "run controller status and retry"
                    ) from inspect_error
                raise exc
            if not visible:
                if replace_returned or not temporary.exists():
                    raise OSError(
                        f"controller launcher replacement may already be visible at {path}, "
                        "but the current pointer differs; run controller status before retrying"
                    ) from exc
                raise
            try:
                _fsync_dir(path.parent)
                return {"recovered_after_replace": True, "durability_warning": False}
            except OSError as retry_error:
                raise OSError(
                    f"controller launcher replacement is visible at {path}, but its "
                    "parent directory fsync failed; run controller status and retry"
                ) from retry_error
    finally:
        if temporary.exists():
            temporary.unlink()


def _switch_lock(root: Path):
    path = root / LOCK_NAME
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    handle = os.fdopen(fd, "r+b")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    return handle


def activate_controller(run_dir, build_dir, *, control_events=False,
                        admission_hints=False, producer_index=False,
                        plan_accept_post_seconds=None, maintenance_pause=False) -> dict:
    if plan_accept_post_seconds is not None:
        plan_accept_post_seconds = validate_script_defer_seconds(
            plan_accept_post_seconds, "plan_accept_post_seconds"
        )
    root, run_metadata = _run_metadata(run_dir)
    build = _read_build(build_dir)
    launcher = _launcher_path(root)
    with _switch_lock(root) as lock:
        del lock  # Keep the stable lock inode open until the switch completes.
        previous = launcher.read_bytes()
        previous_mode = stat.S_IMODE(launcher.stat().st_mode)
        active = _managed_metadata(previous)
        if active is not None:
            current = _validate_managed(root, run_metadata, previous)
            if (current["build_id"] == build["build_id"]
                    and current["build_dir"] == build["directory"]
                    and current["control_events"] is bool(control_events)
                    and current["admission_hints"] is bool(admission_hints)
                    and current["producer_index"] is bool(producer_index)
                    and current.get("maintenance_pause", False) is bool(maintenance_pause)
                    and current.get("plan_accept_post_seconds") == plan_accept_post_seconds):
                try:
                    _fsync_dir(root)
                except OSError as exc:
                    raise OSError(
                        f"managed controller launcher is visible at {launcher}, but its "
                        "parent directory fsync could not be reestablished; run controller "
                        "status and retry"
                    ) from exc
                return dict(_inspect(root, run_metadata), idempotent=True,
                            recovered_after_replace=False, durability_warning=False)
        else:
            original = _original_launcher(root, run_metadata)
            if previous != original:
                raise ValueError(
                    f"unrecognized controller launcher at {launcher}; refusing activation. "
                    "Restore the generated campaign_shards launcher or review it manually."
                )

        backup = _write_backup(root, previous, previous_mode)
        metadata = {
            "schema": "mcprod-controller-switch-v1",
            "run_dir": str(root),
            "runtime_release": run_metadata["runtime_release"],
            "original_core_root": run_metadata["core_root"],
            "build_id": build["build_id"],
            "build_dir": build["directory"],
            "control_events": bool(control_events),
            "admission_hints": bool(admission_hints),
            "producer_index": bool(producer_index),
            "maintenance_pause": bool(maintenance_pause),
            "plan_accept_post_seconds": plan_accept_post_seconds,
            "previous_launcher": str(backup),
            "previous_sha256": _sha256(previous),
            "previous_mode": previous_mode,
        }
        result = _atomic_launcher_replace(
            launcher, _managed_launcher(root, metadata), previous_mode
        )
        return dict(_inspect(root, run_metadata), idempotent=False, **result)


def rollback_controller(run_dir, *, expect_build=None) -> dict:
    root, run_metadata = _run_metadata(run_dir)
    launcher = _launcher_path(root)
    with _switch_lock(root) as lock:
        del lock
        current_bytes = launcher.read_bytes()
        metadata = _validate_managed(root, run_metadata, current_bytes)
        if expect_build and expect_build != metadata["build_id"]:
            raise ValueError(
                f"active controller build is {metadata['build_id']}, not expected {expect_build}"
            )
        previous_path = Path(metadata["backup"])
        previous = previous_path.read_bytes()
        previous_mode = metadata["previous_mode"]
        if _managed_metadata(previous) is None and previous != _original_launcher(root, run_metadata):
            raise ValueError("previous launcher backup is not a known generated launcher")
        if _managed_metadata(previous) is not None:
            _validate_managed(root, run_metadata, previous)
        result = _atomic_launcher_replace(launcher, previous, previous_mode)
        return dict(_inspect(root, run_metadata), rolled_back_build_id=metadata["build_id"],
                    idempotent=False, **result)


def controller_status(run_dir) -> dict:
    root, run_metadata = _run_metadata(run_dir)
    return _inspect(root, run_metadata)


def sync_producer_index(run_dir) -> dict:
    """Rebuild only the optional producer index under the staging state lock.

    Accepted receipt files remain authoritative. The nonblocking transaction
    lock ensures this snapshot cannot race a current state writer; a later
    legacy writer can make the count stale, which normal indexed reads detect.
    """
    from full_mc.application import producer_index, shards, staging

    root, run_metadata = _run_metadata(run_dir)
    if run_metadata.get("orchestration") != "campaign_shards":
        raise ValueError("producer index synchronization requires campaign_shards")
    try:
        with staging.transaction(root) as state:
            before = state.get("producer_index")
            rebuilt = producer_index.rebuild(root, state, shards._receipt)
            return {
                "run_dir": str(root),
                "status": "rebuilt" if rebuilt is not None else "ambiguous_index_cleared",
                "accepted_count": len(state["accepted"]),
                "indexed_artifact_count": len(rebuilt["artifacts"]) if rebuilt is not None else 0,
                "previous_index_present": isinstance(before, dict),
                "runtime_release": run_metadata["runtime_release"],
            }
    except staging.LockBusy:
        return {"run_dir": str(root), "exit_code": 75,
                "reason": "local state lock busy", "runtime_release": run_metadata["runtime_release"]}
