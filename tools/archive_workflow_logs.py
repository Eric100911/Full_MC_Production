#!/usr/bin/env python3
"""Archive staged workflow logs from a top-level DAGMan FINAL worker."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple
from urllib.parse import urlsplit


SCHEMA_VERSION = 1
STAGES = ("processing", "miniaod_merge", "ntuple", "final")
JOB_COMPONENT_RE = re.compile(r"^job_[0-9]+$")
RETRY_DELAYS_SECONDS = (5, 15, 30)
EXCLUDED_BASENAMES = {
    "log_archive_status.json",
}


class ArchiveError(RuntimeError):
    """An expected archival failure with an operator-facing phase."""

    def __init__(self, phase: str, message: str):
        super().__init__(message)
        self.phase = phase
        self.message = message


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_xrootd_url(url: str) -> Tuple[str, str]:
    parsed = urlsplit(url)
    if parsed.scheme != "root" or not parsed.netloc:
        raise ValueError(f"Invalid XRootD URL: {url}")
    endpoint = f"root://{parsed.netloc}/"
    remote_path = "/" + parsed.path.lstrip("/")
    return endpoint, remote_path


def join_target(base: str, *parts: str) -> str:
    clean_parts = [part.strip("/") for part in parts if part.strip("/")]
    return "/".join([base.rstrip("/"), *clean_parts])


def _is_excluded(path: Path) -> bool:
    name = path.name
    return (
        name in EXCLUDED_BASENAMES
        or name.startswith("workflow_log_archive_status_")
        or ".tmp." in name
    )


def discover_log_groups(log_root: Path) -> Dict[Tuple[str, str], Dict[str, Path]]:
    groups: MutableMapping[Tuple[str, str], Dict[str, Path]] = defaultdict(dict)
    if not log_root.is_dir():
        return {}
    for campaign_dir in sorted(log_root.iterdir(), key=lambda item: item.name):
        if (
            not campaign_dir.is_dir()
            or campaign_dir.is_symlink()
            or campaign_dir.name.startswith("_")
        ):
            continue
        for stage in STAGES:
            stage_dir = campaign_dir / stage
            if not stage_dir.is_dir() or stage_dir.is_symlink():
                continue
            for job_dir in sorted(stage_dir.iterdir(), key=lambda item: item.name):
                if (
                    job_dir.is_dir()
                    and not job_dir.is_symlink()
                    and JOB_COMPONENT_RE.fullmatch(job_dir.name)
                ):
                    groups[(campaign_dir.name, job_dir.name)][stage] = job_dir
    return dict(groups)


def collect_group_files(
    log_root: Path,
    stage_directories: Mapping[str, Path],
) -> Tuple[List[Tuple[Path, str]], Dict[str, Dict[str, object]], List[str]]:
    files: List[Tuple[Path, str]] = []
    stages: Dict[str, Dict[str, object]] = {}
    warnings: List[str] = []
    for stage in STAGES:
        stage_dir = stage_directories.get(stage)
        stage_files: List[Tuple[Path, str]] = []
        if stage_dir is not None:
            for root, directory_names, file_names in os.walk(stage_dir):
                directory_names[:] = sorted(
                    name
                    for name in directory_names
                    if not (Path(root) / name).is_symlink()
                )
                for name in sorted(file_names):
                    path = Path(root) / name
                    if path.is_symlink() or not path.is_file() or _is_excluded(path):
                        continue
                    relative = path.relative_to(log_root).as_posix()
                    stage_files.append((path, relative))
        if stage_dir is None:
            warnings.append(f"missing stage directory: {stage}")
        stages[stage] = {
            "present": stage_dir is not None,
            "file_count": len(stage_files),
            "size_bytes": sum(path.stat().st_size for path, _ in stage_files),
        }
        files.extend(stage_files)
    files.sort(key=lambda item: item[1])
    return files, stages, warnings


def create_deterministic_archive(
    archive_path: Path,
    files: Sequence[Tuple[Path, str]],
) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with archive_path.open("wb") as raw_handle:
        with gzip.GzipFile(
            filename="",
            fileobj=raw_handle,
            mode="wb",
            mtime=0,
        ) as gzip_handle:
            with tarfile.open(fileobj=gzip_handle, mode="w") as archive:
                for source, relative in files:
                    info = archive.gettarinfo(str(source), arcname=relative)
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.mtime = 0
                    with source.open("rb") as handle:
                        archive.addfile(info, handle)


def run_with_retries(
    action: Callable[[], None],
    *,
    attempts: int,
    delays: Sequence[int] = RETRY_DELAYS_SECONDS,
) -> None:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            action()
            return
        except Exception as exc:  # subprocess errors need the same retry policy
            last_error = exc
            if attempt >= attempts:
                break
            delay = delays[min(attempt - 1, len(delays) - 1)] if delays else 0
            print(
                f"WARN: archive operation failed on attempt {attempt}/{attempts}: "
                f"{exc}; retrying in {delay}s",
                file=sys.stderr,
                flush=True,
            )
            if delay > 0:
                time.sleep(delay)
    assert last_error is not None
    raise last_error


def _run(command: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess:
    print("+", " ".join(command), flush=True)
    return subprocess.run(
        list(command),
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def ensure_remote_directory(remote_dir: str, attempts: int) -> None:
    endpoint, remote_path = parse_xrootd_url(remote_dir)

    def action() -> None:
        proc = _run(["xrdfs", endpoint, "mkdir", "-p", remote_path], check=False)
        if proc.returncode != 0:
            raise RuntimeError(proc.stdout.strip() or f"xrdfs mkdir failed: {remote_dir}")

    run_with_retries(action, attempts=attempts)


def upload_file(local_path: Path, destination: str, attempts: int) -> None:
    if destination.startswith("root://"):
        endpoint, final_path = parse_xrootd_url(destination)
        part_path = f"{final_path}.part.{os.getpid()}"
        parsed = urlsplit(destination)
        part_url = f"root://{parsed.netloc}///{part_path.lstrip('/')}"

        def action() -> None:
            proc = _run(
                ["xrdcp", "--nopbar", "-f", str(local_path), part_url],
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(proc.stdout.strip() or f"xrdcp failed: {destination}")
            _run(["xrdfs", endpoint, "rm", final_path], check=False)
            proc = _run(
                ["xrdfs", endpoint, "mv", part_path, final_path],
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    proc.stdout.strip() or f"xrdfs mv failed: {destination}"
                )

        run_with_retries(action, attempts=attempts)
        return

    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    def local_action() -> None:
        temporary = destination_path.with_name(
            f"{destination_path.name}.part.{os.getpid()}"
        )
        shutil.copy2(local_path, temporary)
        os.replace(temporary, destination_path)

    run_with_retries(local_action, attempts=attempts)


def prepare_proxy(proxy_bundle: Path, workdir: Path) -> Tuple[Path, int]:
    if not proxy_bundle.is_file() or proxy_bundle.stat().st_size == 0:
        raise ArchiveError(
            "proxy_validation",
            f"proxy bundle is missing, unreadable, or empty: {proxy_bundle}",
        )
    credential_dir = workdir / "credential"
    credential_dir.mkdir(mode=0o700)
    try:
        with tarfile.open(proxy_bundle, "r:gz") as archive:
            member = archive.getmember("credentials/x509_user_proxy")
            extracted = archive.extractfile(member)
            if extracted is None:
                raise KeyError(member.name)
            proxy_path = credential_dir / "x509_user_proxy"
            with proxy_path.open("wb") as handle:
                shutil.copyfileobj(extracted, handle)
            os.chmod(proxy_path, 0o600)
    except (KeyError, tarfile.TarError, OSError) as exc:
        raise ArchiveError(
            "proxy_validation",
            f"failed to extract proxy bundle {proxy_bundle}: {exc}",
        ) from exc

    tool = shutil.which("voms-proxy-info")
    if not tool:
        raise ArchiveError(
            "proxy_validation",
            "voms-proxy-info is unavailable in the FINAL worker",
        )
    valid = _run(
        [tool, "--file", str(proxy_path), "--exists", "--valid", "0:10"],
        check=False,
    )
    if valid.returncode != 0:
        raise ArchiveError(
            "proxy_validation",
            "bundled proxy is expired or has less than 10 minutes remaining",
        )
    lifetime = _run(
        [tool, "--file", str(proxy_path), "--timeleft"],
        check=False,
    )
    try:
        timeleft = int(lifetime.stdout.strip())
    except ValueError as exc:
        raise ArchiveError(
            "proxy_validation",
            f"invalid bundled proxy lifetime: {lifetime.stdout.strip()}",
        ) from exc
    if lifetime.returncode != 0 or timeleft < 600:
        raise ArchiveError(
            "proxy_validation",
            "could not verify at least 10 minutes of bundled proxy lifetime",
        )
    os.environ["X509_USER_PROXY"] = str(proxy_path)
    return proxy_path, timeleft


def per_job_status_path(log_root: Path, campaign: str, job_component: str) -> Path:
    return log_root / campaign / "final" / job_component / "log_archive_status.json"


def make_failed_results(
    groups: Mapping[Tuple[str, str], Mapping[str, Path]],
    *,
    phase: str,
    message: str,
    workflow_id: str,
) -> List[Dict[str, object]]:
    return [
        {
            "schema_version": SCHEMA_VERSION,
            "workflow_archive_id": workflow_id,
            "campaign": campaign,
            "job_component": job_component,
            "status": "failed",
            "phase": phase,
            "message": message,
            "updated_at": utc_now(),
        }
        for campaign, job_component in sorted(groups)
    ]


def archive_workflow(
    *,
    log_root: Path,
    target_base_url: str,
    workflow_id: str,
    proxy_bundle: Path,
    retry_attempts: int,
) -> Dict[str, object]:
    groups = discover_log_groups(log_root)
    overall_path = (
        log_root
        / "_shared"
        / "summary"
        / f"workflow_log_archive_status_{workflow_id}.json"
    )
    base_status: Dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "workflow_archive_id": workflow_id,
        "log_root": str(log_root),
        "target_base_url": target_base_url,
        "retry_attempts": retry_attempts,
        "updated_at": utc_now(),
        "results": [],
    }
    if not groups:
        payload = {
            **base_status,
            "archive_status": "failed",
            "phase": "log_discovery",
            "message": f"no staged workflow logs found under {log_root}",
        }
        atomic_write_json(overall_path, payload)
        return payload

    with tempfile.TemporaryDirectory(prefix="workflow_log_archive_") as temporary:
        workdir = Path(temporary)
        try:
            _, proxy_timeleft = prepare_proxy(proxy_bundle, workdir)
        except ArchiveError as exc:
            results = make_failed_results(
                groups,
                phase=exc.phase,
                message=exc.message,
                workflow_id=workflow_id,
            )
            for result in results:
                atomic_write_json(
                    per_job_status_path(
                        log_root,
                        str(result["campaign"]),
                        str(result["job_component"]),
                    ),
                    result,
                )
            payload = {
                **base_status,
                "archive_status": "failed",
                "phase": exc.phase,
                "message": exc.message,
                "results": results,
            }
            atomic_write_json(overall_path, payload)
            return payload

        results: List[Dict[str, object]] = []
        for campaign, job_component in sorted(groups):
            files, stages, warnings = collect_group_files(
                log_root,
                groups[(campaign, job_component)],
            )
            result: Dict[str, object] = {
                "schema_version": SCHEMA_VERSION,
                "workflow_archive_id": workflow_id,
                "campaign": campaign,
                "job_component": job_component,
                "status": "failed",
                "phase": "archive_creation",
                "proxy_timeleft_seconds": proxy_timeleft,
                "stages": stages,
                "warnings": warnings,
                "updated_at": utc_now(),
            }
            if not files:
                result["message"] = "stage directories contained no log files"
                results.append(result)
                atomic_write_json(
                    per_job_status_path(log_root, campaign, job_component),
                    result,
                )
                continue

            archive_name = f"logs_{campaign}_{job_component}.tar.gz"
            archive_path = workdir / archive_name
            manifest_path = workdir / f"logs_{campaign}_{job_component}.json"
            try:
                create_deterministic_archive(archive_path, files)
                archive_size = archive_path.stat().st_size
                archive_sha256 = sha256_file(archive_path)
                remote_dir = join_target(
                    target_base_url,
                    "output",
                    campaign,
                    f"{job_component}_logs",
                    workflow_id,
                )
                archive_url = join_target(remote_dir, archive_name)
                manifest_url = join_target(remote_dir, manifest_path.name)
                manifest = {
                    **result,
                    "status": "ok",
                    "phase": "complete",
                    "message": "log archive uploaded",
                    "archive": {
                        "name": archive_name,
                        "url": archive_url,
                        "size_bytes": archive_size,
                        "sha256": archive_sha256,
                    },
                    "manifest_url": manifest_url,
                    "members": [
                        {"path": relative, "size_bytes": path.stat().st_size}
                        for path, relative in files
                    ],
                }
                atomic_write_json(manifest_path, manifest)
                if remote_dir.startswith("root://"):
                    ensure_remote_directory(remote_dir, retry_attempts)
                else:
                    Path(remote_dir).mkdir(parents=True, exist_ok=True)
                upload_file(archive_path, archive_url, retry_attempts)
                upload_file(manifest_path, manifest_url, retry_attempts)
                result = manifest
            except Exception as exc:
                result["phase"] = "archive_upload"
                result["message"] = str(exc)
            results.append(result)
            atomic_write_json(
                per_job_status_path(log_root, campaign, job_component),
                result,
            )

        ok_count = sum(result.get("status") == "ok" for result in results)
        if ok_count == len(results):
            archive_status = "ok"
        elif ok_count:
            archive_status = "partial"
        else:
            archive_status = "failed"
        payload = {
            **base_status,
            "archive_status": archive_status,
            "phase": "complete" if archive_status == "ok" else "partial_failure",
            "proxy_timeleft_seconds": proxy_timeleft,
            "results": results,
            "updated_at": utc_now(),
        }
        atomic_write_json(overall_path, payload)

        index_path = workdir / "archive_index.json"
        atomic_write_json(index_path, payload)
        index_dir = join_target(
            target_base_url,
            "output",
            "_log_archives",
            workflow_id,
        )
        index_url = join_target(index_dir, index_path.name)
        try:
            if index_dir.startswith("root://"):
                ensure_remote_directory(index_dir, retry_attempts)
            else:
                Path(index_dir).mkdir(parents=True, exist_ok=True)
            upload_file(index_path, index_url, retry_attempts)
            payload["index_url"] = index_url
        except Exception as exc:
            payload["index_error"] = str(exc)
            if payload["archive_status"] == "ok":
                payload["archive_status"] = "partial"
                payload["phase"] = "index_upload"
        payload["updated_at"] = utc_now()
        atomic_write_json(overall_path, payload)
        return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive staged campaign/job logs from a top-level FINAL worker."
    )
    parser.add_argument("--log-root", required=True)
    parser.add_argument("--target-base-url", required=True)
    parser.add_argument("--workflow-id", required=True)
    parser.add_argument("--proxy-bundle", required=True)
    parser.add_argument("--retry-attempts", type=int, default=3)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.retry_attempts <= 0:
        raise ValueError("--retry-attempts must be positive")
    payload = archive_workflow(
        log_root=Path(args.log_root).resolve(),
        target_base_url=args.target_base_url.rstrip("/"),
        workflow_id=args.workflow_id,
        proxy_bundle=Path(args.proxy_bundle).resolve(),
        retry_attempts=args.retry_attempts,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("archive_status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
