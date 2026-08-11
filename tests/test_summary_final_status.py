#!/usr/bin/env python3
"""Verify that the top-level FINAL preserves upstream DAG status."""

from __future__ import annotations

import subprocess
import tarfile
import tempfile
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
SUMMARY = BASE_DIR / "processing" / "templates" / "summary.sh"


def run_summary(
    root: Path,
    *,
    archive_enabled: bool,
    dag_status: int,
    failed_count: int,
) -> subprocess.CompletedProcess:
    bundle_root = root / "bundle"
    helper = bundle_root / "runtime" / "tools" / "archive_workflow_logs.py"
    helper.parent.mkdir(parents=True, exist_ok=True)
    helper.write_text("raise SystemExit(1)\n", encoding="utf-8")
    bundle = root / "summary_runtime_bundle.tar.gz"
    with tarfile.open(bundle, "w:gz") as archive:
        archive.add(helper, arcname="runtime/tools/archive_workflow_logs.py")
    proxy = root / "proxy_bundle.tar.gz"
    proxy.write_bytes(b"unused")
    log_root = root / "logs"
    log_root.mkdir()
    return subprocess.run(
        [
            "bash",
            str(SUMMARY),
            str(bundle),
            str(proxy),
            str(log_root),
            str(root / "target"),
            "status-test",
            "true" if archive_enabled else "false",
            str(dag_status),
            str(failed_count),
        ],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="summary_status_", dir="/tmp/chiw") as tmp:
        root = Path(tmp)
        successful = run_summary(
            root / "success",
            archive_enabled=False,
            dag_status=0,
            failed_count=0,
        )
        assert successful.returncode == 0, successful.stdout

        archive_failed = run_summary(
            root / "archive_failed",
            archive_enabled=True,
            dag_status=0,
            failed_count=0,
        )
        assert archive_failed.returncode == 0, archive_failed.stdout
        assert "archive_rc=1" in archive_failed.stdout

        upstream_failed = run_summary(
            root / "upstream_failed",
            archive_enabled=False,
            dag_status=1,
            failed_count=1,
        )
        assert upstream_failed.returncode != 0, upstream_failed.stdout

    print("[OK] FINAL archive is fail-soft and upstream DAG failures are preserved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
