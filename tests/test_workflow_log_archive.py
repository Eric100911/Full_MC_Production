#!/usr/bin/env python3
"""Tests for top-level FINAL workflow log archival."""

from __future__ import annotations

import json
import os
import stat
import sys
import tarfile
import tempfile
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from tools.archive_workflow_logs import (  # noqa: E402
    archive_workflow,
    collect_group_files,
    create_deterministic_archive,
    discover_log_groups,
    parse_xrootd_url,
    run_with_retries,
)


def write_proxy_bundle(path: Path) -> None:
    source = path.parent / "proxy_source"
    proxy = source / "credentials" / "x509_user_proxy"
    proxy.parent.mkdir(parents=True)
    proxy.write_text("mock proxy\n", encoding="utf-8")
    with tarfile.open(path, "w:gz") as archive:
        archive.add(proxy, arcname="credentials/x509_user_proxy")


def write_mock_voms(path: Path) -> None:
    path.write_text(
        """#!/bin/bash
set -euo pipefail
if [[ ${MOCK_PROXY_EXPIRED:-0} == 1 ]]; then
    exit 1
fi
for arg in "$@"; do
    if [[ "$arg" == "--timeleft" ]]; then
        echo 7200
        exit 0
    fi
done
exit 0
""",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def populate_logs(log_root: Path) -> None:
    for campaign in ("CAMPAIGN_A", "CAMPAIGN_B"):
        for stage in ("processing", "miniaod_merge", "final"):
            leaf = log_root / campaign / stage / "job_000000"
            leaf.mkdir(parents=True)
            (leaf / f"{stage}.stdout").write_text(
                f"{campaign} {stage}\n",
                encoding="utf-8",
            )
    ntuple = log_root / "CAMPAIGN_A" / "ntuple" / "job_000000"
    ntuple.mkdir(parents=True)
    (ntuple / "ntuple.stdout").write_text("ntuple\n", encoding="utf-8")
    shared = log_root / "_shared" / "summary"
    shared.mkdir(parents=True)
    (shared / "summary.stdout").write_text("excluded\n", encoding="utf-8")


def main() -> int:
    attempts = []

    def flaky_action() -> None:
        attempts.append(len(attempts) + 1)
        if len(attempts) < 3:
            raise RuntimeError("transient failure")

    run_with_retries(flaky_action, attempts=3, delays=(0, 0))
    assert attempts == [1, 2, 3]

    with tempfile.TemporaryDirectory(prefix="workflow_archive_", dir="/tmp/chiw") as tmp:
        root = Path(tmp)
        log_root = root / "logs"
        populate_logs(log_root)

        groups = discover_log_groups(log_root)
        assert sorted(groups) == [
            ("CAMPAIGN_A", "job_000000"),
            ("CAMPAIGN_B", "job_000000"),
        ]
        files, stages, warnings = collect_group_files(
            log_root,
            groups[("CAMPAIGN_B", "job_000000")],
        )
        assert stages["ntuple"]["present"] is False
        assert "missing stage directory: ntuple" in warnings
        assert all(not relative.startswith("_shared/") for _, relative in files)

        first = root / "first.tar.gz"
        second = root / "second.tar.gz"
        create_deterministic_archive(first, files)
        create_deterministic_archive(second, files)
        assert first.read_bytes() == second.read_bytes()

        endpoint, remote_path = parse_xrootd_url(
            "root://cceos.ihep.ac.cn:1094///store/user/chiw/test"
        )
        assert endpoint == "root://cceos.ihep.ac.cn:1094/"
        assert remote_path == "/store/user/chiw/test"

        proxy_bundle = root / "proxy_bundle.tar.gz"
        write_proxy_bundle(proxy_bundle)
        mock_bin = root / "mock_bin"
        mock_bin.mkdir()
        write_mock_voms(mock_bin / "voms-proxy-info")
        old_path = os.environ.get("PATH", "")
        os.environ["PATH"] = f"{mock_bin}:{old_path}"
        try:
            payload = archive_workflow(
                log_root=log_root,
                target_base_url=str(root / "target"),
                workflow_id="test-workflow",
                proxy_bundle=proxy_bundle,
                retry_attempts=3,
            )
        finally:
            os.environ["PATH"] = old_path

        assert payload["archive_status"] == "ok"
        assert len(payload["results"]) == 2
        assert payload["index_url"].endswith(
            "output/_log_archives/test-workflow/archive_index.json"
        )
        for campaign in ("CAMPAIGN_A", "CAMPAIGN_B"):
            archive_path = (
                root
                / "target"
                / "output"
                / campaign
                / "job_000000_logs"
                / "test-workflow"
                / f"logs_{campaign}_job_000000.tar.gz"
            )
            manifest_path = archive_path.with_suffix("").with_suffix(".json")
            assert archive_path.is_file()
            assert manifest_path.is_file()
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            assert manifest["status"] == "ok"
            with tarfile.open(archive_path, "r:gz") as archive:
                members = archive.getnames()
            assert members
            assert all(name.startswith(f"{campaign}/") for name in members)

        status = json.loads(
            (
                log_root
                / "_shared"
                / "summary"
                / "workflow_log_archive_status_test-workflow.json"
            ).read_text(encoding="utf-8")
        )
        assert status["archive_status"] == "ok"

        os.environ["PATH"] = f"{mock_bin}:{old_path}"
        os.environ["MOCK_PROXY_EXPIRED"] = "1"
        try:
            expired = archive_workflow(
                log_root=log_root,
                target_base_url=str(root / "target"),
                workflow_id="test-workflow-expired",
                proxy_bundle=proxy_bundle,
                retry_attempts=3,
            )
        finally:
            os.environ["PATH"] = old_path
            os.environ.pop("MOCK_PROXY_EXPIRED", None)
        assert expired["archive_status"] == "failed"
        assert expired["phase"] == "proxy_validation"
        assert all(result["phase"] == "proxy_validation" for result in expired["results"])

    print("[OK] top-level FINAL workflow log archive grouping and manifests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
