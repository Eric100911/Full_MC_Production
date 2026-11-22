#!/usr/bin/env python3
"""Regression tests for plain-Condor LHE inventory counting."""

from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import sys


BASE_DIR = Path(__file__).resolve().parents[1]
TMP_ROOT = Path("/tmp/chiw")
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR / "tests"))

from dag_generator import load_existing_lhe_inventory, pool_storage_name  # noqa: E402
from generate_synthetic_lhe import generate  # noqa: E402
from tools.lhe_inventory_condor import canonicalize_lhe_path  # noqa: E402


def write_gzipped_lhe(path: Path, n_events: int) -> None:
    plain = path.with_suffix("")
    plain.parent.mkdir(parents=True, exist_ok=True)
    generate(str(plain), n_events=n_events)
    with open(plain, "rb") as source, gzip.open(path, "wb") as target:
        shutil.copyfileobj(source, target)
    plain.unlink()


def run(command: list[str], *, check: bool = True, env=None) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        cwd=BASE_DIR,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )


def main() -> int:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="condor_lhe_inventory_", dir=TMP_ROOT) as tmp:
        root = Path(tmp)
        local_base = root / "local"
        pool_dir = local_base / "lhe_pools" / pool_storage_name("pool_2jpsi_cs")
        first = pool_dir / "sample_pool_2jpsi_cs_60100.lhe.gz"
        second = pool_dir / "sample_pool_2jpsi_cs_60101.lhe.gz"
        write_gzipped_lhe(first, 11)
        write_gzipped_lhe(second, 19)

        workdir = root / "counting"
        output = root / "complete.json"
        prepare = run(
            [
                "python3",
                "dag_generator.py",
                "scan-lhe-inventory",
                "--campaign",
                "JJP_SPS_CS",
                "--output",
                str(output),
                "--run-on-condor",
                str(workdir),
                "--machine-env",
                "local_condor",
                "--local-output-base",
                str(local_base),
                "--count-events",
                "--condor-max-materialize",
                "7",
            ]
        )
        assert "Prepared Condor inventory workspace" in prepare.stdout
        worklist = json.loads((workdir / "worklist.json").read_text(encoding="utf-8"))
        assert [task["path"] for task in worklist["tasks"]] == [str(first), str(second)]
        submit_text = (workdir / "count_lhe_inventory.sub").read_text(encoding="utf-8")
        assert "max_materialize_count = 7" in submit_text
        assert "task_count = 2" in submit_text
        assert "queue $(task_count)" in submit_text
        assert "condor_submit_dag" not in submit_text
        worker = BASE_DIR / "tools" / "lhe_inventory_condor.py"
        for index in range(2):
            run(
                [
                    "python3",
                    str(worker),
                    "worker",
                    str(workdir / "worklist.json"),
                    str(index),
                    "-",
                    str(workdir / "results" / f"count_{index}.json"),
                ]
            )

        remote_worklist = root / "remote_worklist.json"
        remote_payload = {
            "tasks": [
                {
                    "task_id": 0,
                    "pool": "pool_2jpsi_cs",
                    "path": "root://example.invalid///store/sample_pool_2jpsi_cs_60100.lhe.gz",
                    "discovered_path": str(first),
                    "seed": 60100,
                }
            ]
        }
        remote_worklist.write_text(json.dumps(remote_payload), encoding="utf-8")
        fake_xrdcp = root / "fake_xrdcp" / "xrdcp"
        fake_xrdcp.parent.mkdir()
        fake_xrdcp.write_text(
            "#!/bin/sh\ncp \"${FAKE_LHE_SOURCE}\" \"$4\"\n",
            encoding="utf-8",
        )
        fake_xrdcp.chmod(0o755)
        fake_proxy = root / "x509up"
        fake_proxy.write_text("test proxy", encoding="utf-8")
        remote_env = os.environ.copy()
        remote_env["PATH"] = str(fake_xrdcp.parent) + os.pathsep + remote_env.get("PATH", "")
        remote_env["FAKE_LHE_SOURCE"] = str(first)
        remote_result = root / "remote_result.json"
        run(
            [
                "python3",
                str(worker),
                "worker",
                str(remote_worklist),
                "0",
                str(fake_proxy),
                str(remote_result),
            ],
            env=remote_env,
        )
        assert json.loads(remote_result.read_text(encoding="utf-8"))["actual_events"] == 11

        summarize = run(
            [
                "python3",
                "dag_generator.py",
                "scan-lhe-inventory",
                "--summarize-from",
                str(workdir),
                "--output",
                str(output),
            ]
        )
        assert "complete LHE inventory" in summarize.stdout
        complete = json.loads(output.read_text(encoding="utf-8"))
        pool = complete["pools"]["pool_2jpsi_cs"]
        assert complete["complete"] is True
        assert pool["count"] == 2
        assert pool["counted_events"] == 30
        assert [item["actual_events"] for item in pool["files"]] == [11, 19]

        (workdir / "results" / "count_1.json").unlink()
        partial_path = root / "partial.json"
        partial_result = run(
            [
                "python3",
                "dag_generator.py",
                "scan-lhe-inventory",
                "--summarize-from",
                str(workdir),
                "--output",
                str(partial_path),
            ],
            check=False,
        )
        assert partial_result.returncode != 0
        partial = json.loads(partial_path.read_text(encoding="utf-8"))
        assert partial["complete"] is False
        partial_pool = partial["pools"]["pool_2jpsi_cs"]
        assert partial_pool["count"] == 1
        assert partial_pool["failures"][0]["status"] == "missing_result"
        try:
            load_existing_lhe_inventory(str(partial_path))
        except ValueError as exc:
            assert "marked incomplete" in str(exc)
        else:
            raise AssertionError("Incomplete inventory was accepted without an override")
        accepted = load_existing_lhe_inventory(str(partial_path), allow_incomplete=True)
        assert len(accepted["pool_2jpsi_cs"]) == 1

        converted = canonicalize_lhe_path(
            "/eos/user/c/chiw/example.lhe.gz",
            {
                "cern_eos_mount_prefix": "/eos/user/",
                "cern_eos_xrootd_prefix": "root://eosuser.cern.ch///eos/user/",
            },
        )
        assert converted == "root://eosuser.cern.ch///eos/user/c/chiw/example.lhe.gz"

        fake_bin = root / "bin"
        fake_bin.mkdir()
        fake_submit = fake_bin / "condor_submit"
        fake_submit.write_text("#!/bin/sh\necho '987.0 - 987.1'\n", encoding="utf-8")
        fake_submit.chmod(0o755)
        submit_env = os.environ.copy()
        submit_env["PATH"] = str(fake_bin) + os.pathsep + submit_env.get("PATH", "")
        submit = run(
            [
                "python3",
                "dag_generator.py",
                "scan-lhe-inventory",
                "--run-on-condor",
                str(workdir),
                "--submit",
            ],
            env=submit_env,
        )
        assert "cluster 987" in submit.stdout
        manifest = json.loads((workdir / "run_manifest.json").read_text(encoding="utf-8"))
        assert manifest["cluster_id"] == 987

        duplicate_submit = run(
            [
                "python3",
                "dag_generator.py",
                "scan-lhe-inventory",
                "--run-on-condor",
                str(workdir),
                "--submit",
            ],
            check=False,
            env=submit_env,
        )
        assert duplicate_submit.returncode != 0
        assert "already records cluster 987" in duplicate_submit.stderr

    print("[OK] plain-Condor LHE inventory preparation, counting, and summary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
