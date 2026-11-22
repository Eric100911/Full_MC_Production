#!/usr/bin/env python3
"""Generate a minimal DAGMan PRE+NOOP environment smoke test.

The generated root DAG deliberately has no ``ENV SET`` directive.  Its PRE
launcher exports the proxy and CA variables locally, while the NOOP worker is
submitted with ``getenv = False`` so accidental root-environment propagation
is visible in the recorded marker.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import stat
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from full_mc.planning.graph import write_root_dag  # noqa: E402


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def generate(output_dir: Path, proxy: str) -> Path:
    output_dir = output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "steps").mkdir()
    (output_dir / "logs").mkdir()
    (output_dir / "results").mkdir()
    controller = output_dir / "controller"
    controller.mkdir()

    # This is the stand-in for mcprod internal advance-wave.  It records the
    # environment visible to SCRIPT PRE and then succeeds immediately.
    _write_executable(
        controller / "mcprod",
        "#!/bin/bash\n"
        "set -euo pipefail\n"
        "run_dir=''\n"
        "while [[ $# -gt 0 ]]; do\n"
        "    if [[ $1 == --run-dir ]]; then run_dir=$2; shift 2; else shift; fi\n"
        "done\n"
        "printf 'X509_USER_PROXY=%s\\nX509_CERT_DIR=%s\\n' \\\n"
        "    \"$X509_USER_PROXY\" \"$X509_CERT_DIR\" > \"$run_dir/pre-env.txt\"\n"
        "touch \"$run_dir/pre-ok\"\n",
    )

    noop = output_dir / "noop.sh"
    _write_executable(
        noop,
        "#!/bin/bash\n"
        "set -euo pipefail\n"
        "printf 'NOOP_X509_USER_PROXY=%s\\nNOOP_X509_CERT_DIR=%s\\n' \\\n"
        "    \"${X509_USER_PROXY-}\" \"${X509_CERT_DIR-}\" > noop-env.txt\n",
    )
    (output_dir / "noop.sub").write_text(
        "universe = vanilla\n"
        "executable = /bin/bash\n"
        "arguments = noop.sh\n"
        f"transfer_input_files = {noop}\n"
        "should_transfer_files = YES\n"
        "when_to_transfer_output = ON_EXIT\n"
        "transfer_output_files = noop-env.txt\n"
        f'transfer_output_remaps = "noop-env.txt={output_dir / "noop-env.txt"}"\n'
        "getenv = False\n"
        f"output = {output_dir / 'logs/noop.out'}\n"
        f"error = {output_dir / 'logs/noop.err'}\n"
        f"log = {output_dir / 'logs/noop.events.log'}\n"
        'request_cpus = 1\n'
        'request_memory = 128MB\n'
        'request_disk = 100MB\n'
        '+JobFlavour = "espresso"\n'
        "queue\n",
        encoding="utf-8",
    )
    (output_dir / "steps/000000.dag").write_text(
        f"CONFIG {output_dir / 'dagman.config'}\n"
        f"JOB NOOP {output_dir / 'noop.sub'}\n",
        encoding="utf-8",
    )
    write_root_dag(
        output_dir,
        {"core_root": str(controller)},
        1,
        proxy,
    )
    return output_dir / "run.dag"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--proxy",
        default=os.environ.get("X509_USER_PROXY", ""),
        help="persistent proxy path for the generated PRE launcher",
    )
    args = parser.parse_args()
    if not args.proxy:
        parser.error("--proxy or X509_USER_PROXY is required")
    dag = generate(args.output_dir, args.proxy)
    print(dag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
