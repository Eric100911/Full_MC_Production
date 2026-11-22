#!/usr/bin/env python3

import sys
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dag_generator import render_dagman_config


def main() -> None:
    throttled = render_dagman_config(
        SimpleNamespace(
            dagman_max_jobs_submitted=20000,
            dagman_max_jobs_idle=20000,
        )
    )
    assert "DAGMAN_MAX_SUBMITS_PER_INTERVAL = 100" in throttled
    assert "DAGMAN_SUBMIT_DELAY = 0" in throttled

    unlimited = render_dagman_config(
        SimpleNamespace(
            dagman_max_jobs_submitted=0,
            dagman_max_jobs_idle=0,
        )
    )
    assert "DAGMAN_MAX_SUBMITS_PER_INTERVAL" not in unlimited
    assert "DAGMAN_SUBMIT_DELAY" not in unlimited


if __name__ == "__main__":
    main()
