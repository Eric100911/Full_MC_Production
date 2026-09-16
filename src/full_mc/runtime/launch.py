"""Capture each command stream once and retain failure diagnostics."""
from pathlib import Path
import logging
import subprocess
from .profiles import run_profile

def launch(profile, command, work_dir, env=None):
    work_dir = Path(work_dir)
    with (work_dir / "commands.log").open("a", encoding="utf-8") as stream:
        logging.debug("Runtime %s: %r", profile, command)
        outcome = run_profile(profile, command, cwd=work_dir, env=env, check=False,
                              stdout=stream, stderr=subprocess.STDOUT)
    if outcome.returncode:
        raise RuntimeError(f"{profile} command failed ({outcome.returncode}); diagnostics: {work_dir / 'commands.log'}")
