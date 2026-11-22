"""Resolve run-scoped DAGMan deferred-script intervals."""

import os
from pathlib import Path
import re

DEFAULT_SECONDS = 60
MAX_SECONDS = 2**31 - 1
OVERRIDE_SECONDS_ENV = "MCPROD_PLAN_ACCEPT_POST_SECONDS"
OVERRIDE_RUN_DIR_ENV = "MCPROD_PLAN_ACCEPT_POST_RUN_DIR"


def validate_script_defer_seconds(value, name="DAGMan SCRIPT DEFER interval"):
    if type(value) is not int or not 1 <= value <= MAX_SECONDS:
        raise ValueError(
            f"{name} must be an integer in the supported range 1..{MAX_SECONDS}"
        )
    return value


def resolve_script_defer(scheduler):
    """Return validated default and PLAN accept-result POST intervals.

    Missing settings intentionally resolve to 60 seconds so old frozen run
    specs keep their original retry cadence.
    """
    if not isinstance(scheduler, dict):
        raise ValueError("scheduler must be an object")
    options = scheduler.get("script_defer", {})
    if not isinstance(options, dict):
        raise ValueError("scheduler.script_defer must be an object")
    unknown = set(options) - {"default_seconds", "plan_accept_post_seconds"}
    if unknown:
        raise ValueError("scheduler.script_defer has an unknown setting")
    default_seconds = validate_script_defer_seconds(
        options.get("default_seconds", DEFAULT_SECONDS),
        "scheduler.script_defer.default_seconds",
    )
    plan_accept_post_seconds = validate_script_defer_seconds(
        options.get("plan_accept_post_seconds", default_seconds),
        "scheduler.script_defer.plan_accept_post_seconds",
    )
    return {
        "default_seconds": default_seconds,
        "plan_accept_post_seconds": plan_accept_post_seconds,
    }


def controller_render_override(run_dir):
    """Read an optional per-run managed-controller render override."""
    raw_seconds = os.environ.get(OVERRIDE_SECONDS_ENV)
    raw_run_dir = os.environ.get(OVERRIDE_RUN_DIR_ENV)
    if raw_seconds is None and raw_run_dir is None:
        return None
    if raw_seconds is None or raw_run_dir is None:
        raise ValueError("incomplete controller PLAN POST interval policy")
    if Path(raw_run_dir).expanduser().resolve() != Path(run_dir).expanduser().resolve():
        raise ValueError("controller PLAN POST interval policy belongs to a different run")
    if not re.fullmatch(r"[1-9][0-9]*", raw_seconds):
        raise ValueError("controller PLAN POST interval policy has an invalid value")
    return validate_script_defer_seconds(
        int(raw_seconds), "controller PLAN POST interval policy"
    )
