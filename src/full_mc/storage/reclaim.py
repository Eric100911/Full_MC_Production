"""Batched staged-product deletion inside exactly one utilities container."""
from __future__ import annotations
import shlex
import subprocess
from pathlib import Path

from full_mc.runtime.profiles import run_profile
from full_mc.storage.transfer import _remote_parts, is_remote, local_path

UTILITIES_PROFILE = "utilities-el9"
DEFAULT_WORK_DIR = Path("/tmp/chiw/mcprod-reclaim")
# Removal of a namespace root would delete objects this run never staged.
BROAD_REMOVALS = ("/", "/store", "/eos")
# A single argv element is capped near 128 KiB (MAX_ARG_STRLEN) and this script
# travels as one argument through apptainer, so very long batches move to a file.
INLINE_SCRIPT_LIMIT = 60_000

_REMOVE_FUNCTION = [
    "remove() {",
    '    endpoint="$1"; lfn="$2"; index="$3"',
    '    if output=$(xrdfs "$endpoint" rm "$lfn" 2>&1); then',
    "        printf '%s\\tremoved\\n' \"$index\"",
    "        return 0",
    "    fi",
    '    case "$output" in',
    '        *3011*|*"No such file"*)',
    "            printf '%s\\talready-missing\\n' \"$index\"",
    "            return 0 ;;",
    "    esac",
    "    printf 'reclaim failed for %s: %s\\n' \"$lfn\" \"$output\" >&2",
    "    return 1",
    "}",
]


def _build_script(entries):
    """Return one shell script that deletes every ``(index, endpoint, lfn)``."""
    lines = list(_REMOVE_FUNCTION)
    lines.append("reclaim_error=0")
    for index, endpoint, lfn in entries:
        lines.append("remove %s %s %s || reclaim_error=1" % (
            shlex.quote(endpoint), shlex.quote(lfn), shlex.quote(str(index))))
    lines.append('exit "$reclaim_error"')
    return "\n".join(lines) + "\n"


def _command(script, work_dir):
    if len(script) <= INLINE_SCRIPT_LIMIT:
        return ["bash", "-c", script]
    path = Path(work_dir) / "reclaim.sh"
    path.write_text(script, encoding="utf-8")
    return ["bash", str(path)]


def _remove_local(url):
    path = local_path(url)
    if path.exists() or path.is_symlink():
        path.unlink()
        return "removed"
    return "already-missing"


def _remove_remote(entries, *, runner, work_dir):
    planned = []
    for index, url in entries:
        endpoint, lfn = _remote_parts(url)
        if lfn in BROAD_REMOVALS:
            raise ValueError("refusing broad removal: " + url)
        planned.append((index, endpoint, lfn))
    command = _command(_build_script(planned), work_dir)
    if runner is not None:
        finished = runner(command, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, check=False)
    else:
        finished = run_profile(UTILITIES_PROFILE, command, cwd=work_dir,
                               check=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout = finished.stdout or ""
    stderr = finished.stderr or ""
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    reported = {}
    for line in stdout.splitlines():
        index, separator, disposition = line.partition("\t")
        if separator and index.isdigit() and disposition in ("removed", "already-missing"):
            reported[int(index)] = disposition
    if finished.returncode:
        raise RuntimeError("reclaim command failed: " + stderr[-2000:])
    unreported = [url for index, url in entries if index not in reported]
    if unreported:
        raise RuntimeError("reclaim reported no disposition for: " + ", ".join(unreported))
    return {url: reported[index] for index, url in entries}


def remove_many(targets, *, runner=None, work_dir=None):
    """Delete staged objects, launching at most one container for the whole batch.

    Returns a ``{target: "removed" | "already-missing"}`` mapping.  A missing
    object is successful idempotent cleanup; authentication and other remote
    failures raise so Condor retries the task.  Local targets are unlinked
    directly because the utilities container cannot see worker scratch paths.
    """
    order = []
    for target in targets:
        value = str(target)
        if value not in order:
            order.append(value)
    work_dir = Path(work_dir or DEFAULT_WORK_DIR)
    dispositions = {}
    remote = []
    for index, url in enumerate(order):
        if is_remote(url):
            remote.append((index, url))
        else:
            dispositions[url] = _remove_local(url)
    if remote:
        dispositions.update(_remove_remote(remote, runner=runner, work_dir=work_dir))
    return {url: dispositions[url] for url in order}
