"""Deterministic submit-host paths; unmarked frozen runs retain flat paths."""
from pathlib import Path
import re

LAYOUT_MARKER = ".bucketed-v1"
FILES_PER_BUCKET = 1000
LOG_FILES_PER_TASK = 3  # stdout, stderr, diagnostics


def bucketed(root):
    return (Path(root) / LAYOUT_MARKER).is_file()


def identity_bucket(identity, width=FILES_PER_BUCKET):
    """Keep stage/campaign/profile identity and divide its frozen numeric index."""
    match = re.fullmatch(r"([A-Za-z0-9_.-]*?[A-Za-z_.-])([0-9]+)", identity)
    if not match:
        raise ValueError("identity has no stable numeric suffix: " + identity)
    namespace, number = match.groups()
    return Path(namespace) / f"{int(number) // width:06d}"


def record_path(root, directory, identity):
    root = Path(root)
    parent = root / directory
    if bucketed(root):
        parent /= identity_bucket(identity)
    return parent / (identity + ".json")


def log_path(root, identity, suffix):
    root = Path(root)
    parent = root / "logs"
    if bucketed(root):
        parent /= identity_bucket(identity, FILES_PER_BUCKET // LOG_FILES_PER_TASK)
    return parent / (identity + suffix)
