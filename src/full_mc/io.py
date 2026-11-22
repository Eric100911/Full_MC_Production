"""Small JSON record operations shared across CLI and worker boundaries."""
import json
import os
import stat
import tempfile
from pathlib import Path


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _json_text(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n"


def _sync_file(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sync_directory(path):
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        if not stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("immutable record parent is not a directory")
        os.fsync(fd)
    finally:
        os.close(fd)


def _ensure_durable_parent(path):
    """Create a missing parent chain and sync the entries created in it.

    The chain is bounded by the first already-existing ancestor. Existing
    ancestors are assumed to have been made durable by the run/control setup;
    after a restart, this helper cannot identify which old directories were
    created by a previous interrupted call.
    """
    path = Path(path)
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        parent = current.parent
        if parent == current:
            raise FileNotFoundError("no existing ancestor for immutable record parent")
        current = parent
    if not current.is_dir():
        raise NotADirectoryError(current)

    if not missing:
        # Also repairs a prior failed sync of this existing leaf directory.
        _sync_directory(path)
        return

    for directory in reversed(missing):
        try:
            directory.mkdir()
        except FileExistsError:
            if not directory.is_dir():
                raise

    # Sync the first existing ancestor and each newly present component from
    # top to bottom, so each directory entry is covered before file publish.
    for directory in [missing[-1].parent] + list(reversed(missing)):
        _sync_directory(directory)


def _compare_immutable(path, payload):
    if read_json(path) != payload:
        raise RuntimeError("immutable record conflict: " + str(path))
    # A prior attempt may have published the name and then failed while syncing
    # its parent. Retry both durability barriers before state can point to it.
    _sync_file(path)
    _sync_directory(path.parent)


def write_immutable_json(path, payload):
    """Durably create-or-compare an immutable JSON record without overwriting.

    JSON normalization makes a tuple in a fresh descriptor compare equal to
    the array produced by serializing and reading it back. A hard link gives
    publication exclusive-create semantics: concurrent writers cannot replace
    an existing immutable record, even when their payloads differ. The file
    and containing directory are synced before return. Newly created parent
    entries are synced up to the first existing ancestor; existing ancestors
    are assumed durable, and a later process cannot identify directories left
    by an interrupted earlier parent-creation attempt.
    """
    path = Path(path)
    _ensure_durable_parent(path.parent)
    encoded = _json_text(payload).encode("utf-8")
    # Compare the value consumers read, including JSON's tuple-to-array
    # conversion and object-key conversion.
    canonical = json.loads(encoded.decode("utf-8"))
    if path.exists():
        _compare_immutable(path, canonical)
        return

    fd, temporary = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # Same-directory link creation is atomic and exclusive. os.replace
            # would allow a concurrent retry to overwrite a different record.
            os.link(temporary, path)
        except FileExistsError:
            _compare_immutable(path, canonical)
            return
        os.unlink(temporary)
        _sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="."+path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(_json_text(value))
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return str(path)
