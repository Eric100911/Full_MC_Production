"""Small JSON record operations shared across CLI and worker boundaries."""
import json
import os
import tempfile
from pathlib import Path

def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))

def write_immutable_json(path, payload):
    """Create-or-compare: identical content is a retry, different content is fatal."""
    path = Path(path)
    if path.exists():
        if read_json(path) != payload:
            raise RuntimeError("immutable record conflict: " + str(path))
        return
    atomic_json(path, payload)

def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="."+path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return str(path)
