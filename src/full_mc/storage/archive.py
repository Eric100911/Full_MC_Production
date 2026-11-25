"""Archive compressed members without recompression."""
from pathlib import Path
import tarfile

def create_archive(members, archive_path):
    items = [Path(x) for x in members]
    names = [x.name for x in items]
    if not items or len(set(names)) != len(names):
        raise ValueError("archive requires distinct, nonempty member names")
    if any(not x.is_file() or x.stat().st_size <= 0 for x in items):
        raise ValueError("archive members must be nonempty files")
    archive_path = Path(archive_path)
    with tarfile.open(archive_path, "w") as archive:
        for item in items:
            archive.add(item, arcname=item.name, recursive=False)
    with tarfile.open(archive_path, "r:") as archive:
        if {m.name:m.size for m in archive.getmembers()} != {x.name:x.stat().st_size for x in items}:
            raise RuntimeError("archive member verification failed")
    return archive_path
