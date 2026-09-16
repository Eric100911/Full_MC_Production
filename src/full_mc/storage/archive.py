"""Archive compressed members without recompression and restore selected members."""
from pathlib import Path
from pathlib import PurePosixPath
import shutil
import tarfile
import tempfile
from .transfer import TransferClient

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

def upload_archive(archive_path, destination, transfer=None):
    return (transfer or TransferClient()).copy_out(Path(archive_path), destination)


def restore_members(archive_ref, members, destination, transfer=None):
    """Restore explicitly recorded archive members to a local operator directory.

    The caller supplies the controller-recorded member entries rather than a
    filename pattern. This keeps recovery tied to the verified archive manifest
    and avoids extracting arbitrary archive paths.
    """
    if not isinstance(archive_ref, dict) or not archive_ref.get("url"):
        raise ValueError("archive restoration requires an archive artifact reference")
    requested = list(members)
    if not requested:
        raise ValueError("archive restoration requires at least one recorded member")
    identities = [item.get("artifact_id") for item in requested]
    names = [item.get("member") for item in requested]
    if (any(not value for value in identities + names)
            or len(set(identities)) != len(identities)
            or len(set(names)) != len(names)):
        raise ValueError("archive restoration members must have unique identities and names")
    if any(PurePosixPath(name).name != name or name in (".", "..") for name in names):
        raise ValueError("archive restoration refuses unsafe member names")

    client = transfer or TransferClient()
    target = Path(destination).resolve()
    target.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mcprod-restore-", dir="/tmp/chiw") as temporary:
        local_archive = client.copy_in(archive_ref["url"], Path(temporary) / "upstream.tar")
        expected_size = archive_ref.get("size_bytes")
        if expected_size is not None and local_archive.stat().st_size != int(expected_size):
            raise RuntimeError("downloaded archive size disagrees with its verified record")
        with tarfile.open(local_archive, "r:") as archive:
            indexed = {item.name: item for item in archive.getmembers() if item.isfile()}
            if not set(names) <= set(indexed):
                raise RuntimeError("archive no longer contains every recorded member")
            restored = []
            for recorded in requested:
                member = indexed[recorded["member"]]
                expected_member_size = recorded.get("size_bytes")
                if expected_member_size is not None and member.size != int(expected_member_size):
                    raise RuntimeError("archive member size disagrees with its recorded artifact")
                output = target / member.name
                if output.exists():
                    raise FileExistsError(f"refusing to overwrite restored member: {output}")
                incoming = archive.extractfile(member)
                if incoming is None:
                    raise RuntimeError("archive member cannot be read")
                with incoming, output.open("xb") as stream:
                    shutil.copyfileobj(incoming, stream)
                if output.stat().st_size != member.size:
                    raise RuntimeError("restored archive member has an unexpected size")
                ref = {key: value for key, value in recorded.items() if key != "member"}
                ref.update(url=str(output), local_path=str(output), size_bytes=output.stat().st_size,
                           restored_from=archive_ref["artifact_id"])
                restored.append(ref)
    return restored

def reclaim_components(members, *, component_gates):
    if not component_gates or not all(component_gates.values()):
        raise RuntimeError("component validation must precede reclamation")
    client = TransferClient()
    for url in members:
        client.remove(str(url))
