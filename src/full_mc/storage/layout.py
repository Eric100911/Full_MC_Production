"""Configured storage roots; worker code never guesses alternate layouts."""

from __future__ import annotations

from dataclasses import dataclass
import json
import posixpath
from pathlib import Path
from pathlib import PurePosixPath
from urllib.parse import urlsplit


def canonical_storage_location(value):
    """Return a namespace-aware identity for an XRootD URL or local path.

    XRootD authorities are retained so identical LFNs on CERN and IHEP do not
    compare equal.  A missing XRootD port means the repository's explicit
    default endpoint port, 1094.  Local CERN EOS mounts are mapped to the
    corresponding ``eosuser.cern.ch`` namespace.
    """
    text = str(value).strip()
    if text.startswith("root://"):
        parsed = urlsplit(text)
        host = (parsed.hostname or "").lower()
        if not host:
            raise ValueError(f"invalid XRootD location: {value!r}")
        port = parsed.port or 1094
        path = posixpath.normpath("/" + parsed.path.lstrip("/"))
        return ("xrootd", f"{host}:{port}", PurePosixPath(path))

    expanded = Path(text).expanduser()
    logical_text = posixpath.normpath(expanded.as_posix())
    if logical_text.startswith("/eos/user/") or logical_text == "/eos/user":
        # Preserve the EOS namespace spelling even when this host exposes
        # /eos/user as a symlink to an implementation-specific mount.
        return ("xrootd", "eosuser.cern.ch:1094", PurePosixPath(logical_text))
    path = expanded.resolve(strict=False)
    path_text = path.as_posix()
    return ("file", "", PurePosixPath(path_text))


def storage_path_is_within(candidate, directory):
    """Return whether *candidate* is equal to or below *directory*.

    Comparison requires both namespace kind and normalized authority to match;
    textual prefixes alone are deliberately insufficient.
    """
    return storage_location_is_within(
        canonical_storage_location(candidate), canonical_storage_location(directory)
    )


def storage_location_is_within(candidate, directory):
    """Compare two already-canonical ``(kind, authority, path)`` tuples."""
    candidate_kind, candidate_authority, candidate_path = candidate
    directory_kind, directory_authority, directory_path = directory
    return (
        (candidate_kind, candidate_authority) == (directory_kind, directory_authority)
        and (candidate_path == directory_path or directory_path in candidate_path.parents)
    )


def _parent_storage_location(value):
    kind, authority, path = canonical_storage_location(value)
    return (kind, authority, path.parent)


def storage_location_parent(value):
    """Return a canonical tuple for the parent of a storage location."""
    return _parent_storage_location(value)


def _root_url(value: str) -> str:
    if not value.startswith("root://"):
        raise ValueError(f"storage URL must be root://: {value!r}")
    parsed = urlsplit(value)
    if not parsed.netloc or not (parsed.path.startswith("///store/")
                                 or parsed.path.startswith("///eos/")):
        raise ValueError(f"storage URL must use root://host///store/... or root://host///eos/...: {value!r}")
    return value.rstrip("/")


@dataclass(frozen=True)
class StorageLayout:
    cern_staging: str
    ihep_products: str
    ihep_archive: str

    @classmethod
    def from_config(cls, path: Path) -> "StorageLayout":
        data = json.loads(path.read_text(encoding="utf-8"))
        roots = data.get("storage", {}).get("roots", {})
        # Legacy configs keep their sole product root in eos_lfn_base.
        product = roots.get("ihep_products")
        if not product:
            endpoint = data["storage"]["eos_redirector"]
            product = f"root://{endpoint}///{data['storage']['eos_lfn_base'].lstrip('/')}"
        return cls(
            _root_url(roots.get("cern_staging", "root://eosuser.cern.ch///store/user/chiw/MC_Production_v3")),
            _root_url(product),
            _root_url(roots.get("ihep_archive", product.rstrip("/") + "/archive")),
        )

    def root(self, name: str) -> str:
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise ValueError(f"unknown storage root: {name}") from exc

    def url(self, root: str, relative_path: str) -> str:
        relative = relative_path.lstrip("/")
        if not relative or ".." in Path(relative).parts:
            raise ValueError(f"invalid storage relative path: {relative_path!r}")
        return f"{self.root(root)}/{relative}"
