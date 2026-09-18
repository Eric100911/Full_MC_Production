"""Configured storage roots; worker code never guesses alternate layouts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from urllib.parse import urlsplit


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
