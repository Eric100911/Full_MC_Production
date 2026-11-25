"""Exact local/XRootD transfers and compact product records."""
from __future__ import annotations
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import zlib
from pathlib import Path
from urllib.parse import urlsplit

def is_remote(url):
    return str(url).startswith("root://")

def local_path(url):
    value = str(url)
    return Path(value[7:] if value.startswith("file://") else value.removeprefix("file:"))

def _remote_parts(url):
    parsed = urlsplit(str(url))
    if parsed.scheme != "root" or not parsed.netloc:
        raise ValueError(f"not an XRootD URL: {url}")
    return f"root://{parsed.netloc}/", "/" + parsed.path.lstrip("/")

def _transfer_scratch():
    root = Path(os.environ.get("_CONDOR_SCRATCH_DIR") or "/tmp/chiw")
    scratch = root / "mcprod-transfer"
    scratch.mkdir(parents=True, exist_ok=True)
    return scratch


class TransferClient:
    def __init__(self, runner=None):
        self.runner = runner

    def _run(self, command):
        if self.runner:
            result = self.runner(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        else:
            from full_mc.runtime.profiles import run_profile
            scratch = _transfer_scratch()
            # ``xrdfs`` receives a remote endpoint followed by remote LFN
            # arguments (which are conventionally rooted at /store or /eos),
            # so none of its positional paths are host paths to bind.  For
            # xrdcp, retain binds for the local scratch side of a copy while
            # ignoring the root:// side and paths whose parent is unavailable.
            binds = []
            if command[0] != "xrdfs":
                for arg in command[1:]:
                    text = str(arg)
                    if is_remote(text) or not (text.startswith("/") or text.startswith("file:")):
                        continue
                    path = local_path(text)
                    if path.parent.exists():
                        binds.append(str(path.resolve().parent))
            result = run_profile("utilities-el9", command, cwd=scratch, check=False,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 bind_paths=binds)
        if result.returncode:
            if "3011" in result.stderr or "No such file" in result.stderr:
                raise FileNotFoundError(result.stderr)
            raise RuntimeError(f"transfer command failed: {command[0]}: {result.stderr[-2000:]}")
        return result

    def size(self, url):
        if not is_remote(url):
            return local_path(url).stat().st_size
        endpoint, lfn = _remote_parts(url)
        output = self._run(["xrdfs", endpoint, "stat", lfn]).stdout
        for line in output.splitlines():
            fields = line.replace(":", " ").split()
            if fields and fields[0].lower() == "size":
                return int(fields[1])
        raise RuntimeError(f"remote stat returned no size: {url}")

    def mkdir_parent(self, destination):
        if is_remote(destination):
            endpoint, lfn = _remote_parts(destination)
            self._run(["xrdfs", endpoint, "mkdir", "-p", str(Path(lfn).parent)])
        else:
            local_path(destination).parent.mkdir(parents=True, exist_ok=True)

    def mkdir(self, url, parents=True):
        """Create exactly one configured directory tree."""
        if is_remote(url):
            endpoint, lfn = _remote_parts(url)
            command = ["xrdfs", endpoint, "mkdir"]
            if parents:
                command.append("-p")
            command.append(lfn)
            self._run(command)
        else:
            local_path(url).mkdir(parents=True, exist_ok=True)

    def read_json_if_exists(self, url):
        try:
            return self.read_json(url)
        except FileNotFoundError:
            return None

    def directory_has_entries(self, url):
        if not is_remote(url):
            path = local_path(url)
            return path.is_dir() and any(path.iterdir())
        endpoint, lfn = _remote_parts(url)
        try:
            output = self._run(["xrdfs", endpoint, "ls", lfn]).stdout
        except FileNotFoundError:
            return False
        return bool(str(output).strip())

    def copy_json_if_absent(self, payload, destination):
        """Create a JSON marker without replacing a competing marker."""
        encoded = json.dumps(payload, sort_keys=True, indent=2) + "\n"
        if not is_remote(destination):
            target = local_path(destination)
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                with target.open("x", encoding="utf-8") as handle:
                    handle.write(encoded)
            except FileExistsError:
                pass
            return
        Path("/tmp/chiw").mkdir(exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir="/tmp/chiw",
                                         prefix="mcprod-marker-", delete=False) as handle:
            handle.write(encoded)
            temporary = Path(handle.name)
        try:
            self._run(["xrdcp", "--nopbar", str(temporary), str(destination)])
        except (FileNotFoundError, RuntimeError) as exc:
            # A no-replace copy reports an existing marker as an error.  The
            # caller reads it immediately afterwards and rejects mismatches.
            if "exists" not in str(exc).lower() and "file exists" not in str(exc).lower():
                raise
        finally:
            temporary.unlink(missing_ok=True)

    def checksum(self, url):
        """Return the Adler-32 checksum of an exact stored object."""
        if not is_remote(url):
            return file_checksum(local_path(url))
        endpoint, lfn = _remote_parts(url)
        fields = self._run(["xrdfs", endpoint, "query", "checksum", lfn]).stdout.split()
        if len(fields) < 2 or fields[0].lower() != "adler32":
            raise RuntimeError(f"remote checksum is not Adler-32: {url}")
        value = fields[1].lower()
        if not 0 < len(value) <= 8 or any(c not in "0123456789abcdef" for c in value):
            raise RuntimeError(f"invalid remote checksum: {url}")
        return value.zfill(8)

    def copy_out(self, source, destination):
        source = Path(source)
        if source.stat().st_size <= 0:
            raise ValueError(f"refusing empty output: {source}")
        self.mkdir_parent(destination)
        if is_remote(destination):
            self._run(["xrdcp", "--nopbar", "--force", str(source), str(destination)])
        elif source.resolve() != local_path(destination).resolve():
            shutil.copy2(source, local_path(destination))
        size = self.size(destination)
        if size != source.stat().st_size:
            raise RuntimeError(f"stage-out size mismatch: {destination}")
        return size

    def copy_atomic(self, source, destination, proc_id):
        """Verify a same-directory temporary payload before replacing its final name."""
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", proc_id):
            raise ValueError("invalid publication ID")
        source = Path(source)
        expected_size = source.stat().st_size
        if expected_size <= 0:
            raise ValueError(f"output size actual={expected_size} expected=positive: {source}")
        expected_checksum = file_checksum(source)
        temporary = str(destination) + ".tmp." + proc_id
        try:
            self.mkdir_parent(destination)
            if is_remote(destination):
                self._run(["xrdcp", "--nopbar", "--force", str(source), temporary])
            else:
                shutil.copyfile(source, local_path(temporary))
            try:
                actual_size = self.size(temporary)
            except (OSError, RuntimeError, ValueError) as exc:
                raise RuntimeError(
                    f"upload size actual=unavailable expected={expected_size}: {temporary}: {exc}") from exc
            if actual_size != expected_size:
                raise RuntimeError(
                    f"upload size actual={actual_size} expected={expected_size}: {temporary}")
            try:
                actual_checksum = self.checksum(temporary)
            except (OSError, RuntimeError, ValueError) as exc:
                raise RuntimeError(
                    f"upload Adler-32 actual=unavailable expected={expected_checksum}: {temporary}: {exc}") from exc
            if actual_checksum != expected_checksum:
                raise RuntimeError(
                    f"upload Adler-32 actual={actual_checksum} expected={expected_checksum}: {temporary}")
            if is_remote(destination):
                endpoint, target = _remote_parts(destination)
                _, source_lfn = _remote_parts(temporary)
                try:
                    self.remove(destination)
                except FileNotFoundError:
                    pass
                self._run(["xrdfs", endpoint, "mv", source_lfn, target])
            else:
                os.replace(local_path(temporary), local_path(destination))
        except BaseException:
            try:
                self.remove(temporary)
            except FileNotFoundError:
                pass
            except (OSError, RuntimeError) as exc:
                logging.warning("could not remove temporary upload %s: %s", temporary, exc)
            raise
        return expected_size, expected_checksum

    def copy_in(self, source, destination):
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if is_remote(source):
            self._run(["xrdcp", "--nopbar", "--force", str(source), str(destination)])
        elif local_path(source).resolve() != destination.resolve():
            shutil.copy2(local_path(source), destination)
        if destination.stat().st_size <= 0:
            raise RuntimeError(f"empty input: {source}")
        return destination

    def read_json(self, url):
        if not is_remote(url):
            return json.loads(local_path(url).read_text())
        with tempfile.TemporaryDirectory(prefix="record-", dir=_transfer_scratch()) as tmp:
            path = self.copy_in(url, Path(tmp)/"record.json")
            return json.loads(path.read_text())

    def remove(self, url):
        if is_remote(url):
            endpoint, lfn = _remote_parts(url)
            if lfn in ("/", "/store", "/eos"):
                raise ValueError("refusing broad removal")
            self._run(["xrdfs", endpoint, "rm", lfn])
        else:
            local_path(url).unlink(missing_ok=True)

def file_checksum(path):
    checksum = 1
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum = zlib.adler32(chunk, checksum)
    return f"{checksum:08x}"


def materialize_input(reference, work_dir, transfer=None):
    url = str(reference["url"])
    expected = reference.get("size_bytes")
    cached = reference.get("local_path")
    if cached and Path(cached).is_file() and Path(cached).stat().st_size == expected:
        return Path(cached).resolve()
    direct = not is_remote(url) and local_path(url).is_file()
    if direct:
        destination = local_path(url).resolve()
    else:
        name = Path(urlsplit(url).path).name
        # Artifact IDs distinguish equally named inputs from different source pools.
        identifier = str(reference.get("artifact_id", "input")).replace("/", "_").replace(":", "_")
        destination = (transfer or TransferClient()).copy_in(
            url, Path(work_dir)/"inputs"/identifier/name)
    # A producer manifest is not proof that this copy is intact: a truncated or
    # stale file would otherwise be showered silently.  A mismatch is a hard
    # failure, so remove the bad copy rather than leave it to be reused.
    if expected is not None and destination.stat().st_size != int(expected):
        if not direct:
            destination.unlink(missing_ok=True)
        raise RuntimeError(f"downloaded size disagrees with {reference['artifact_id']}")
    return destination

def artifact_ref(path, artifact_id, kind, events=None, url=None):
    value = {"artifact_id":artifact_id, "url":url or str(path), "kind":kind,
             "size_bytes":Path(path).stat().st_size}
    if events is not None:
        value["events"] = int(events)
    return value

def validated_record(ref, client=None, record=None):
    client = client or TransferClient()
    try:
        record = record if record is not None else client.read_json(
            ref.get("sidecar_url", str(ref["url"]) + ".json"))
        if record.get("status") not in ("ok", "partial") or record.get("artifact_id") != ref["artifact_id"]:
            return None
        if record.get("url") != ref["url"] or not record.get("complete", False):
            return None
        for key in ("events", "size_bytes", "kind", "runtime_release", "count_source"):
            if key in ref and record.get(key) != ref[key]:
                return None
        size = client.size(ref["url"])
        if size <= 0 or size != record.get("size_bytes"):
            return None
        if "adler32" in ref and (record.get("adler32") != ref["adler32"]
                                 or client.checksum(ref["url"]) != ref["adler32"]):
            return None
        return record
    except (FileNotFoundError, ValueError, KeyError):
        return None


def artifact_ready(ref, client=None):
    return validated_record(ref, client) is not None
