"""Exact local/XRootD transfers and compact product records."""
from __future__ import annotations
import json
import os
import shutil
import subprocess
import tempfile
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

class TransferClient:
    def __init__(self, runner=None):
        self.runner = runner

    def _run(self, command):
        if self.runner:
            result = self.runner(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        else:
            from full_mc.runtime.profiles import run_profile
            scratch = Path("/tmp/chiw/mcprod-transfer")
            binds = [str(local_path(arg).resolve().parent) for arg in command[1:]
                     if str(arg).startswith("/") or str(arg).startswith("file:")]
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
        Path("/tmp/chiw").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="mcprod-record-", dir="/tmp/chiw") as tmp:
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

def materialize_input(reference, work_dir, transfer=None):
    url = str(reference["url"])
    cached = reference.get("local_path")
    if cached and Path(cached).is_file() and Path(cached).stat().st_size == reference.get("size_bytes"):
        return Path(cached).resolve()
    if not is_remote(url) and local_path(url).is_file():
        return local_path(url).resolve()
    name = Path(urlsplit(url).path).name
    # Artifact IDs distinguish equally named inputs from different source pools.
    identifier = str(reference.get("artifact_id", "input")).replace("/", "_").replace(":", "_")
    return (transfer or TransferClient()).copy_in(url, Path(work_dir)/"inputs"/identifier/name)

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
        return record if size > 0 and size == record.get("size_bytes") else None
    except (FileNotFoundError, ValueError, KeyError):
        return None


def artifact_ready(ref, client=None):
    return validated_record(ref, client) is not None
