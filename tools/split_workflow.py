#!/usr/bin/env python3
"""Manifest-driven CERN MIX / IHEP MERGE+NTUPLE workflow support."""
from __future__ import annotations
import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = "full-mc-production-split-v1"


def atomic_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def remote_parts(url):
    match = re.match(r"^(root://[^/]+)/(.*)$", url)
    return None if not match else (match.group(1) + "/", "/" + match.group(2).lstrip("/"))


def read_json_url(url):
    if not url:
        return None
    temporary = ""
    try:
        if url.startswith("root://"):
            Path("/tmp/chiw").mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(prefix="split_audit_", suffix=".json", dir="/tmp/chiw")
            os.close(fd)
            result = subprocess.run(["xrdcp", "--nopbar", "-f", url, temporary], check=False,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if result.returncode:
                return None
            path = Path(temporary)
        else:
            path = Path(url[5:] if url.startswith("file:") else url)
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except (OSError, json.JSONDecodeError):
        return None
    finally:
        if temporary:
            Path(temporary).unlink(missing_ok=True)


def processing_ok(payload):
    if not isinstance(payload, dict) or payload.get("failure_reason"):
        return False
    return ((payload.get("status") == "ok" and payload.get("complete") is True)
            or (payload.get("status") == "partial" and payload.get("merge_eligible") is True))


def selected(available, requested):
    names = sorted(set(available))
    if not requested:
        return names
    missing = sorted(set(requested) - set(names))
    if missing:
        raise ValueError("unknown campaigns: " + ", ".join(missing))
    return [name for name in names if name in set(requested)]


def export_mix_manifest(workspace, output, campaigns=None):
    root = Path(workspace).resolve()
    paths = sorted(root.glob("plan_subdags/*/job_*/coord_manifest_*.json"))
    if not paths:
        raise ValueError(f"no coordinator manifests below {root}")
    coords = []
    for path in paths:
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid coordinator manifest {path}: {exc}") from exc
        if not item.get("miniaod_merge_enabled"):
            raise ValueError(f"{path}: missing merge plan")
        item["_path"] = str(path)
        coords.append(item)
    result = OrderedDict()
    failures = []
    for campaign in selected((str(x["campaign"]) for x in coords), campaigns):
        shards = sorted((x for x in coords if x["campaign"] == campaign),
                        key=lambda x: int(x["job_index"]))
        components, merges, ntuples = OrderedDict(), [], []
        for shard in shards:
            ntuple_by_job = {str(x["job_id"]): x for x in shard.get("ntuples", [])}
            for group in shard.get("merge_groups", []):
                inputs = []
                for component in group["components"]:
                    record = {
                        "block_index": component["block_index"],
                        "job_id": component["job_id"],
                        "url": component["miniaod_url"],
                        "manifest_url": component["processing_manifest_url"],
                        "expected_events": component.get("expected_events"),
                        "packing_weight_events": component.get("packing_weight_events"),
                        "inputs": component.get("inputs", []),
                        "sources": component.get("sources", []),
                    }
                    inputs.append(record)
                    components.setdefault(str(record["job_id"]), record)
                job_id = str(group["job_id"])
                planned_ntuple = ntuple_by_job.get(job_id)
                if not planned_ntuple:
                    raise ValueError(f"{campaign}:{job_id}: missing ntuple plan")
                merged = str(group["merged_miniaod_url"])
                merge_manifest = merged.rsplit("/", 1)[0] + f"/merge_manifest_{campaign}_{job_id}.json"
                merges.append({
                    "task_id": f"{campaign}:{job_id}", "campaign": campaign, "job_id": job_id,
                    "manifest_url": merge_manifest,
                    "config": {
                        "campaign": campaign, "job_id": job_id, "input_miniaods": inputs,
                        "expected_events": group.get("expected_events"),
                        "packing_weight_events": group.get("packing_weight_events"),
                        "require_processing_manifests": True, "output_url": merged,
                        "max_size": 5000000,
                        "validation": shard.get("miniaod_merge_validation", "event-count"),
                        "storage": shard.get("storage", {}),
                    },
                })
                ntuple_url = str(planned_ntuple["ntuple_url"])
                ntuples.append({
                    "task_id": f"{campaign}:{job_id}", "campaign": campaign, "job_id": job_id,
                    "ntuple_url": ntuple_url,
                    "manifest_url": ntuple_url.rsplit("/", 1)[0] + f"/split_ntuple_manifest_{campaign}_{job_id}.json",
                    "config": {
                        "analysis": shard["analysis_type"], "campaign": campaign, "job_id": job_id,
                        "max_events": int(shard.get("processing_max_events", -1)),
                        "efficiency_ntuple": bool(shard.get("efficiency_ntuple", False)),
                        "cleanup": bool(shard.get("cleanup", True)),
                        "miniaod_input": planned_ntuple["miniaod_input"], "local_output_base": "",
                        "target_eos_base": shard.get("target_eos_base", ""),
                        "custom_output_subpath": "", "custom_ntuple_basename": "",
                        "storage": shard.get("storage", {}),
                    },
                })
        for component in components.values():
            if not processing_ok(read_json_url(component["manifest_url"])):
                failures.append(f"{campaign}:{component['job_id']}:{component['manifest_url']}")
        if not merges:
            raise ValueError(f"{campaign}: no merge tasks")
        result[campaign] = {"coordinator_manifests": [x["_path"] for x in shards],
                            "components": list(components.values()),
                            "merge_tasks": merges, "ntuple_tasks": ntuples}
    if failures:
        raise ValueError(f"{len(failures)} processing manifests are not merge-eligible:\n"
                         + "\n".join(failures[:20]))
    payload = {"schema": SCHEMA, "created_at": datetime.now(timezone.utc).isoformat(),
               "source_workspace": str(root), "validation": "processing-manifest",
               "campaigns": result}
    atomic_json(Path(output).resolve(), payload)
    return payload


def load_split(path, campaigns=None):
    try:
        payload = json.loads(Path(path).resolve().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid split manifest: {exc}") from exc
    if payload.get("schema") != SCHEMA:
        raise ValueError(f"unsupported split schema: {payload.get('schema')!r}")
    mapping = payload.get("campaigns")
    if not isinstance(mapping, dict) or not mapping:
        raise ValueError("split manifest campaigns must be non-empty")
    return payload, selected(mapping, campaigns)


def quote(value):
    return shlex.quote(str(value))


def executable(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


def proxy_refresh(root, proxy):
    source = quote(proxy) if proxy else "${X509_USER_PROXY:-/tmp/x509up_u$(id -u)}"
    bundle = quote(root / "bundles/proxy_bundle.tar.gz")
    return f"""PROXY_SOURCE={source}
[[ -s "${{PROXY_SOURCE}}" ]] || {{ echo "ERROR: missing proxy" >&2; exit 2; }}
if command -v voms-proxy-info >/dev/null 2>&1; then
  LEFT=$(voms-proxy-info -file "${{PROXY_SOURCE}}" --timeleft 2>/dev/null || echo 0)
  (( LEFT >= 600 )) || {{ echo "ERROR: proxy expires in under 10 minutes" >&2; exit 2; }}
fi
TMP=$(mktemp -d {quote(root / '.proxy.XXXXXX')})
trap 'rm -rf "${{TMP}}"' EXIT
mkdir -p "${{TMP}}/credentials"
install -m 600 "${{PROXY_SOURCE}}" "${{TMP}}/credentials/x509_user_proxy"
tar -czf {bundle}.tmp -C "${{TMP}}" credentials
chmod 600 {bundle}.tmp
mv -f {bundle}.tmp {bundle}
""".replace("\n+", "\n")


def submit_text(stage, campaign, count, wrapper, root, group, walltime, memory, proxy, gate=None):
    check = "" if not gate else f'[[ -s {quote(gate)} ]] || {{ echo "ERROR: missing gate {gate}" >&2; exit 3; }}\n'
    mem = f" -mem {memory}" if memory > 0 else ""
    logs = root / "logs" / stage / campaign
    return f"""#!/bin/bash
set -euo pipefail
source ~/.bashrc 2>/dev/null || true
{check}{proxy_refresh(root, proxy)}
mkdir -p {quote(logs)}
hep_sub {quote(wrapper)} -g {quote(group)} -gwn CMS -wt {quote(walltime)}{mem} \\
  -argu "%{{ProcId}}" -n {count} \\
  -o {quote(logs)}/{stage}_%{{ProcId}}.out \\
  -e {quote(logs)}/{stage}_%{{ProcId}}.err
"""


def prepare_workspace(split_manifest, output_dir, repo_base, ntuple_bundle, campaigns=None,
                      group="cms", merge_walltime="short", ntuple_walltime="mid",
                      merge_memory=0, ntuple_memory=0, proxy=""):
    payload, names = load_split(split_manifest, campaigns)
    root, repo = Path(output_dir).resolve(), Path(repo_base).resolve()
    root.mkdir(parents=True, exist_ok=True)
    (root / "bundles").mkdir(exist_ok=True)
    (root / "gates").mkdir(exist_ok=True)
    source_bundle = Path(ntuple_bundle).resolve()
    bundle = root / "bundles" / source_bundle.name
    if source_bundle != bundle:
        shutil.copy2(source_bundle, bundle)
    worker = repo / "processing/condor_wrappers/run_ihep_split_task.sh"
    metadata = OrderedDict()
    for campaign in names:
        data = payload["campaigns"][campaign]
        task_dir = root / "tasks" / campaign
        merge_tasks, ntuple_tasks = task_dir / "merge_tasks.json", task_dir / "ntuple_tasks.json"
        atomic_json(merge_tasks, data["merge_tasks"])
        atomic_json(ntuple_tasks, data["ntuple_tasks"])
        wrappers = {}
        for stage, tasks in (("merge", merge_tasks), ("ntuple", ntuple_tasks)):
            wrapper = root / "scripts" / f"run_{stage}_{campaign}.sh"
            executable(wrapper, "#!/bin/bash\nset -euo pipefail\n"
                       + f"exec {quote(worker)} {quote(stage)} \"$1\" {quote(tasks)} "
                       + f"{quote(root / 'bundles/proxy_bundle.tar.gz')} {quote(bundle)}\n")
            wrappers[stage] = wrapper
        submit_merge, submit_ntuple = root / f"submit_merge_{campaign}.sh", root / f"submit_ntuple_{campaign}.sh"
        executable(submit_merge, submit_text("merge", campaign, len(data["merge_tasks"]),
                                             wrappers["merge"], root, group, merge_walltime,
                                             merge_memory, proxy))
        executable(submit_ntuple, submit_text("ntuple", campaign, len(data["ntuple_tasks"]),
                                              wrappers["ntuple"], root, group, ntuple_walltime,
                                              ntuple_memory, proxy,
                                              root / "gates" / f"merge_{campaign}.json"))
        metadata[campaign] = {"merge_tasks": str(merge_tasks), "ntuple_tasks": str(ntuple_tasks),
                              "submit_merge": str(submit_merge), "submit_ntuple": str(submit_ntuple)}
    shutil.copy2(Path(split_manifest).resolve(), root / "split_manifest.json")
    result = {"schema": SCHEMA, "split_manifest": str(root / "split_manifest.json"),
              "repo_base": str(repo), "worker": str(worker), "ntuple_bundle": str(bundle),
              "settings": {"group": group, "merge_walltime": merge_walltime,
                           "ntuple_walltime": ntuple_walltime,
                           "merge_memory": merge_memory, "ntuple_memory": ntuple_memory,
                           "proxy": proxy},
              "campaigns": metadata}
    atomic_json(root / "split_workspace.json", result)
    return result


def load_workspace(path):
    root = Path(path).resolve()
    try:
        return root, json.loads((root / "split_workspace.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid split workspace: {exc}") from exc


def valid_manifest(stage, task, payload):
    if not isinstance(payload, dict) or payload.get("failure_reason"):
        return False
    if stage == "merge":
        return (payload.get("status") in {"ok", "partial"}
                and payload.get("merge_eligible") is True
                and payload.get("output_url") == task["config"]["output_url"])
    return payload.get("status") == "ok" and payload.get("ntuple_url") == task["ntuple_url"]


def audit_stage(workspace, stage, campaigns=None):
    if stage not in {"merge", "ntuple"}:
        raise ValueError("workspace stage must be merge or ntuple")
    root, metadata = load_workspace(workspace)
    results, complete = OrderedDict(), True
    for campaign in selected(metadata["campaigns"], campaigns):
        tasks_path = Path(metadata["campaigns"][campaign][f"{stage}_tasks"])
        tasks = json.loads(tasks_path.read_text(encoding="utf-8"))
        failed = [task for task in tasks
                  if not valid_manifest(stage, task, read_json_url(task["manifest_url"]))]
        gate = root / "gates" / f"{stage}_{campaign}.json"
        gate.unlink(missing_ok=True)
        retry = root / "tasks" / campaign / f"{stage}_retry_tasks.json"
        retry_submit = root / f"submit_{stage}_{campaign}_retry.sh"
        if failed:
            complete = False
            atomic_json(retry, failed)
            retry_wrapper = root / "scripts" / f"run_{stage}_{campaign}_retry.sh"
            executable(
                retry_wrapper,
                "#!/bin/bash\nset -euo pipefail\n"
                + f"exec {quote(metadata['worker'])} {quote(stage)} \"$1\" {quote(retry)} "
                + f"{quote(root / 'bundles/proxy_bundle.tar.gz')} {quote(metadata['ntuple_bundle'])}\n",
            )
            settings = metadata["settings"]
            executable(
                retry_submit,
                submit_text(
                    stage, campaign, len(failed), retry_wrapper, root,
                    settings["group"], settings[f"{stage}_walltime"],
                    int(settings[f"{stage}_memory"]), settings.get("proxy", ""),
                    root / "gates" / f"merge_{campaign}.json" if stage == "ntuple" else None,
                ),
            )
        else:
            retry.unlink(missing_ok=True)
            retry_submit.unlink(missing_ok=True)
            atomic_json(gate, {"stage": stage, "campaign": campaign, "status": "complete",
                               "tasks": len(tasks), "checked_at": datetime.now(timezone.utc).isoformat()})
        results[campaign] = {"total": len(tasks), "passed": len(tasks) - len(failed),
                             "failed": len(failed), "retry_manifest": str(retry) if failed else "",
                             "retry_submit": str(retry_submit) if failed else ""}
    return {"stage": stage, "complete": complete, "campaigns": results}


def remove_url(url):
    parts = remote_parts(url)
    if parts:
        subprocess.run(["xrdfs", parts[0], "rm", parts[1]], check=True)
    else:
        Path(url[5:] if url.startswith("file:") else url).unlink(missing_ok=True)


def finalize(workspace, campaigns=None, apply=False):
    root, metadata = load_workspace(workspace)
    split, _ = load_split(metadata["split_manifest"])
    records = []
    for campaign in selected(metadata["campaigns"], campaigns):
        gate = root / "gates" / f"ntuple_{campaign}.json"
        if not gate.is_file():
            raise ValueError(f"missing ntuple audit gate: {gate}")
        for component in split["campaigns"][campaign]["components"]:
            record = {"campaign": campaign, "job_id": component["job_id"],
                      "url": component["url"], "action": "would-delete"}
            if apply:
                try:
                    remove_url(component["url"])
                    record["action"] = "deleted"
                except (OSError, subprocess.CalledProcessError) as exc:
                    record.update(action="failed", error=str(exc))
            records.append(record)
    result = {"apply": apply, "created_at": datetime.now(timezone.utc).isoformat(), "records": records}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    atomic_json(root / "logs/cleanup" / f"cleanup_{stamp}.json", result)
    return result
