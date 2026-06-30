#!/usr/bin/env python3
"""
coordinate_lhe_blocks.py — Campaign-level multi-source LHE block coordinator.

Runs as a Condor job after all per-pool LHE planners complete for a campaign
job index. Reads plan manifests from each source, matches blocks across sources
with strict-min policy, and generates a SubDAG where each MIX_BLOCK node
processes one block from every required source.

Usage:
  python3 coordinate_lhe_blocks.py \\
      --campaign JJP_DPS2_CS --job-index 0 \\
      --source-manifests '[{"pool":"pool_2jpsi_cs","seed":100,"path":"/.../plan_manifest.json"},...]' \\
      --shower-modes "normal,phi_mpi_off" --analysis-type JJP --n-sources 2 \\
      --output-dir /path/to/subdag/output \\
      --processing-sub-template-path /path/to/processing.sub \\
      --processing-bundle-path /path/to/processing_runtime_bundle.tar.gz \\
      ...
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Multi-source LHE block coordinator")
    p.add_argument("--campaign", required=True)
    p.add_argument("--job-index", type=int, required=True)
    p.add_argument("--source-manifests", required=True,
                   help="JSON string: [{'pool':'A','seed':100,'path':'...'}, ...]")
    p.add_argument("--shower-modes", required=True, help="Comma-separated shower modes")
    p.add_argument("--campaign-inputs", required=True,
                   help="Comma-separated campaign input pool names (with duplicates)")
    p.add_argument("--analysis-type", required=True, help="JJP or JUP")
    p.add_argument("--n-sources", type=int, required=True)
    p.add_argument("--max-events", type=int, default=-1)
    p.add_argument("--enable-ntuple", action="store_true")
    p.add_argument("--efficiency-ntuple", action="store_true")
    p.add_argument("--cleanup", action="store_true")
    p.add_argument("--shuffle-mixing", action="store_true")
    p.add_argument("--log-root", default=".")
    p.add_argument("--request-cpus", default="8")
    p.add_argument("--request-memory", default="20GB")
    p.add_argument("--request-disk", default="50GB")
    p.add_argument("--target-machine", default="")
    p.add_argument("--target-eos-base", default="",
                   help="EOS base for block lookup and output staging")
    p.add_argument("--output-dir", required=True, help="Directory for coordinator output")
    p.add_argument("--processing-sub-template-path", required=True,
                   help="Path to processing.sub for the SubDAG JOB lines")
    p.add_argument("--processing-bundle-path", required=True)
    p.add_argument("--processing-bundle-name", required=True)
    p.add_argument("--proxy-bundle-path", required=True)
    p.add_argument("--proxy-bundle-name", required=True)
    p.add_argument("--processing-wrapper-path", required=True)
    p.add_argument("--ntuple-sub-template-path", default="")
    p.add_argument("--ntuple-bundle-path", default="")
    p.add_argument("--ntuple-bundle-name", default="")
    p.add_argument("--ntuple-wrapper-path", default="")
    p.add_argument("--miniaod-merge-events", type=int, default=0,
                   help="Target events per merged MiniAOD; 0 disables merge")
    p.add_argument("--miniaod-merge-validation", default="event-count",
                   choices=("none", "event-count"))
    p.add_argument("--max-miniaod-merge-jobs", type=int, default=10,
                   help="MAXJOBS miniaod_merge throttle inside the SubDAG")
    p.add_argument("--miniaod-merge-sub-template-path", default="")
    p.add_argument("--miniaod-merge-wrapper-path", default="")
    p.add_argument("--final-sub-template-path", default="")
    p.add_argument("--final-wrapper-path", default="")
    p.add_argument("--subdag-output-path", required=True,
                   help="Full path for the output blocks_processing.dag")
    p.add_argument("--max-block-subdag-jobs", type=int, default=10,
                   help="MAXJOBS block_processing throttle")
    p.add_argument("--local-output-base", default="",
                   help="Local storage base (for local condor mode)")
    p.add_argument("--storage-config", default="{}",
                   help="JSON object with explicit storage roots for node configs")
    p.add_argument("--processing-environment-config", default="{}",
                   help="JSON object with processing environment defaults")
    return p.parse_args()


def dag_escape(value) -> str:
    """Escape a value for use in a DAG VARS double-quoted string."""
    s = str(value)
    s = s.replace("\\", "\\\\")
    s = s.replace('"', '\\"')
    return s


def bool_str(flag: bool) -> str:
    return "true" if flag else "false"


def write_json_file(path: str, payload: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    os.rename(tmp, path)


def log_directory(log_root: str, *parts: object) -> str:
    """Return and create one deterministic leaf directory for Condor logs."""
    path = os.path.join(log_root, *(str(part) for part in parts))
    os.makedirs(path, exist_ok=True)
    return path


def output_base(args: argparse.Namespace, storage_config: dict) -> str:
    return str(
        args.target_eos_base
        or storage_config.get("target_eos_base")
        or storage_config.get("default_eos_base")
        or ""
    ).rstrip("/")


def block_job_id(job_index: int, block_index: int) -> str:
    return f"JOB{job_index:06d}_BLOCK{block_index:06d}"


def merge_job_id(job_index: int, merge_index: int) -> str:
    return f"JOB{job_index:06d}_MERGE{merge_index:06d}"


def miniaod_url(base: str, campaign: str, job_id: str) -> str:
    return f"{base}/output/{campaign}/{job_id}/output_MINIAOD.root"


def ntuple_url(base: str, campaign: str, job_id: str) -> str:
    return f"{base}/output/{campaign}/{job_id}/output_ntuple.root"


def main() -> int:
    args = parse_args()

    # --- 1. Parse source manifests ---
    try:
        source_infos = json.loads(args.source_manifests)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid --source-manifests JSON: {e}", file=sys.stderr)
        return 1
    try:
        storage_config = json.loads(args.storage_config)
        processing_environment_config = json.loads(args.processing_environment_config)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid node config JSON: {e}", file=sys.stderr)
        return 1
    if not isinstance(storage_config, dict):
        print("[ERROR] --storage-config must be a JSON object", file=sys.stderr)
        return 1
    if not isinstance(processing_environment_config, dict):
        print("[ERROR] --processing-environment-config must be a JSON object", file=sys.stderr)
        return 1

    if len(source_infos) == 0:
        print("[ERROR] No source manifests provided", file=sys.stderr)
        return 1

    # --- 2. Read plan manifests ---
    source_blocks: list = []  # list of (pool, group_id, primary_seed, seeds, [block_dict, ...])
    for info in source_infos:
        manifest_path = info["path"]
        if not os.path.exists(manifest_path):
            print(f"[ERROR] Manifest not found: {manifest_path}", file=sys.stderr)
            return 1
        with open(manifest_path, "r") as f:
            m = json.load(f)
        primary_seed = info.get("primary_seed", info.get("seed", m.get("primary_seed", m.get("helac_seed"))))
        group_id = info.get("group_id", m.get("group_id", str(primary_seed)))
        seeds = info.get("seeds", m.get("seeds", [primary_seed]))
        source_blocks.append((info["pool"], group_id, primary_seed, seeds, m.get("blocks", [])))
        print(f"[INFO] Source {info['pool']} (group={group_id}, primary_seed={primary_seed}): "
              f"{len(m.get('blocks', []))} blocks")

    # --- 3. Resolve campaign input multiplicity and strict-min block count ---
    campaign_inputs = [p.strip() for p in args.campaign_inputs.split(",") if p.strip()]
    shower_modes = [m.strip() for m in args.shower_modes.split(",")]
    if len(campaign_inputs) != args.n_sources:
        print(f"[ERROR] --campaign-inputs count ({len(campaign_inputs)}) != --n-sources ({args.n_sources})",
              file=sys.stderr)
        return 1
    if len(shower_modes) != args.n_sources:
        print(f"[ERROR] --shower-modes count ({len(shower_modes)}) != --n-sources ({args.n_sources})",
              file=sys.stderr)
        return 1

    # Build pool→source lookup for block generation
    pool_lookup: dict = {}
    for pool, group_id, primary_seed, seeds, blocks in source_blocks:
        if pool in pool_lookup:
            print(f"[ERROR] Duplicate source manifest for pool: {pool}", file=sys.stderr)
            return 1
        pool_lookup[pool] = {
            "group_id": group_id,
            "primary_seed": primary_seed,
            "seeds": seeds,
            "blocks": blocks,
        }

    missing_pools = sorted(set(campaign_inputs) - set(pool_lookup))
    if missing_pools:
        print(
            f"[ERROR] --campaign-inputs references missing pools: {','.join(missing_pools)}",
            file=sys.stderr,
        )
        return 1

    input_multiplicity = Counter(campaign_inputs)
    n_blocks_by_pool = {pool: len(source["blocks"]) for pool, source in pool_lookup.items()}
    n_mixed = min(
        n_blocks_by_pool[pool] // input_multiplicity[pool]
        for pool in input_multiplicity
    )
    if n_mixed == 0:
        msg = "no common blocks across campaign inputs ("
        msg += ", ".join(
            f"{pool}={n_blocks_by_pool[pool]} blocks/{input_multiplicity[pool]} uses"
            for pool in input_multiplicity
        )
        msg += ")"
        print(f"[ERROR] {msg}", file=sys.stderr)
        return 1
    print(f"[INFO] Strict-min with duplicate inputs: {n_mixed} mixed blocks")

    # Record unused blocks after duplicate source offsets are consumed.
    unused = []
    for pool, group_id, primary_seed, seeds, blocks in source_blocks:
        used_count = n_mixed * input_multiplicity.get(pool, 0)
        leftover = list(range(used_count, len(blocks)))
        if leftover:
            unused.append({"pool": pool, "group_id": group_id, "primary_seed": primary_seed,
                           "seeds": seeds, "unused_indices": leftover,
                           "unused_count": len(leftover)})
            print(f"[INFO] {pool}: {len(leftover)} unused blocks (indices {leftover[0]}-{leftover[-1]})")

    def mixed_block_inputs(block_index: int) -> list:
        occurrence_seen = defaultdict(int)
        inputs = []
        for pool_name in campaign_inputs:
            occurrence = occurrence_seen[pool_name]
            occurrence_seen[pool_name] += 1
            source = pool_lookup[pool_name]
            source_block_index = block_index * input_multiplicity[pool_name] + occurrence
            block = source["blocks"][source_block_index]
            inputs.append({
                "pool": pool_name,
                "group_id": source["group_id"],
                "primary_seed": source["primary_seed"],
                "seeds": source["seeds"],
                "block_index": source_block_index,
                "occurrence": occurrence,
                "n_events": int(block.get("n_events", 0) or 0),
                "path": block.get("path", ""),
            })
        return inputs

    def mixed_block_event_count(block_index: int) -> int:
        counts = [
            int(item.get("n_events", 0) or 0)
            for item in mixed_block_inputs(block_index)
            if int(item.get("n_events", 0) or 0) > 0
        ]
        return min(counts) if counts else 0

    # --- 4. Generate SubDAG ---
    os.makedirs(os.path.dirname(args.subdag_output_path), exist_ok=True)
    merge_enabled = (
        args.enable_ntuple
        and bool(args.ntuple_sub_template_path)
        and args.miniaod_merge_events > 0
    )
    if args.enable_ntuple and args.ntuple_sub_template_path:
        ntuple_target_base = (
            args.target_eos_base
            or storage_config.get("target_eos_base")
            or storage_config.get("default_eos_base")
        )
        if not ntuple_target_base:
            print(
                "[ERROR] Ntuple SubDAG generation requires target_eos_base in args or storage config",
                file=sys.stderr,
            )
            return 1
    if merge_enabled and (
        not args.miniaod_merge_sub_template_path
        or not args.miniaod_merge_wrapper_path
    ):
        print("[ERROR] MiniAOD merge mode requires merge submit template and wrapper paths", file=sys.stderr)
        return 1
    if not args.final_sub_template_path or not args.final_wrapper_path:
        print("[ERROR] Final inventory requires final submit template and wrapper paths", file=sys.stderr)
        return 1

    target_base = output_base(args, storage_config)
    node_config_dir = os.path.join(args.output_dir, "node_configs")
    processing_config_dir = os.path.join(node_config_dir, "processing")
    merge_config_dir = os.path.join(node_config_dir, "miniaod_merge")
    ntuple_config_dir = os.path.join(node_config_dir, "ntuple")
    final_config_dir = os.path.join(node_config_dir, "final")
    os.makedirs(processing_config_dir, exist_ok=True)
    os.makedirs(final_config_dir, exist_ok=True)
    if merge_enabled:
        os.makedirs(merge_config_dir, exist_ok=True)
    if args.enable_ntuple and args.ntuple_sub_template_path:
        os.makedirs(ntuple_config_dir, exist_ok=True)

    block_records = []
    for i in range(n_mixed):
        jid = block_job_id(args.job_index, i)
        block_records.append({
            "block_index": i,
            "job_id": jid,
            "expected_events": mixed_block_event_count(i),
            "inputs": mixed_block_inputs(i),
            "miniaod_url": miniaod_url(target_base, args.campaign, jid),
        })

    merge_groups = []
    if merge_enabled:
        current = []
        current_events = 0
        for record in block_records:
            if current and current_events >= args.miniaod_merge_events:
                merge_groups.append(current)
                current = []
                current_events = 0
            current.append(record)
            current_events += int(record.get("expected_events", 0) or 0)
        if current:
            merge_groups.append(current)
    merge_records = []
    ntuple_records = []
    if merge_enabled:
        for merge_index, components in enumerate(merge_groups):
            jid = merge_job_id(args.job_index, merge_index)
            merged_url = miniaod_url(target_base, args.campaign, jid)
            merge_records.append({
                "merge_index": merge_index,
                "job_id": jid,
                "expected_events": sum(int(item.get("expected_events", 0) or 0) for item in components),
                "component_block_ids": [item["job_id"] for item in components],
                "components": components,
                "merged_miniaod_url": merged_url,
            })
            ntuple_records.append({
                "merge_index": merge_index,
                "job_id": jid,
                "miniaod_input": merged_url,
                "ntuple_url": ntuple_url(target_base, args.campaign, jid),
            })
    elif args.enable_ntuple and args.ntuple_sub_template_path:
        ntuple_records = [
            {
                "block_index": record["block_index"],
                "job_id": record["job_id"],
                "miniaod_input": record["miniaod_url"],
                "ntuple_url": ntuple_url(target_base, args.campaign, record["job_id"]),
            }
            for record in block_records
        ]
    dag_tmp = args.subdag_output_path + ".tmp"

    with open(dag_tmp, "w") as dag:
        dag.write("# ================================================\n")
        dag.write(f"# Block processing SubDAG\n")
        dag.write(f"# Campaign: {args.campaign}  Job index: {args.job_index}\n")
        dag.write(f"# Generated: {datetime.now(timezone.utc).isoformat()}\n")
        dag.write(f"# Tool: coordinate_lhe_blocks.py\n")
        dag.write(f"# Strict-min: {n_mixed} mixed blocks from "
                  f"{' + '.join(f'{pool}({n_blocks_by_pool[pool]}/{input_multiplicity.get(pool, 0)} uses)' for pool, _, _, _, _ in source_blocks)}\n")
        dag.write("# ================================================\n")
        dag.write("\n")
        dag.write(f"MAXJOBS block_processing {args.max_block_subdag_jobs}\n")
        if merge_enabled:
            dag.write(f"MAXJOBS miniaod_merge {args.max_miniaod_merge_jobs}\n")
        dag.write("\n")

        for i in range(n_mixed):
            # Build inputs string from campaign inputs (with duplicates)
            input_parts = [
                f"BLOCK:{item['pool']}:{item['group_id']}:{item['block_index']:06d}"
                for item in mixed_block_inputs(i)
            ]
            modes = [mode.strip() for mode in args.shower_modes.split(",") if mode.strip()]

            node_name = f"MIX_{args.campaign}_{args.job_index}_BLOCK{i:06d}"
            block_job_id_value = block_job_id(args.job_index, i)
            processing_enable_ntuple = (
                args.enable_ntuple and not args.ntuple_sub_template_path
            )
            processing_config = {
                "inputs": input_parts,
                "modes": modes,
                "analysis": args.analysis_type,
                "campaign": args.campaign,
                "job_id": block_job_id_value,
                "max_events": args.max_events,
                "enable_ntuple": processing_enable_ntuple,
                "efficiency_ntuple": args.efficiency_ntuple,
                "cleanup": args.cleanup,
                "shuffle_mixing": args.shuffle_mixing,
                "local_output_base": args.local_output_base,
                "target_eos_base": args.target_eos_base,
                "storage": storage_config,
                "processing_environment": processing_environment_config,
            }
            processing_config_name = f"{node_name}.json"
            processing_config_path = os.path.join(processing_config_dir, processing_config_name)
            write_json_file(processing_config_path, processing_config)
            block_log_component = f"block_{i:06d}"
            processing_log_root = log_directory(
                args.log_root,
                args.campaign,
                "processing",
                f"job_{args.job_index:06d}",
                block_log_component,
            )

            dag.write(f"JOB {node_name} {args.processing_sub_template_path}\n")
            dag.write(f"CATEGORY {node_name} block_processing\n")

            vars_line = (
                f'VARS {node_name} '
                f'campaign="{dag_escape(args.campaign)}" '
                f'job_id="{dag_escape(block_job_id_value)}" '
                f'request_cpus="{dag_escape(args.request_cpus)}" '
                f'request_memory="{dag_escape(args.request_memory)}" '
                f'request_disk="{dag_escape(args.request_disk)}" '
                f'processing_bundle_path="{dag_escape(args.processing_bundle_path)}" '
                f'processing_bundle_name="{dag_escape(args.processing_bundle_name)}" '
                f'proxy_bundle_path="{dag_escape(args.proxy_bundle_path)}" '
                f'proxy_bundle_name="{dag_escape(args.proxy_bundle_name)}" '
                f'processing_wrapper_path="{dag_escape(args.processing_wrapper_path)}" '
                f'log_root="{dag_escape(processing_log_root)}" '
                f'target_machine="{dag_escape(args.target_machine)}" '
                f'config_path="{dag_escape(processing_config_path)}" '
                f'config_name="{dag_escape(processing_config_name)}"'
            )
            dag.write(vars_line + "\n")
            dag.write(f"RETRY {node_name} 1\n")

            # Ntuple node (if enabled)
            if args.enable_ntuple and args.ntuple_sub_template_path and not merge_enabled:
                ntuple_name = f"NTUPLE_{args.campaign}_{args.job_index}_BLOCK{i:06d}"
                miniaod_input = miniaod_url(target_base, args.campaign, block_job_id_value)
                ntuple_config = {
                    "analysis": args.analysis_type,
                    "campaign": args.campaign,
                    "job_id": block_job_id_value,
                    "max_events": args.max_events,
                    "efficiency_ntuple": args.efficiency_ntuple,
                    "cleanup": args.cleanup,
                    "miniaod_input": miniaod_input,
                    "local_output_base": args.local_output_base,
                    "target_eos_base": args.target_eos_base,
                    "custom_output_subpath": "",
                    "custom_ntuple_basename": "",
                    "storage": storage_config,
                }
                ntuple_config_name = f"{ntuple_name}.json"
                ntuple_config_path = os.path.join(ntuple_config_dir, ntuple_config_name)
                write_json_file(ntuple_config_path, ntuple_config)
                ntuple_log_root = log_directory(
                    args.log_root,
                    args.campaign,
                    "ntuple",
                    f"job_{args.job_index:06d}",
                    block_log_component,
                )
                dag.write(f"JOB {ntuple_name} {args.ntuple_sub_template_path}\n")
                dag.write(f"CATEGORY {ntuple_name} ntuple\n")
                ntuple_vars = (
                    f'VARS {ntuple_name} '
                    f'campaign="{dag_escape(args.campaign)}" '
                    f'job_id="{dag_escape(block_job_id_value)}" '
                    f'request_cpus="2" request_memory="12GB" request_disk="8GB" '
                    f'ntuple_bundle_path="{dag_escape(args.ntuple_bundle_path)}" '
                    f'ntuple_bundle_name="{dag_escape(args.ntuple_bundle_name)}" '
                    f'proxy_bundle_path="{dag_escape(args.proxy_bundle_path)}" '
                    f'proxy_bundle_name="{dag_escape(args.proxy_bundle_name)}" '
                    f'ntuple_wrapper_path="{dag_escape(args.ntuple_wrapper_path)}" '
                    f'ntuple_wrapper_name="{dag_escape(os.path.basename(args.ntuple_wrapper_path or "run_ntuple_only.sh"))}" '
                    f'log_root="{dag_escape(ntuple_log_root)}" '
                    f'config_path="{dag_escape(ntuple_config_path)}" '
                    f'config_name="{dag_escape(ntuple_config_name)}"'
                )
                dag.write(ntuple_vars + "\n")
                dag.write(f"RETRY {ntuple_name} 1\n")
                dag.write(f"PARENT {node_name} CHILD {ntuple_name}\n")

            dag.write("\n")

        if merge_enabled:
            for record in merge_records:
                merge_index = record["merge_index"]
                merge_name = f"MERGE_{args.campaign}_{args.job_index}_GROUP{merge_index:06d}"
                merge_log_root = log_directory(
                    args.log_root,
                    args.campaign,
                    "miniaod_merge",
                    f"job_{args.job_index:06d}",
                    f"group_{merge_index:06d}",
                )
                merge_config = {
                    "campaign": args.campaign,
                    "job_id": record["job_id"],
                    "input_miniaods": [
                        {
                            "block_index": component["block_index"],
                            "job_id": component["job_id"],
                            "url": component["miniaod_url"],
                            "expected_events": component["expected_events"],
                            "inputs": component["inputs"],
                        }
                        for component in record["components"]
                    ],
                    "expected_events": record["expected_events"],
                    "output_url": record["merged_miniaod_url"],
                    "max_size": 5000000,
                    "validation": args.miniaod_merge_validation,
                    "storage": storage_config,
                }
                merge_config_name = f"{merge_name}.json"
                merge_config_path = os.path.join(merge_config_dir, merge_config_name)
                write_json_file(merge_config_path, merge_config)
                dag.write(f"JOB {merge_name} {args.miniaod_merge_sub_template_path}\n")
                dag.write(f"CATEGORY {merge_name} miniaod_merge\n")
                dag.write(
                    f'VARS {merge_name} '
                    f'campaign="{dag_escape(args.campaign)}" '
                    f'job_id="{dag_escape(record["job_id"])}" '
                    f'request_cpus="2" request_memory="12GB" request_disk="20GB" '
                    f'proxy_bundle_path="{dag_escape(args.proxy_bundle_path)}" '
                    f'proxy_bundle_name="{dag_escape(args.proxy_bundle_name)}" '
                    f'miniaod_merge_wrapper_path="{dag_escape(args.miniaod_merge_wrapper_path)}" '
                    f'miniaod_merge_wrapper_name="{dag_escape(os.path.basename(args.miniaod_merge_wrapper_path))}" '
                    f'log_root="{dag_escape(merge_log_root)}" '
                    f'config_path="{dag_escape(merge_config_path)}" '
                    f'config_name="{dag_escape(merge_config_name)}"\n'
                )
                dag.write(f"RETRY {merge_name} 1\n")
                parent_nodes = " ".join(
                    f"MIX_{args.campaign}_{args.job_index}_BLOCK{component['block_index']:06d}"
                    for component in record["components"]
                )
                dag.write(f"PARENT {parent_nodes} CHILD {merge_name}\n")

                ntuple_name = f"NTUPLE_{args.campaign}_{args.job_index}_MERGE{merge_index:06d}"
                ntuple_log_root = log_directory(
                    args.log_root,
                    args.campaign,
                    "ntuple",
                    f"job_{args.job_index:06d}",
                    f"merge_{merge_index:06d}",
                )
                ntuple_config = {
                    "analysis": args.analysis_type,
                    "campaign": args.campaign,
                    "job_id": record["job_id"],
                    "max_events": args.max_events,
                    "efficiency_ntuple": args.efficiency_ntuple,
                    "cleanup": args.cleanup,
                    "miniaod_input": record["merged_miniaod_url"],
                    "local_output_base": args.local_output_base,
                    "target_eos_base": args.target_eos_base,
                    "custom_output_subpath": "",
                    "custom_ntuple_basename": "",
                    "storage": storage_config,
                }
                ntuple_config_name = f"{ntuple_name}.json"
                ntuple_config_path = os.path.join(ntuple_config_dir, ntuple_config_name)
                write_json_file(ntuple_config_path, ntuple_config)
                dag.write(f"JOB {ntuple_name} {args.ntuple_sub_template_path}\n")
                dag.write(f"CATEGORY {ntuple_name} ntuple\n")
                dag.write(
                    f'VARS {ntuple_name} '
                    f'campaign="{dag_escape(args.campaign)}" '
                    f'job_id="{dag_escape(record["job_id"])}" '
                    f'request_cpus="2" request_memory="12GB" request_disk="8GB" '
                    f'ntuple_bundle_path="{dag_escape(args.ntuple_bundle_path)}" '
                    f'ntuple_bundle_name="{dag_escape(args.ntuple_bundle_name)}" '
                    f'proxy_bundle_path="{dag_escape(args.proxy_bundle_path)}" '
                    f'proxy_bundle_name="{dag_escape(args.proxy_bundle_name)}" '
                    f'ntuple_wrapper_path="{dag_escape(args.ntuple_wrapper_path)}" '
                    f'ntuple_wrapper_name="{dag_escape(os.path.basename(args.ntuple_wrapper_path or "run_ntuple_only.sh"))}" '
                    f'log_root="{dag_escape(ntuple_log_root)}" '
                    f'config_path="{dag_escape(ntuple_config_path)}" '
                    f'config_name="{dag_escape(ntuple_config_name)}"\n'
                )
                dag.write(f"RETRY {ntuple_name} 1\n")
                dag.write(f"PARENT {merge_name} CHILD {ntuple_name}\n\n")

        final_name = f"FINAL_{args.campaign}_{args.job_index}"
        final_job_id = f"JOB{args.job_index:06d}_FINAL"
        final_output_url = (
            f"{target_base}/output/{args.campaign}/{final_job_id}/"
            f"subdag_inventory_{args.campaign}_{args.job_index}.json"
        )
        final_config = {
            "campaign": args.campaign,
            "job_index": args.job_index,
            "output_url": final_output_url,
            "blocks": block_records,
            "merge_groups": merge_records,
            "ntuples": ntuple_records,
        }
        final_config_name = f"{final_name}.json"
        final_config_path = os.path.join(final_config_dir, final_config_name)
        write_json_file(final_config_path, final_config)
        final_log_root = log_directory(
            args.log_root,
            args.campaign,
            "final",
            f"job_{args.job_index:06d}",
        )
        dag.write(f"FINAL {final_name} {args.final_sub_template_path}\n")
        dag.write(
            f'VARS {final_name} '
            f'campaign="{dag_escape(args.campaign)}" '
            f'job_id="{dag_escape(final_job_id)}" '
            f'request_cpus="1" request_memory="2GB" request_disk="2GB" '
            f'proxy_bundle_path="{dag_escape(args.proxy_bundle_path)}" '
            f'proxy_bundle_name="{dag_escape(args.proxy_bundle_name)}" '
            f'final_wrapper_path="{dag_escape(args.final_wrapper_path)}" '
            f'final_wrapper_name="{dag_escape(os.path.basename(args.final_wrapper_path))}" '
            f'log_root="{dag_escape(final_log_root)}" '
            f'config_path="{dag_escape(final_config_path)}" '
            f'config_name="{dag_escape(final_config_name)}"\n'
        )

    # Atomic rename
    os.rename(dag_tmp, args.subdag_output_path)
    print(f"[INFO] SubDAG written: {args.subdag_output_path}")

    # --- 5. Write coordinator manifest ---
    coord_manifest = {
        "tool": "coordinate_lhe_blocks",
        "version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "campaign": args.campaign,
        "job_index": args.job_index,
        "n_mixed_blocks": n_mixed,
        "events_per_block": args.max_events,
        "miniaod_merge_enabled": merge_enabled,
        "miniaod_merge_events": args.miniaod_merge_events,
        "miniaod_merge_validation": args.miniaod_merge_validation,
        "sources": [
            {"pool": pool, "group_id": group_id, "primary_seed": primary_seed,
             "seeds": seeds, "n_blocks": len(blocks),
             "n_used": n_mixed * input_multiplicity.get(pool, 0),
             "multiplicity": input_multiplicity.get(pool, 0)}
            for pool, group_id, primary_seed, seeds, blocks in source_blocks
        ],
        "mixed_blocks": [
            {
                "index": i,
                "expected_events": mixed_block_event_count(i),
                "miniaod_url": miniaod_url(target_base, args.campaign, block_job_id(args.job_index, i)),
                "inputs": mixed_block_inputs(i),
            }
            for i in range(n_mixed)
        ],
        "merge_groups": merge_records,
        "ntuples": ntuple_records,
        "unused": unused,
    }
    coord_manifest_path = os.path.join(
        args.output_dir,
        f"coord_manifest_{args.campaign}_{args.job_index}.json"
    )
    coord_tmp = coord_manifest_path + ".tmp"
    with open(coord_tmp, "w") as f:
        json.dump(coord_manifest, f, indent=2)
    os.rename(coord_tmp, coord_manifest_path)
    print(f"[INFO] Coordinator manifest written: {coord_manifest_path}")

    print(f"[OK] Coordination complete: {n_mixed} mixed blocks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
