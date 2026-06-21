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
    p.add_argument("--subdag-output-path", required=True,
                   help="Full path for the output blocks_processing.dag")
    p.add_argument("--max-block-subdag-jobs", type=int, default=10,
                   help="MAXJOBS block_processing throttle")
    p.add_argument("--local-output-base", default="",
                   help="Local storage base (for local condor mode)")
    return p.parse_args()


def dag_escape(value) -> str:
    """Escape a value for use in a DAG VARS double-quoted string."""
    s = str(value)
    s = s.replace("\\", "\\\\")
    s = s.replace('"', '\\"')
    return s


def bool_str(flag: bool) -> str:
    return "True" if flag else "False"


def main() -> int:
    args = parse_args()

    # --- 1. Parse source manifests ---
    try:
        source_infos = json.loads(args.source_manifests)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid --source-manifests JSON: {e}", file=sys.stderr)
        return 1

    if len(source_infos) == 0:
        print("[ERROR] No source manifests provided", file=sys.stderr)
        return 1

    # --- 2. Read plan manifests ---
    source_blocks: list = []  # list of (pool, seed, [block_dict, ...])
    for info in source_infos:
        manifest_path = info["path"]
        if not os.path.exists(manifest_path):
            print(f"[ERROR] Manifest not found: {manifest_path}", file=sys.stderr)
            return 1
        with open(manifest_path, "r") as f:
            m = json.load(f)
        source_blocks.append((info["pool"], info["seed"], m.get("blocks", [])))
        print(f"[INFO] Source {info['pool']} (seed={info['seed']}): "
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

    # Build pool→(seed, blocks) lookup for block generation
    pool_lookup: dict = {}
    for pool, seed, blocks in source_blocks:
        if pool in pool_lookup:
            print(f"[ERROR] Duplicate source manifest for pool: {pool}", file=sys.stderr)
            return 1
        pool_lookup[pool] = (seed, blocks)

    missing_pools = sorted(set(campaign_inputs) - set(pool_lookup))
    if missing_pools:
        print(
            f"[ERROR] --campaign-inputs references missing pools: {','.join(missing_pools)}",
            file=sys.stderr,
        )
        return 1

    input_multiplicity = Counter(campaign_inputs)
    n_blocks_by_pool = {pool: len(blocks) for pool, (_, blocks) in pool_lookup.items()}
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
    for pool, seed, blocks in source_blocks:
        used_count = n_mixed * input_multiplicity.get(pool, 0)
        leftover = list(range(used_count, len(blocks)))
        if leftover:
            unused.append({"pool": pool, "seed": seed, "unused_indices": leftover,
                           "unused_count": len(leftover)})
            print(f"[INFO] {pool}: {len(leftover)} unused blocks (indices {leftover[0]}-{leftover[-1]})")

    def mixed_block_inputs(block_index: int) -> list:
        occurrence_seen = defaultdict(int)
        inputs = []
        for pool_name in campaign_inputs:
            occurrence = occurrence_seen[pool_name]
            occurrence_seen[pool_name] += 1
            seed, _ = pool_lookup[pool_name]
            source_block_index = block_index * input_multiplicity[pool_name] + occurrence
            inputs.append({
                "pool": pool_name,
                "seed": seed,
                "block_index": source_block_index,
                "occurrence": occurrence,
            })
        return inputs

    # --- 4. Generate SubDAG ---
    os.makedirs(os.path.dirname(args.subdag_output_path), exist_ok=True)
    dag_tmp = args.subdag_output_path + ".tmp"

    with open(dag_tmp, "w") as dag:
        dag.write("# ================================================\n")
        dag.write(f"# Block processing SubDAG\n")
        dag.write(f"# Campaign: {args.campaign}  Job index: {args.job_index}\n")
        dag.write(f"# Generated: {datetime.now(timezone.utc).isoformat()}\n")
        dag.write(f"# Tool: coordinate_lhe_blocks.py\n")
        dag.write(f"# Strict-min: {n_mixed} mixed blocks from "
                  f"{' + '.join(f'{pool}({n_blocks_by_pool[pool]}/{input_multiplicity.get(pool, 0)} uses)' for pool, _, _ in source_blocks)}\n")
        dag.write("# ================================================\n")
        dag.write("\n")
        dag.write(f"MAXJOBS block_processing {args.max_block_subdag_jobs}\n")
        dag.write("\n")

        for i in range(n_mixed):
            # Build inputs string from campaign inputs (with duplicates)
            input_parts = [
                f"BLOCK:{item['pool']}:{item['seed']}:{item['block_index']:06d}"
                for item in mixed_block_inputs(i)
            ]
            inputs_str = ",".join(input_parts)
            modes_str = args.shower_modes

            node_name = f"MIX_{args.campaign}_{args.job_index}_BLOCK{i:06d}"

            dag.write(f"JOB {node_name} {args.processing_sub_template_path}\n")
            dag.write(f"CATEGORY {node_name} block_processing\n")

            # VARS — following the pattern of add_processing_job() in dag_generator.py
            vars_line = (
                f'VARS {node_name} '
                f'campaign="{dag_escape(args.campaign)}" '
                f'job_id="{dag_escape(f"BLOCK{i:06d}")}" '
                f'inputs="{dag_escape(inputs_str)}" '
                f'modes="{dag_escape(modes_str)}" '
                f'analysis="{dag_escape(args.analysis_type)}" '
                f'n_sources="{dag_escape(args.n_sources)}" '
                f'max_events="{dag_escape(args.max_events)}" '
                f'enable_ntuple="{dag_escape(bool_str(args.enable_ntuple))}" '
                f'efficiency_ntuple="{dag_escape(bool_str(args.efficiency_ntuple))}" '
                f'cleanup="{dag_escape(bool_str(args.cleanup))}" '
                f'shuffle_mixing="{dag_escape(bool_str(args.shuffle_mixing))}" '
                f'request_cpus="{dag_escape(args.request_cpus)}" '
                f'request_memory="{dag_escape(args.request_memory)}" '
                f'request_disk="{dag_escape(args.request_disk)}" '
                f'processing_bundle_path="{dag_escape(args.processing_bundle_path)}" '
                f'processing_bundle_name="{dag_escape(args.processing_bundle_name)}" '
                f'proxy_bundle_path="{dag_escape(args.proxy_bundle_path)}" '
                f'proxy_bundle_name="{dag_escape(args.proxy_bundle_name)}" '
                f'processing_wrapper_path="{dag_escape(args.processing_wrapper_path)}" '
                f'log_root="{dag_escape(args.log_root)}" '
                f'local_output_base="{dag_escape(args.local_output_base)}" '
                f'target_machine="{dag_escape(args.target_machine)}"'
            )
            dag.write(vars_line + "\n")
            dag.write(f"RETRY {node_name} 1\n")

            # Ntuple node (if enabled)
            if args.enable_ntuple and args.ntuple_sub_template_path:
                ntuple_name = f"NTUPLE_{args.campaign}_{args.job_index}_BLOCK{i:06d}"
                dag.write(f"JOB {ntuple_name} {args.ntuple_sub_template_path}\n")
                dag.write(f"CATEGORY {ntuple_name} ntuple\n")
                ntuple_vars = (
                    f'VARS {ntuple_name} '
                    f'campaign="{dag_escape(args.campaign)}" '
                    f'job_id="{dag_escape(f"BLOCK{i:06d}")}" '
                    f'analysis="{dag_escape(args.analysis_type)}" '
                    f'max_events="{dag_escape(args.max_events)}" '
                    f'cleanup="{dag_escape(bool_str(args.cleanup))}" '
                    f'efficiency_ntuple="{dag_escape(bool_str(args.efficiency_ntuple))}" '
                    f'miniaod_input="" '
                    f'local_output_base="{dag_escape(args.local_output_base)}" '
                    f'request_cpus="2" request_memory="12GB" request_disk="8GB" '
                    f'ntuple_bundle_path="{dag_escape(args.ntuple_bundle_path)}" '
                    f'ntuple_bundle_name="{dag_escape(args.ntuple_bundle_name)}" '
                    f'proxy_bundle_path="{dag_escape(args.proxy_bundle_path)}" '
                    f'proxy_bundle_name="{dag_escape(args.proxy_bundle_name)}" '
                    f'ntuple_wrapper_path="{dag_escape(args.ntuple_wrapper_path)}" '
                    f'ntuple_wrapper_name="{dag_escape(os.path.basename(args.ntuple_wrapper_path or "run_ntuple_only.sh"))}" '
                    f'log_root="{dag_escape(args.log_root)}"'
                )
                dag.write(ntuple_vars + "\n")
                dag.write(f"RETRY {ntuple_name} 1\n")
                dag.write(f"PARENT {node_name} CHILD {ntuple_name}\n")

            dag.write("\n")

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
        "sources": [
            {"pool": pool, "seed": seed, "n_blocks": len(blocks),
             "n_used": n_mixed * input_multiplicity.get(pool, 0),
             "multiplicity": input_multiplicity.get(pool, 0)}
            for pool, seed, blocks in source_blocks
        ],
        "mixed_blocks": [
            {
                "index": i,
                "inputs": mixed_block_inputs(i),
            }
            for i in range(n_mixed)
        ],
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
