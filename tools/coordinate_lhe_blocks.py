#!/usr/bin/env python3
"""
coordinate_lhe_blocks.py — Campaign-level multi-source LHE block coordinator.

Runs as a Condor job after all per-pool LHE planners complete for a campaign
job index. Reads plan manifests from each source, consumes distinct blocks to
satisfy per-source LHE-event budgets, and generates a SubDAG where each
MIX_BLOCK node processes one budgeted input group from every required source.

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

DEFAULT_TARGET_MIXED_EVENTS = 100
DEFAULT_NORMAL_MAX_LHE_EVENTS = 110
DEFAULT_PHI_MAX_LHE_EVENTS = 350
DEFAULT_PHI_MAX_HADRONIZATION_RETRIES = 5000
DEFAULT_MINIMUM_OUTPUT_FRACTION = 0.8
DEFAULT_UNUSED_HEPMC_WARNING_FRACTION = 0.15
EDM_EVENT_ID_SCHEME = "run1-cantor-job-block-lumi-v1"
UINT32_MAX = 2**32 - 1
UINT64_MAX = 2**64 - 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Multi-source LHE block coordinator")
    p.add_argument("--campaign", required=True)
    p.add_argument("--job-index", type=int, required=True)
    source_manifests = p.add_mutually_exclusive_group(required=True)
    source_manifests.add_argument(
        "--source-manifests",
        help="JSON string: [{'pool':'A','seed':100,'path':'...'}, ...]",
    )
    source_manifests.add_argument(
        "--source-manifests-file",
        help="JSON file containing the source manifest list",
    )
    p.add_argument("--shower-modes", required=True, help="Comma-separated shower modes")
    p.add_argument("--campaign-inputs", required=True,
                   help="Comma-separated campaign input pool names (with duplicates)")
    p.add_argument("--analysis-type", required=True, help="JJP or JUP")
    p.add_argument("--n-sources", type=int, required=True)
    p.add_argument("--max-events", type=int, default=-1)
    p.add_argument("--target-mixed-events", type=int, default=0,
                   help="Target accepted mixed HepMC events per processing block")
    p.add_argument("--normal-max-lhe-events", type=int, default=DEFAULT_NORMAL_MAX_LHE_EVENTS)
    p.add_argument("--phi-max-lhe-events", type=int, default=DEFAULT_PHI_MAX_LHE_EVENTS)
    p.add_argument("--phi-max-hadronization-retries", type=int,
                   default=DEFAULT_PHI_MAX_HADRONIZATION_RETRIES)
    p.add_argument("--minimum-output-fraction", type=float,
                   default=DEFAULT_MINIMUM_OUTPUT_FRACTION)
    p.add_argument("--phi-consumption-mode", choices=("target", "exhaustive"),
                   default="target")
    p.add_argument("--normal-shortfall-policy",
                   choices=("fail", "report-and-truncate"), default="fail")
    p.add_argument(
        "--unused-hepmc-warning-fraction",
        type=float,
        default=DEFAULT_UNUSED_HEPMC_WARNING_FRACTION,
        help="Report a warning when a source exceeds this unused accepted-HepMC fraction",
    )
    p.add_argument("--source-lhe-budgets", default="[]",
                   help="JSON list with one LHE-event budget per campaign source slot")
    p.add_argument("--pool-start-blocks", default="{}",
                   help="JSON pool-to-block cursor map for exclusive campaign allocation")
    p.add_argument("--processing-start-index", type=int, default=0,
                   help="First node in the deterministic global pool stream")
    p.add_argument("--max-processing-nodes", type=int, default=0,
                   help="Maximum nodes emitted by this shard; 0 emits the remainder")
    p.add_argument(
        "--allocation-manifest",
        default="",
        help="Authoritative campaign/shard allocation manifest",
    )
    p.add_argument(
        "--allocation-shard-index",
        type=int,
        default=-1,
        help="Shard index to load from --allocation-manifest",
    )
    p.add_argument("--physics-campaign", default="")
    p.add_argument("--source-rng-seeds", default="[]")
    p.add_argument("--mixing-rng-seed", type=int, default=0)
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


def processing_manifest_url(base: str, campaign: str, job_id: str) -> str:
    return (
        f"{base}/output/{campaign}/{job_id}/"
        f"processing_manifest_{campaign}_{job_id}.json"
    )


def ntuple_url(base: str, campaign: str, job_id: str) -> str:
    return f"{base}/output/{campaign}/{job_id}/output_ntuple.root"


def is_phi_mode(mode: str) -> bool:
    return mode.strip() in {
        "phi",
        "phi_default",
        "phi_mode1",
        "phi_mpi_off",
        "sps",
        "phi_mode2",
        "phi_mpi_on_gluon",
        "phi_gluon",
    }


def edm_luminosity_block(job_index: int, block_index: int) -> int:
    """Map one campaign job/block pair injectively into a uint32 lumi."""
    if job_index < 0 or block_index < 0:
        raise ValueError(
            f"EventID indices must be non-negative: "
            f"job_index={job_index} block_index={block_index}"
        )
    diagonal = job_index + block_index
    paired = diagonal * (diagonal + 1) // 2 + block_index
    luminosity_block = paired + 1
    if luminosity_block > UINT32_MAX:
        raise OverflowError(
            "Cantor-paired luminosity block exceeds uint32: "
            f"job_index={job_index} block_index={block_index} "
            f"luminosity_block={luminosity_block} limit={UINT32_MAX}"
        )
    return luminosity_block


def compute_edm_event_ids(
    job_index: int,
    block_indices,
    block_event_count_fn,
    processing_max_events: int,
) -> dict:
    """Return deterministic EventID metadata unique within one campaign."""
    event_ids = {}
    for block_index in block_indices:
        raw_events = int(block_event_count_fn(block_index) or 0)
        reserved_events = raw_events
        if processing_max_events > 0:
            reserved_events = min(reserved_events, processing_max_events)
        if reserved_events <= 0:
            raise ValueError(
                f"Block {block_index}: reserved_events={reserved_events} "
                "cannot assign an empty EventID span"
            )
        if reserved_events > UINT64_MAX:
            raise OverflowError(
                f"Block {block_index}: reserved_events={reserved_events} "
                f"exceeds uint64 limit {UINT64_MAX}"
            )
        event_ids[block_index] = {
            "first_run": 1,
            "first_luminosity_block": edm_luminosity_block(
                job_index, block_index
            ),
            "first_event": 1,
            "reserved_events": reserved_events,
            "number_events_in_luminosity_block": 0,
        }
    return event_ids


def validate_edm_event_ids(block_records: list) -> None:
    """Validate full run/lumi/event ranges are bounded and non-overlapping."""
    seen_ranges = defaultdict(list)
    for record in block_records:
        eid = record["edm_event_id"]
        run = int(eid["first_run"])
        lumi = int(eid["first_luminosity_block"])
        first = int(eid["first_event"])
        reserved = int(eid["reserved_events"])
        last = first + reserved - 1
        if run < 1 or run > UINT32_MAX or lumi < 1 or lumi > UINT32_MAX:
            raise ValueError(
                f"Block {record['block_index']}: run/luminosityBlock outside "
                f"uint32 range: run={run} luminosityBlock={lumi}"
            )
        if first < 1 or reserved <= 0:
            raise ValueError(
                f"Block {record['block_index']}: invalid EventID span "
                f"first_event={first} reserved_events={reserved}"
            )
        if last > UINT64_MAX:
            raise ValueError(
                f"Block {record['block_index']}: EventID span ends above uint64: "
                f"last_event={last}"
            )
        stream = (run, lumi)
        for prev_first, prev_last in seen_ranges[stream]:
            if first <= prev_last and last >= prev_first:
                raise ValueError(
                    f"EventID overlap: block {record['block_index']} "
                    f"run={run} lumi={lumi} [{first}, {last}] overlaps "
                    f"[{prev_first}, {prev_last}]"
                )
        seen_ranges[stream].append((first, last))


def main() -> int:
    args = parse_args()

    # --- 1. Parse source manifests ---
    try:
        if args.source_manifests_file:
            with open(args.source_manifests_file, "r", encoding="utf-8") as handle:
                source_infos = json.load(handle)
        else:
            source_infos = json.loads(args.source_manifests)
    except (OSError, json.JSONDecodeError) as e:
        print(f"[ERROR] Invalid source manifests: {e}", file=sys.stderr)
        return 1
    try:
        storage_config = json.loads(args.storage_config)
        processing_environment_config = json.loads(args.processing_environment_config)
        source_rng_seeds = json.loads(args.source_rng_seeds)
        source_lhe_budgets = json.loads(args.source_lhe_budgets)
        pool_start_blocks = json.loads(args.pool_start_blocks)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid node config JSON: {e}", file=sys.stderr)
        return 1
    if not isinstance(storage_config, dict):
        print("[ERROR] --storage-config must be a JSON object", file=sys.stderr)
        return 1
    if not isinstance(processing_environment_config, dict):
        print("[ERROR] --processing-environment-config must be a JSON object", file=sys.stderr)
        return 1
    if not isinstance(source_rng_seeds, list) or not isinstance(source_lhe_budgets, list):
        print("[ERROR] source RNG seeds and LHE budgets must be JSON lists", file=sys.stderr)
        return 1
    if not isinstance(pool_start_blocks, dict):
        print("[ERROR] --pool-start-blocks must be a JSON object", file=sys.stderr)
        return 1
    try:
        pool_start_blocks = {
            str(pool): int(cursor)
            for pool, cursor in pool_start_blocks.items()
        }
    except (TypeError, ValueError):
        print("[ERROR] --pool-start-blocks values must be integers", file=sys.stderr)
        return 1
    if any(cursor < 0 for cursor in pool_start_blocks.values()):
        print("[ERROR] --pool-start-blocks values must be non-negative", file=sys.stderr)
        return 1
    allocation_record = None
    if args.allocation_manifest:
        if args.allocation_shard_index < 0:
            print(
                "[ERROR] --allocation-shard-index is required with "
                "--allocation-manifest",
                file=sys.stderr,
            )
            return 1
        try:
            with open(args.allocation_manifest, "r", encoding="utf-8") as handle:
                allocation = json.load(handle)
            campaign_allocation = allocation["campaigns"][args.campaign]
            shards = campaign_allocation["shards"]
            allocation_record = next(
                shard
                for shard in shards
                if int(shard["shard_index"]) == args.allocation_shard_index
            )
            pool_start_blocks = {
                str(pool): int(cursor)
                for pool, cursor
                in allocation_record["pool_start_blocks"].items()
            }
            args.processing_start_index = 0
            args.max_processing_nodes = int(allocation_record["node_count"])
        except (OSError, json.JSONDecodeError, KeyError, StopIteration, TypeError, ValueError) as e:
            print(f"[ERROR] Invalid shard allocation: {e}", file=sys.stderr)
            return 1
        if args.max_processing_nodes <= 0:
            print("[ERROR] Allocated shard contains no processing nodes", file=sys.stderr)
            return 1
        print(
            f"[INFO] Loaded authoritative allocation for {args.campaign} "
            f"shard {args.allocation_shard_index}: "
            f"{args.max_processing_nodes} nodes, starts={pool_start_blocks}"
        )
    if not 0.0 <= args.unused_hepmc_warning_fraction < 1.0:
        print(
            "[ERROR] unused HepMC warning fraction must satisfy 0 <= warning < 1",
            file=sys.stderr,
        )
        return 1
    if args.processing_start_index < 0 or args.max_processing_nodes < 0:
        print("[ERROR] processing shard indices must be non-negative", file=sys.stderr)
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

    # --- 3. Resolve campaign inputs and budget-derived source groups ---
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
    if source_rng_seeds and len(source_rng_seeds) != args.n_sources:
        print("[ERROR] --source-rng-seeds count must match --n-sources", file=sys.stderr)
        return 1
    if args.phi_consumption_mode == "exhaustive" and not any(
        is_phi_mode(mode) for mode in shower_modes
    ):
        print("[ERROR] exhaustive mode requires at least one phi source", file=sys.stderr)
        return 1
    if args.target_mixed_events > 0:
        target_mixed_events = args.target_mixed_events
    elif args.max_events > 0:
        target_mixed_events = args.max_events
    else:
        target_mixed_events = DEFAULT_TARGET_MIXED_EVENTS
    if target_mixed_events <= 0:
        print("[ERROR] target_mixed_events must be positive", file=sys.stderr)
        return 1

    if source_lhe_budgets and len(source_lhe_budgets) != args.n_sources:
        print("[ERROR] --source-lhe-budgets count must match --n-sources", file=sys.stderr)
        return 1
    if source_lhe_budgets and any(int(value) <= 0 for value in source_lhe_budgets):
        print("[ERROR] --source-lhe-budgets entries must be positive", file=sys.stderr)
        return 1

    # Build a pool-level global block stream. Each item retains the original
    # manifest group and local block index used by BLOCK: resolution.
    pool_lookup: dict = {}
    for pool, group_id, primary_seed, seeds, blocks in source_blocks:
        source = pool_lookup.setdefault(pool, {"blocks": [], "manifests": []})
        source["manifests"].append({
            "group_id": group_id,
            "primary_seed": primary_seed,
            "seeds": seeds,
            "n_blocks": len(blocks),
        })
        for local_index, block in enumerate(blocks):
            source["blocks"].append({
                **block,
                "group_id": group_id,
                "primary_seed": primary_seed,
                "seeds": seeds,
                "block_index": local_index,
            })

    missing_pools = sorted(set(campaign_inputs) - set(pool_lookup))
    if missing_pools:
        print(
            f"[ERROR] --campaign-inputs references missing pools: {','.join(missing_pools)}",
            file=sys.stderr,
        )
        return 1
    unknown_start_pools = sorted(set(pool_start_blocks) - set(pool_lookup))
    if unknown_start_pools:
        print(
            "[ERROR] --pool-start-blocks references missing pools: "
            + ",".join(unknown_start_pools),
            file=sys.stderr,
        )
        return 1

    n_blocks_by_pool = {pool: len(source["blocks"]) for pool, source in pool_lookup.items()}
    for pool_name, cursor in pool_start_blocks.items():
        if cursor > n_blocks_by_pool[pool_name]:
            print(
                f"[ERROR] --pool-start-blocks cursor {cursor} exceeds "
                f"{pool_name} capacity {n_blocks_by_pool[pool_name]}",
                file=sys.stderr,
            )
            return 1
    input_multiplicity = Counter(campaign_inputs)

    source_templates = []
    occurrence_seen = defaultdict(int)
    for slot, (pool_name, mode) in enumerate(zip(campaign_inputs, shower_modes)):
        occurrence = occurrence_seen[pool_name]
        occurrence_seen[pool_name] += 1
        phi_like = is_phi_mode(mode)
        source_templates.append({
            "slot": slot,
            "pool": pool_name,
            "mode": mode,
            "occurrence": occurrence,
            "target_hepmc_events": (
                0 if phi_like and args.phi_consumption_mode == "exhaustive"
                else target_mixed_events
            ),
            "max_lhe_events": int(source_lhe_budgets[slot]) if source_lhe_budgets else (
                args.phi_max_lhe_events if phi_like else args.normal_max_lhe_events
            ),
            "max_hadronization_retries": (
                args.phi_max_hadronization_retries if phi_like else 1000
            ),
            "rng_seed": int(source_rng_seeds[slot]) if source_rng_seeds else 0,
        })

    pool_cursors = defaultdict(int, pool_start_blocks)
    mixed_source_slots = []
    build_limit = None
    if args.max_processing_nodes:
        build_limit = args.processing_start_index + args.max_processing_nodes
    while build_limit is None or len(mixed_source_slots) < build_limit:
        block_sources = []
        next_cursors = dict(pool_cursors)
        can_build = True
        for tmpl in source_templates:
            pool_name = tmpl["pool"]
            source = pool_lookup[pool_name]
            blocks = source["blocks"]
            cursor = next_cursors.get(pool_name, 0)
            chosen = []
            accumulated = 0
            while cursor < len(blocks) and (not chosen or accumulated < tmpl["max_lhe_events"]):
                block = blocks[cursor]
                n_events = int(block.get("n_events", 0) or 0)
                chosen.append({
                    "pool": pool_name,
                    "group_id": block["group_id"],
                    "primary_seed": block["primary_seed"],
                    "seeds": block["seeds"],
                    "block_index": block["block_index"],
                    "global_block_index": cursor,
                    "n_events": n_events,
                    "path": block.get("path", ""),
                })
                accumulated += max(0, n_events)
                cursor += 1
            if not chosen:
                can_build = False
                break
            block_source = dict(tmpl)
            block_source["inputs"] = [
                f"BLOCK:{item['pool']}:{item['group_id']}:{item['block_index']:06d}"
                for item in chosen
            ]
            block_source["blocks"] = chosen
            block_source["planned_lhe_events"] = accumulated
            if (
                args.phi_consumption_mode == "exhaustive"
                and is_phi_mode(tmpl["mode"])
                and accumulated < tmpl["max_lhe_events"]
            ):
                print(
                    f"[ERROR] Exhaustive phi source {pool_name} has only "
                    f"{accumulated} planned events; requires {tmpl['max_lhe_events']}",
                    file=sys.stderr,
                )
                return 1
            block_sources.append(block_source)
            next_cursors[pool_name] = cursor
        if not can_build:
            break
        mixed_source_slots.append(block_sources)
        pool_cursors = defaultdict(int, next_cursors)

    total_mixed = len(mixed_source_slots)
    if total_mixed == 0:
        msg = "no common blocks across campaign source slots ("
        msg += ", ".join(
            f"{pool}={n_blocks_by_pool[pool]} blocks/{input_multiplicity.get(pool, 0)} uses"
            for pool in input_multiplicity
        )
        msg += ")"
        print(f"[ERROR] {msg}", file=sys.stderr)
        return 1
    shard_stop = total_mixed
    if args.max_processing_nodes:
        shard_stop = min(
            total_mixed,
            args.processing_start_index + args.max_processing_nodes,
        )
    mixed_source_slots = mixed_source_slots[args.processing_start_index:shard_stop]
    n_mixed = len(mixed_source_slots)
    if n_mixed == 0:
        print(
            f"[ERROR] processing shard [{args.processing_start_index}, {shard_stop}) "
            f"is empty for global stream of {total_mixed} nodes",
            file=sys.stderr,
        )
        return 1
    print(
        f"[INFO] Budget-derived global stream: {total_mixed} nodes; "
        f"emitting [{args.processing_start_index}, {shard_stop})"
    )

    unused = []
    for pool, source in pool_lookup.items():
        blocks = source["blocks"]
        used_count = pool_cursors.get(pool, 0)
        leftover = list(range(used_count, len(blocks)))
        if leftover:
            unused.append({"pool": pool,
                           "unused_global_indices": leftover,
                           "unused_count": len(leftover)})
            print(f"[INFO] {pool}: {len(leftover)} unused blocks (indices {leftover[0]}-{leftover[-1]})")

    def mixed_block_sources(block_index: int) -> list:
        return mixed_source_slots[block_index]

    def mixed_block_inputs(block_index: int) -> list:
        inputs = []
        for source in mixed_block_sources(block_index):
            inputs.extend(source["blocks"])
        return inputs

    def mixed_block_event_count(block_index: int) -> int:
        return target_mixed_events

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
    event_id_span = (
        max(int(source["max_lhe_events"]) for source in source_templates)
        if args.phi_consumption_mode == "exhaustive"
        else target_mixed_events
    )
    phi_exposure_events = next(
        (
            int(source["max_lhe_events"])
            for source in source_templates
            if is_phi_mode(source["mode"])
        ),
        0,
    )
    packing_weight_events = event_id_span
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

    try:
        edm_ids = compute_edm_event_ids(
            args.job_index,
            range(n_mixed),
            lambda _index: event_id_span,
            -1,
        )
    except (OverflowError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    block_records = []
    for i in range(n_mixed):
        jid = block_job_id(args.job_index, i)
        block_records.append({
            "block_index": i,
            "job_id": jid,
            "expected_events": None if args.phi_consumption_mode == "exhaustive" else target_mixed_events,
            "packing_weight_events": packing_weight_events,
            "target_mixed_events": None if args.phi_consumption_mode == "exhaustive" else target_mixed_events,
            "event_id_span": event_id_span,
            "edm_event_id": edm_ids[i],
            "inputs": mixed_block_inputs(i),
            "sources": mixed_block_sources(i),
            "miniaod_url": miniaod_url(target_base, args.campaign, jid),
            "processing_manifest_url": processing_manifest_url(target_base, args.campaign, jid),
        })
    try:
        validate_edm_event_ids(block_records)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    merge_groups = []
    if merge_enabled:
        current = []
        current_events = 0
        for record in block_records:
            weight = int(record.get("packing_weight_events", 0) or 0)
            if (
                current
                and abs(current_events - args.miniaod_merge_events)
                <= abs(current_events + weight - args.miniaod_merge_events)
            ):
                merge_groups.append(current)
                current = []
                current_events = 0
            current.append(record)
            current_events += weight
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
                "expected_events": (
                    None if args.phi_consumption_mode == "exhaustive"
                    else sum(int(item.get("expected_events", 0) or 0) for item in components)
                ),
                "packing_weight_events": sum(
                    int(item.get("packing_weight_events", 0) or 0) for item in components
                ),
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
        dag.write(f"# Budget-derived: {n_mixed} mixed blocks from "
                  f"{' + '.join(f'{pool}({n_blocks_by_pool[pool]}/{input_multiplicity.get(pool, 0)} uses)' for pool, _, _, _, _ in source_blocks)}\n")
        dag.write("# ================================================\n")
        dag.write("\n")
        dag.write(f"MAXJOBS block_processing {args.max_block_subdag_jobs}\n")
        if merge_enabled:
            dag.write(f"MAXJOBS miniaod_merge {args.max_miniaod_merge_jobs}\n")
        dag.write("\n")

        for i in range(n_mixed):
            # Legacy inputs/modes stay for old wrappers; sources[] is authoritative.
            input_parts = [
                source["inputs"][0]
                for source in mixed_block_sources(i)
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
                "physics_campaign": args.physics_campaign or args.campaign,
                "job_id": block_job_id_value,
                "max_events": args.max_events,
                "phi_consumption_mode": args.phi_consumption_mode,
                "normal_shortfall_policy": args.normal_shortfall_policy,
                "unused_hepmc_warning_fraction": args.unused_hepmc_warning_fraction,
                "phi_exposure_events": phi_exposure_events if args.phi_consumption_mode == "exhaustive" else None,
                "minimum_accepted_events": 1 if args.phi_consumption_mode == "exhaustive" else None,
                "target_mixed_events": None if args.phi_consumption_mode == "exhaustive" else target_mixed_events,
                "event_id_span": event_id_span,
                "minimum_output_fraction": None if args.phi_consumption_mode == "exhaustive" else args.minimum_output_fraction,
                "mixing_rng_seed": args.mixing_rng_seed,
                "require_processing_manifests": args.phi_consumption_mode == "exhaustive",
                "sources": mixed_block_sources(i),
                "edm_event_id": edm_ids[i],
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
                            "manifest_url": component["processing_manifest_url"],
                            "expected_events": component["expected_events"],
                            "packing_weight_events": component["packing_weight_events"],
                            "inputs": component["inputs"],
                            "sources": component["sources"],
                        }
                        for component in record["components"]
                    ],
                    "expected_events": record["expected_events"],
                    "packing_weight_events": record["packing_weight_events"],
                    "require_processing_manifests": args.phi_consumption_mode == "exhaustive",
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
                    f'request_cpus="1" request_memory="3GB" request_disk="20GB" '
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
            "event_id_scheme": EDM_EVENT_ID_SCHEME,
            "output_url": final_output_url,
            "blocks": block_records,
            "merge_groups": merge_records,
            "ntuples": ntuple_records,
            "cleanup_components": bool(
                args.cleanup and merge_records and ntuple_records
            ),
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
        "version": "1.1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "campaign": args.campaign,
        "job_index": args.job_index,
        "event_id_scheme": EDM_EVENT_ID_SCHEME,
        "n_mixed_blocks": n_mixed,
        "processing_max_events": args.max_events,
        "physics_campaign": args.physics_campaign or args.campaign,
        "phi_consumption_mode": args.phi_consumption_mode,
        "normal_shortfall_policy": args.normal_shortfall_policy,
        "unused_hepmc_warning_fraction": args.unused_hepmc_warning_fraction,
        "source_lhe_budgets": [int(value) for value in source_lhe_budgets],
        "pool_start_blocks": pool_start_blocks,
        "allocation": {
            "manifest_path": args.allocation_manifest,
            "shard_index": (
                args.allocation_shard_index
                if args.allocation_manifest else None
            ),
            "record": allocation_record,
        },
        "global_processing_nodes": total_mixed,
        "processing_start_index": args.processing_start_index,
        "processing_stop_index": shard_stop,
        "phi_exposure_events": phi_exposure_events if args.phi_consumption_mode == "exhaustive" else None,
        "target_mixed_events": None if args.phi_consumption_mode == "exhaustive" else target_mixed_events,
        "event_id_span": event_id_span,
        "minimum_output_fraction": None if args.phi_consumption_mode == "exhaustive" else args.minimum_output_fraction,
        "miniaod_merge_enabled": merge_enabled,
        "miniaod_merge_events": args.miniaod_merge_events,
        "miniaod_merge_validation": args.miniaod_merge_validation,
        "sources": [
            {"pool": pool, "group_id": group_id, "primary_seed": primary_seed,
             "seeds": seeds, "n_blocks": len(blocks),
             "n_used": pool_cursors.get(pool, 0),
             "multiplicity": input_multiplicity.get(pool, 0)}
            for pool, group_id, primary_seed, seeds, blocks in source_blocks
        ],
        "mixed_blocks": [
            {
                "index": i,
                "expected_events": None if args.phi_consumption_mode == "exhaustive" else mixed_block_event_count(i),
                "packing_weight_events": packing_weight_events,
                "target_mixed_events": None if args.phi_consumption_mode == "exhaustive" else target_mixed_events,
                "event_id_span": event_id_span,
                "edm_event_id": edm_ids[i],
                "miniaod_url": miniaod_url(target_base, args.campaign, block_job_id(args.job_index, i)),
                "processing_manifest_url": processing_manifest_url(target_base, args.campaign, block_job_id(args.job_index, i)),
                "inputs": mixed_block_inputs(i),
                "sources": mixed_block_sources(i),
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
