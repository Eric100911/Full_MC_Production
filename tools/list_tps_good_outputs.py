#!/usr/bin/env python3
"""List existing TPS outputs whose two J/psi sources use approved LHE seeds."""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path


DEFAULT_GENERATED_DIR = Path("generated/JpsiJpsiPhi_MC_Production_v4_2_full")
DEFAULT_JOB_SPEC = Path("common/campaign_job_specs/jjp_efficiency_balanced_full_v2.json")
DEFAULT_OUTPUT = Path("docs/tps_retained_ntuple_files.txt")
DEFAULT_ENDPOINT = "root://cceos.ihep.ac.cn:1094"
DEFAULT_OUTPUT_LFN = (
    "/store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/"
    "output/JJP_TPS_MC_v4_1"
)
CAMPAIGN = "JJP_TPS_MC_v4_1"
JPSI_POOL = "pool_jpsi_CSCO_g"


BLOCK_SIZE_HELPER = r"""
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <random>
#include <vector>

int main() {
    long long n_events;
    std::uint64_t seed;
    while (std::cin >> n_events >> seed) {
        const int events_per_block = 1000;
        int n_strata = static_cast<int>((n_events + events_per_block - 1) / events_per_block);
        n_strata = std::max(10, std::min(100, n_strata));
        n_strata = std::min<long long>(n_strata, n_events);

        std::vector<long long> remaining(n_strata, n_events / n_strata);
        for (int stratum = 0; stratum < n_events % n_strata; ++stratum) {
            ++remaining[stratum];
        }

        std::vector<int> sizes;
        long long selected = 0;
        for (int block_index = 0; selected < n_events; ++block_index) {
            std::vector<int> order;
            for (int stratum = 0; stratum < n_strata; ++stratum) {
                if (remaining[stratum] > 0) order.push_back(stratum);
            }
            if (order.empty()) break;

            std::mt19937_64 rng(seed + static_cast<std::uint64_t>(block_index) + 999999ULL);
            std::shuffle(order.begin(), order.end(), rng);
            const int target = static_cast<int>(std::min<long long>(1000, n_events - selected));
            const int per_stratum = (target + static_cast<int>(order.size()) - 1)
                / static_cast<int>(order.size());
            int size = 0;
            for (int stratum : order) {
                if (size >= target) break;
                const int take = static_cast<int>(std::min<long long>(
                    target - size,
                    std::min<long long>(per_stratum, remaining[stratum])));
                remaining[stratum] -= take;
                size += take;
            }
            if (size == 0) break;
            sizes.push_back(size);
            selected += size;
        }

        std::cout << sizes.size();
        for (int size : sizes) std::cout << ' ' << size;
        std::cout << '\n';
    }
}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generated-dir", type=Path, default=DEFAULT_GENERATED_DIR)
    parser.add_argument("--job-spec", type=Path, default=DEFAULT_JOB_SPEC)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--product",
        choices=("ntuple", "merged-miniaod", "both"),
        default="ntuple",
    )
    parser.add_argument("--good-seed-min", type=int, default=100)
    parser.add_argument("--good-seed-max", type=int, default=1099)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--output-lfn", default=DEFAULT_OUTPUT_LFN)
    parser.add_argument(
        "--listing-file",
        type=Path,
        help="Use a saved recursive xrdfs listing instead of querying IHEP.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def verify_inventory(job_spec: dict, repository_root: Path) -> str:
    inventory_path = repository_root / job_spec["source_inventory"]
    digest = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
    expected = job_spec["source_inventory_sha256"]
    if digest != expected:
        raise RuntimeError(
            f"inventory checksum mismatch: expected {expected}, found {digest}"
        )
    return digest


def load_ordered_plans(generated_dir: Path, selected_seeds: list[int]) -> list[dict]:
    paths = glob.glob(str(generated_dir / "node_configs/planning/PLAN_JpsiG_CSCO_*.json"))
    plans_by_seed = {}
    for raw_path in paths:
        plan = load_json(Path(raw_path))
        seed = int(plan["primary_seed"])
        if seed in plans_by_seed:
            raise RuntimeError(f"duplicate planning config for seed {seed}")
        plans_by_seed[seed] = plan

    missing = [seed for seed in selected_seeds if seed not in plans_by_seed]
    if missing:
        raise RuntimeError(f"missing planning configs for {len(missing)} seeds: {missing[:10]}")
    extra = sorted(set(plans_by_seed) - set(selected_seeds))
    if extra:
        raise RuntimeError(f"unexpected planning configs for seeds: {extra[:10]}")

    plans = [plans_by_seed[seed] for seed in selected_seeds]
    for plan in plans:
        input_events = sum(int(value) for value in plan["lhe_event_counts"])
        expected = {
            "events_per_block": 1000,
            "shuffle_mode": "stratified",
            "n_strata": "auto",
            "drop_incomplete_last_block": False,
            "max_events_per_plan": input_events,
        }
        for key, value in expected.items():
            if plan.get(key) != value:
                raise RuntimeError(
                    f"unsupported {key} in seed {plan['primary_seed']}: "
                    f"expected {value!r}, found {plan.get(key)!r}"
                )
    return plans


def reconstruct_block_stream(plans: list[dict]) -> list[dict]:
    helper_input = "".join(
        f"{sum(int(value) for value in plan['lhe_event_counts'])} "
        f"{int(plan['shuffle_seed'])}\n"
        for plan in plans
    )
    with tempfile.TemporaryDirectory(prefix="tps_block_sizes_") as tmp_dir:
        helper = Path(tmp_dir) / "block_sizes"
        compile_result = subprocess.run(
            ["g++", "-x", "c++", "-std=c++14", "-O2", "-o", str(helper), "-"],
            input=BLOCK_SIZE_HELPER,
            text=True,
            capture_output=True,
        )
        if compile_result.returncode != 0:
            raise RuntimeError(f"failed to compile block-size helper:\n{compile_result.stderr}")
        result = subprocess.run(
            [str(helper)], input=helper_input, text=True, capture_output=True, check=True
        )

    lines = result.stdout.splitlines()
    if len(lines) != len(plans):
        raise RuntimeError(
            f"block-size helper returned {len(lines)} rows for {len(plans)} plans"
        )

    stream = []
    for plan, line in zip(plans, lines):
        values = [int(value) for value in line.split()]
        declared_blocks, sizes = values[0], values[1:]
        if declared_blocks != len(sizes):
            raise RuntimeError(f"invalid helper result for seed {plan['primary_seed']}")
        input_events = sum(int(value) for value in plan["lhe_event_counts"])
        if sum(sizes) != input_events:
            raise RuntimeError(f"event conservation failed for seed {plan['primary_seed']}")
        for block_index, n_events in enumerate(sizes):
            stream.append(
                {
                    "group_id": plan["group_id"],
                    "seed": int(plan["primary_seed"]),
                    "block_index": block_index,
                    "n_events": n_events,
                }
            )
    return stream


def verify_against_dps_manifests(generated_dir: Path, stream: list[dict]) -> int:
    pattern = generated_dir / "plan_subdags/JJP_DPS1_MC_v4_1/job_*/coord_manifest_*.json"
    paths = sorted(Path(path) for path in glob.glob(str(pattern)))
    if not paths:
        raise RuntimeError("preserved DPS coordinator manifests are missing")

    checked = 0
    for path in paths:
        manifest = load_json(path)
        for record in manifest["mixed_blocks"]:
            for block in record["inputs"]:
                actual = stream[int(block["global_block_index"])]
                expected = (
                    block["group_id"],
                    int(block["primary_seed"]),
                    int(block["block_index"]),
                    int(block["n_events"]),
                )
                reconstructed = (
                    actual["group_id"],
                    actual["seed"],
                    actual["block_index"],
                    actual["n_events"],
                )
                if reconstructed != expected:
                    raise RuntimeError(
                        f"DPS validation mismatch in {path}: "
                        f"expected {expected}, reconstructed {reconstructed}"
                    )
                checked += 1
    return checked


def reconstruct_safe_ids(
    stream: list[dict],
    allocation: dict,
    source_budgets: list[int],
    good_seed_min: int,
    good_seed_max: int,
) -> tuple[set[tuple[int, int]], set[tuple[int, int]]]:
    if source_budgets[:2] != [1000, 1000]:
        raise RuntimeError(f"unexpected TPS J/psi source budgets: {source_budgets[:2]}")

    campaign = allocation["campaigns"][CAMPAIGN]
    total_nodes = int(campaign["processing_nodes"])
    cursor = 0
    slot_block_counts = [0, 0]
    safe_nodes = []
    for _ in range(total_nodes):
        node_is_good = True
        for slot, budget in enumerate(source_budgets[:2]):
            accumulated = 0
            chosen = 0
            while cursor < len(stream) and (chosen == 0 or accumulated < budget):
                block = stream[cursor]
                node_is_good = node_is_good and (
                    good_seed_min <= block["seed"] <= good_seed_max
                )
                accumulated += block["n_events"]
                cursor += 1
                chosen += 1
            if chosen == 0:
                raise RuntimeError("J/psi block stream ended before the TPS plan")
            slot_block_counts[slot] += chosen
        safe_nodes.append(node_is_good)

    expected_cursor = int(campaign["pool_end_blocks"][JPSI_POOL])
    if cursor != expected_cursor:
        raise RuntimeError(f"TPS block cursor mismatch: expected {expected_cursor}, found {cursor}")
    if slot_block_counts != campaign["planned_blocks_by_slot"][:2]:
        raise RuntimeError(
            "TPS per-slot block counts mismatch: "
            f"expected {campaign['planned_blocks_by_slot'][:2]}, found {slot_block_counts}"
        )

    safe_processing = set()
    safe_merges = set()
    offset = 0
    for shard in campaign["shards"]:
        job_index = int(shard["shard_index"])
        count = int(shard["node_count"])
        local_nodes = safe_nodes[offset:offset + count]
        safe_processing.update(
            (job_index, block_index)
            for block_index, is_good in enumerate(local_nodes)
            if is_good
        )
        for merge_index, start in enumerate(range(0, count, 5)):
            if all(local_nodes[start:start + 5]):
                safe_merges.add((job_index, merge_index))
        offset += count
    if offset != total_nodes:
        raise RuntimeError(f"shard node total mismatch: expected {total_nodes}, found {offset}")
    return safe_processing, safe_merges


def get_recursive_listing(args: argparse.Namespace) -> list[str]:
    if args.listing_file:
        return [line.strip() for line in args.listing_file.read_text().splitlines() if line.strip()]
    result = subprocess.run(
        ["xrdfs", args.endpoint.rstrip("/") + "/", "ls", "-R", args.output_lfn],
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"xrdfs listing failed:\n{result.stderr}")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def output_key(path: str) -> tuple[int, int] | None:
    match = re.search(r"/JOB(\d+)_MERGE(\d+)(?:/|$)", path)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def full_url(endpoint: str, lfn: str) -> str:
    return endpoint.rstrip("/") + "//" + lfn


def select_output_files(
    listing: list[str],
    safe_merges: set[tuple[int, int]],
    endpoint: str,
    product: str,
) -> list[str]:
    suffixes = {
        "ntuple": ("/output_ntuple.root",),
        "merged-miniaod": ("/output_MINIAOD.root",),
        "both": ("/output_MINIAOD.root", "/output_ntuple.root"),
    }[product]
    selected = []
    for path in listing:
        key = output_key(path)
        if key in safe_merges and path.endswith(suffixes):
            selected.append((key, path))
    selected.sort(key=lambda item: (item[0][0], item[0][1], item[1]))
    return [full_url(endpoint, path) for _, path in selected]


def main() -> int:
    args = parse_args()
    repository_root = Path(__file__).resolve().parents[1]
    generated_dir = args.generated_dir
    if not generated_dir.is_absolute():
        generated_dir = repository_root / generated_dir
    job_spec_path = args.job_spec
    if not job_spec_path.is_absolute():
        job_spec_path = repository_root / job_spec_path
    output_path = args.output
    if not output_path.is_absolute():
        output_path = repository_root / output_path

    job_spec = load_json(job_spec_path)
    inventory_digest = verify_inventory(job_spec, repository_root)
    selected_seeds = [
        int(seed) for seed in job_spec["planning"]["pool_seeds"][JPSI_POOL]
    ]
    plans = load_ordered_plans(generated_dir, selected_seeds)
    stream = reconstruct_block_stream(plans)
    checked_dps_blocks = verify_against_dps_manifests(generated_dir, stream)
    allocation = load_json(generated_dir / "plan_subdags/campaign_shard_allocation.json")
    source_budgets = [
        int(value) for value in job_spec["campaigns"][CAMPAIGN]["source_lhe_budgets"]
    ]
    safe_processing, safe_merges = reconstruct_safe_ids(
        stream,
        allocation,
        source_budgets,
        args.good_seed_min,
        args.good_seed_max,
    )
    listing = get_recursive_listing(args)
    files = select_output_files(listing, safe_merges, args.endpoint, args.product)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("".join(f"{path}\n" for path in files), encoding="utf-8")
    print(f"inventory_sha256={inventory_digest}")
    print(f"selected_jpsi_sources={len(selected_seeds)}")
    print(f"reconstructed_jpsi_blocks={len(stream)}")
    print(f"validated_dps_block_references={checked_dps_blocks}")
    print(f"planned_safe_processing_nodes={len(safe_processing)}")
    print(f"planned_safe_merge_groups={len(safe_merges)}")
    print(f"listed_paths={len(listing)}")
    print(f"product={args.product}")
    print(f"written_files={len(files)}")
    print(f"output={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
