#!/usr/bin/env python3
"""Review Phase-2 shower efficiency from block manifests.

The script joins three pieces of evidence:

* coordinator manifests, which contain the reserved mixed-event span and output
  sidecar URL;
* processing node configs, which contain source slots and their LHE slice lists;
* processing sidecar manifests, which contain attempted LHE and retry counters.

It is intended for post-pilot checks of phi shower efficiency and LHE budget
usage. For IHEP XRootD URLs, pass --fetch-remote to copy missing sidecars into a
local cache with xrdcp.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Review Phase-2 shower attempted LHE files and retries"
    )
    parser.add_argument(
        "--pilot-dir",
        required=True,
        help="Pilot/output directory containing plan_subdags and node configs",
    )
    parser.add_argument(
        "--cache-dir",
        default="/tmp/chiw/phase2_shower_review_manifests",
        help="Local cache for copied remote processing manifests",
    )
    parser.add_argument(
        "--fetch-remote",
        action="store_true",
        help="Fetch missing root:// processing manifests with xrdcp",
    )
    parser.add_argument(
        "--mode",
        action="append",
        default=[],
        help="Only include source modes matching this value; repeatable. "
             "Default includes all modes.",
    )
    parser.add_argument("--json-output", default="", help="Write detailed JSON rows")
    parser.add_argument("--csv-output", default="", help="Write detailed CSV rows")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def safe_name(value: str) -> str:
    return quote(value, safe="").replace("%", "_")


def fetch_manifest(url: str, cache_dir: Path, fetch_remote: bool) -> Path | None:
    if not url:
        return None
    if url.startswith("file:"):
        return Path(url[5:])
    if not url.startswith("root://"):
        return Path(url)

    cache_dir.mkdir(parents=True, exist_ok=True)
    basename_match = cache_dir / Path(url.rstrip("/")).name
    if basename_match.exists() and basename_match.stat().st_size > 0:
        return basename_match
    local = cache_dir / safe_name(url)
    if local.exists() and local.stat().st_size > 0:
        return local
    if not fetch_remote:
        return None
    cmd = ["xrdcp", "-f", url, str(local)]
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode != 0:
        print(
            f"[WARN] xrdcp failed for {url}: {result.stderr.strip() or result.stdout.strip()}",
            file=sys.stderr,
        )
        return None
    return local


def collect_coord_blocks(pilot_dir: Path) -> dict[str, dict[str, Any]]:
    blocks: dict[str, dict[str, Any]] = {}
    for path in pilot_dir.glob("plan_subdags/*/job_*/coord_manifest_*.json"):
        payload = load_json(path)
        campaign = payload.get("campaign", "")
        for block in payload.get("mixed_blocks", []):
            idx = int(block.get("index", 0))
            job_id = f"JOB{int(payload.get('job_index', 0)):06d}_BLOCK{idx:06d}"
            blocks[job_id] = {
                "campaign": campaign,
                "job_id": job_id,
                "coord_manifest": str(path),
                "target_mixed_events": block.get("target_mixed_events"),
                "event_id_span": block.get("event_id_span"),
                "miniaod_url": block.get("miniaod_url", ""),
                "processing_manifest_url": block.get("processing_manifest_url", ""),
            }
    return blocks


def collect_processing_configs(pilot_dir: Path) -> dict[str, dict[str, Any]]:
    configs: dict[str, dict[str, Any]] = {}
    for path in pilot_dir.glob("plan_subdags/*/job_*/node_configs/processing/*.json"):
        payload = load_json(path)
        job_id = str(payload.get("job_id") or path.stem.replace("MIX_", ""))
        configs[job_id] = {
            "processing_config": str(path),
            "sources": payload.get("sources", []),
            "target_mixed_events": payload.get("target_mixed_events"),
            "event_id_span": payload.get("event_id_span"),
            "minimum_output_fraction": payload.get("minimum_output_fraction"),
        }
    return configs


def source_input_summary(source: dict[str, Any]) -> tuple[int, int, str]:
    blocks = source.get("blocks") or []
    if blocks:
        n_events = sum(int(block.get("n_events", 0) or 0) for block in blocks)
        paths = [str(block.get("path", "")) for block in blocks if block.get("path")]
        return len(blocks), n_events, ";".join(paths)
    inputs = source.get("inputs") or []
    return len(inputs), int(source.get("planned_lhe_events", 0) or 0), ";".join(map(str, inputs))


def build_rows(
    coord_blocks: dict[str, dict[str, Any]],
    configs: dict[str, dict[str, Any]],
    cache_dir: Path,
    fetch_remote: bool,
    modes: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for job_id, coord in sorted(coord_blocks.items()):
        cfg = configs.get(job_id, {})
        manifest_path = fetch_manifest(
            coord.get("processing_manifest_url", ""), cache_dir, fetch_remote
        )
        processing_manifest: dict[str, Any] = {}
        if manifest_path and manifest_path.exists():
            processing_manifest = load_json(manifest_path)

        stats_by_mode: dict[str, list[dict[str, Any]]] = {}
        for stat in processing_manifest.get("source_statistics", []):
            stats_by_mode.setdefault(str(stat.get("mode", "")), []).append(stat)

        for source in cfg.get("sources", []):
            mode = str(source.get("mode", ""))
            if modes and mode not in modes:
                continue
            slot = int(source.get("slot", len(rows)) or 0)
            stats_for_mode = stats_by_mode.get(mode, [])
            stat = stats_for_mode.pop(0) if stats_for_mode else {}
            n_inputs, planned_lhe_events, input_paths = source_input_summary(source)
            attempted = int(stat.get("attempted_lhe_events", 0) or 0)
            accepted = int(stat.get("actual_hepmc_events", stat.get("accepted_hepmc_events", 0)) or 0)
            retries = int(stat.get("total_hadronization_retries", 0) or 0)
            wall = float(stat.get("wall_time_seconds", 0.0) or 0.0)
            rows.append({
                "campaign": coord.get("campaign", ""),
                "job_id": job_id,
                "slot": slot,
                "mode": mode,
                "target_mixed_events": coord.get("target_mixed_events") or cfg.get("target_mixed_events"),
                "event_id_span": coord.get("event_id_span") or cfg.get("event_id_span"),
                "source_target_hepmc_events": source.get("target_hepmc_events"),
                "source_max_lhe_events": source.get("max_lhe_events"),
                "source_max_hadronization_retries": source.get("max_hadronization_retries"),
                "n_input_slices": n_inputs,
                "planned_lhe_events": planned_lhe_events,
                "attempted_lhe_events": attempted,
                "successful_pythia_events": int(stat.get("successful_pythia_events", 0) or 0),
                "accepted_hepmc_events": accepted,
                "failed_phi_selections": int(stat.get("failed_phi_selections", 0) or 0),
                "total_hadronization_retries": retries,
                "accepted_per_attempted_lhe": accepted / attempted if attempted else 0.0,
                "average_retries_per_accepted_event": retries / accepted if accepted else 0.0,
                "average_retries_per_attempted_event": retries / attempted if attempted else 0.0,
                "wall_time_seconds": wall,
                "accepted_per_wall_hour": accepted / (wall / 3600.0) if wall > 0 else 0.0,
                "completion_fraction": float(stat.get("completion_fraction", 0.0) or 0.0),
                "status": stat.get("status", "missing_manifest" if not stat else ""),
                "processing_manifest_url": coord.get("processing_manifest_url", ""),
                "processing_manifest_local": str(manifest_path or ""),
                "input_paths": input_paths,
            })
    return rows


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row["mode"]), []).append(row)
    summary = []
    for mode, items in sorted(groups.items()):
        attempted = sum(int(item["attempted_lhe_events"]) for item in items)
        accepted = sum(int(item["accepted_hepmc_events"]) for item in items)
        retries = sum(int(item["total_hadronization_retries"]) for item in items)
        wall = sum(float(item["wall_time_seconds"]) for item in items)
        summary.append({
            "mode": mode,
            "sources": len(items),
            "attempted_lhe_events": attempted,
            "accepted_hepmc_events": accepted,
            "failed_phi_selections": sum(int(item["failed_phi_selections"]) for item in items),
            "total_hadronization_retries": retries,
            "accepted_per_attempted_lhe": accepted / attempted if attempted else 0.0,
            "average_retries_per_accepted_event": retries / accepted if accepted else 0.0,
            "average_retries_per_attempted_event": retries / attempted if attempted else 0.0,
            "wall_time_seconds": wall,
            "accepted_per_wall_hour": accepted / (wall / 3600.0) if wall > 0 else 0.0,
        })
    return summary


def write_csv(path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with open(path, "w", encoding="utf-8", newline="") as handle:
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    pilot_dir = Path(args.pilot_dir)
    cache_dir = Path(args.cache_dir)
    modes = set(args.mode)

    coord_blocks = collect_coord_blocks(pilot_dir)
    configs = collect_processing_configs(pilot_dir)
    rows = build_rows(coord_blocks, configs, cache_dir, args.fetch_remote, modes)
    summary = aggregate(rows)

    payload = {"summary": summary, "rows": rows}
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    if args.csv_output:
        write_csv(args.csv_output, rows)

    print(json.dumps({"summary": summary, "n_rows": len(rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
