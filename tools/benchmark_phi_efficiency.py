#!/usr/bin/env python3
"""Summarize phi shower efficiency benchmark manifests.

The runner side is intentionally simple: run representative shower jobs with
different retry limits, then point this tool at their `shower_*_manifest.json`
files to get comparable JSON/CSV summaries.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize phi efficiency manifests")
    parser.add_argument("manifests", nargs="+", help="shower manifest JSON files")
    parser.add_argument("--json-output", default="", help="Path to write summary JSON")
    parser.add_argument("--csv-output", default="", help="Path to write summary CSV")
    return parser.parse_args()


def load_manifest(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["_path"] = path
    return payload


def summarize(payloads: list[dict]) -> list[dict]:
    rows = []
    for item in payloads:
        attempted = int(item.get("attempted_lhe_events", 0) or 0)
        accepted = int(item.get("actual_hepmc_events", 0) or 0)
        retries = int(item.get("total_hadronization_retries", 0) or 0)
        wall = float(item.get("wall_time_seconds", 0.0) or 0.0)
        retry_limit = int(item.get("max_hadronization_retries", item.get("maxRetry", 0)) or 0)
        rows.append({
            "path": item.get("_path", ""),
            "mode": item.get("mode", ""),
            "retry_limit": retry_limit,
            "attempted_lhe_events": attempted,
            "accepted_hepmc_events": accepted,
            "accepted_per_attempted_lhe": accepted / attempted if attempted else 0.0,
            "accepted_per_wall_hour": accepted / (wall / 3600.0) if wall > 0 else 0.0,
            "total_hadronization_retries": retries,
            "average_retries_per_accepted_event": (
                retries / accepted if accepted else 0.0
            ),
            "average_retries_per_attempted_event": (
                retries / attempted if attempted else 0.0
            ),
            "wall_time_seconds": wall,
            "completion_fraction": float(item.get("completion_fraction", 0.0) or 0.0),
            "status": item.get("status", ""),
        })
    return rows


def main() -> int:
    args = parse_args()
    rows = summarize([load_manifest(path) for path in args.manifests])
    if args.json_output:
        Path(args.json_output).write_text(
            json.dumps(rows, indent=2) + "\n",
            encoding="utf-8",
        )
    if args.csv_output:
        with open(args.csv_output, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else [])
            if rows:
                writer.writeheader()
                writer.writerows(rows)
    if not args.json_output and not args.csv_output:
        print(json.dumps(rows, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
