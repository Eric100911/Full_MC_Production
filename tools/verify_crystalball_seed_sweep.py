#!/usr/bin/env python3
"""Verify the CrystalBall seed sweep built by tools/crystalball_seed_sweep.sh.

Compares the staged LHE samples of every leg against each other. The question is
whether the job seed changes the physics at all, so the comparison is on
*uncompressed* content: the .lhe.gz files differ in gzip header bytes (mtime)
even when the payloads are identical, and hashing the compressed form therefore
gives a false negative. Both hashes are printed so the trap stays visible.

The leg list is discovered from generated/cb_seed_sweep/*/ and each leg's seed is
read from the node config JSON that was actually submitted, so this script cannot
drift from what ran.

Prediction under test (see tools/crystalball_seed_sweep.sh header): the
uncompressed payloads are byte-identical across all seeds, because the
CrystalBall addon has no seed input and its only RNG is driven by the compiled
constant MC_VEGAS:DSEED = 1234567d0.

Usage:
  tools/verify_crystalball_seed_sweep.py [--work-dir /tmp/chiw/cb_seed_verify]
                                         [--keep] [--proxy <path>]
                                         [--generated-dir generated/cb_seed_sweep]
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

SAMPLE_RE = re.compile(r"^sample_(?P<storage>[A-Za-z0-9_]+)_(?P<seed>\d+)\.lhe(?:\.gz)?$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--generated-dir", default="generated/cb_seed_sweep", help="directory holding the built legs.")
    parser.add_argument("--work-dir", default="/tmp/chiw/cb_seed_verify", help="scratch directory for downloads.")
    parser.add_argument("--keep", action="store_true", help="keep downloaded files (default: remove them).")
    parser.add_argument("--proxy", default="", help="X509 proxy path; exported as X509_USER_PROXY for xrdcp.")
    return parser.parse_args()


def load_legs(generated_dir: Path) -> list[dict]:
    """Read each leg's submitted config. Fails loudly rather than skipping.

    The leg keeps only one node in its DAG, but the generator wrote a config for
    every node it emitted, so the leg directory holds more than one. The DAG is
    therefore the ground truth for which node was actually submitted; picking a
    config by sort order would silently read the wrong seed.
    """
    legs: list[dict] = []
    for leg_dir in sorted(p for p in generated_dir.iterdir() if p.is_dir()):
        dag = leg_dir / f"{leg_dir.name}.dag"
        if not dag.is_file():
            raise SystemExit(f"{leg_dir}: missing {dag.name}")
        jobs = [line.split()[1] for line in dag.read_text(encoding="utf-8").splitlines()
                if line.startswith("JOB ")]
        if len(jobs) != 1:
            raise SystemExit(f"{dag}: expected exactly 1 JOB line, found {len(jobs)}")
        config = leg_dir / "node_configs" / "lhe_generation" / f"{jobs[0]}.json"
        if not config.is_file():
            raise SystemExit(f"{dag}: no config for submitted node {jobs[0]}")
        cfg = json.loads(config.read_text(encoding="utf-8"))
        for key in ("seed", "output_dir", "pool", "nmc", "unwevt"):
            if key not in cfg:
                raise SystemExit(f"{config}: missing key {key!r}")
        legs.append({"slug": leg_dir.name, "node": jobs[0], "config": config, **cfg})
    if not legs:
        raise SystemExit(f"{generated_dir}: no legs found")
    return legs


def sample_url(leg: dict) -> str:
    """Reconstruct the staged URL exactly as run_helac.sh stages it."""
    base = str(leg["output_dir"]).rstrip("/")
    name = f"sample_{leg['pool']}_{leg['seed']}.lhe.gz"
    return f"{base}/{name}"


def fetch(url: str, dest: Path, env: dict) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()
    proc = subprocess.run(
        ["xrdcp", "--force", url, str(dest)],
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        raise SystemExit(
            f"xrdcp failed for {url}\n  rc={proc.returncode}\n  {proc.stderr.strip()}"
        )


def digest(path: Path) -> tuple[str, str, int, int]:
    """Return (compressed_sha256, uncompressed_sha256, uncompressed_bytes, events)."""
    compressed = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(1 << 20):
            compressed.update(chunk)

    raw = hashlib.sha256()
    events = 0
    total = 0
    with gzip.open(path, "rb") as handle:
        while chunk := handle.read(1 << 20):
            raw.update(chunk)
            total += len(chunk)
            events += chunk.count(b"<event>")
    return compressed.hexdigest(), raw.hexdigest(), total, events


def main() -> int:
    args = parse_args()
    generated_dir = Path(args.generated_dir).resolve()
    work_dir = Path(args.work_dir)

    env = dict(os.environ)
    if args.proxy:
        env["X509_USER_PROXY"] = args.proxy

    legs = load_legs(generated_dir)
    print(f"[INFO] {len(legs)} legs from {generated_dir}")
    print(f"[INFO] work dir {work_dir}")

    rows: list[dict] = []
    for leg in legs:
        url = sample_url(leg)
        # The staged name must carry the seed taken from the submitted config;
        # a mismatch means the URL we probe is not the file this leg produced.
        match = SAMPLE_RE.match(Path(url).name)
        if not match or int(match.group("seed")) != int(leg["seed"]):
            raise SystemExit(f"{leg['slug']}: staged name/seed mismatch: {url}")
        print(f"[INFO] {leg['slug']}: seed={leg['seed']} nmc={leg['nmc']} unwevt={leg['unwevt']}")
        print(f"       {url}")

        dest = work_dir / leg["slug"] / Path(url).name
        fetch(url, dest, env)
        csha, usha, nbytes, events = digest(dest)
        rows.append(
            {
                "slug": leg["slug"],
                "seed": int(leg["seed"]),
                "nmc": int(leg["nmc"]),
                "unwevt": int(leg["unwevt"]),
                "bytes": nbytes,
                "events": events,
                "compressed_sha256": csha,
                "sha256": usha,
                "path": dest,
            }
        )

    width = max(len(r["slug"]) for r in rows) + 2
    print()
    print("=" * 118)
    print(
        f"{'leg':<{width}}{'seed':>6}{'nmc':>9}{'unwevt':>8}{'events':>9}{'bytes':>12}"
        f"  {'uncompressed sha256':<20}{'compressed sha256':<20}"
    )
    print("-" * 118)
    for r in rows:
        print(
            f"{r['slug']:<{width}}{r['seed']:>6}{r['nmc']:>9}{r['unwevt']:>8}"
            f"{r['events']:>9}{r['bytes']:>12}  {r['sha256'][:16]:<20}{r['compressed_sha256'][:16]:<20}"
        )
    print("=" * 118)

    groups: dict[str, list[dict]] = {}
    for r in rows:
        groups.setdefault(r["sha256"], []).append(r)
    compressed_distinct = len({r["compressed_sha256"] for r in rows})

    print()
    print(f"distinct uncompressed sha256: {len(groups)}")
    print(f"distinct compressed   sha256: {compressed_distinct}")
    if compressed_distinct > len(groups):
        print("  ^ the compressed forms differ while the payloads do not: gzip header mtime only.")
        print("    Hashing the .lhe.gz directly would have produced a false negative here.")

    print()
    if len(groups) == 1:
        only = next(iter(groups))
        print("VERDICT: SEED HAS NO EFFECT ON THE PHYSICS PAYLOAD (prediction confirmed).")
        print(f"  {len(rows)} legs across seeds {sorted(r['seed'] for r in rows)}")
        print(f"  share one uncompressed sha256 ({only}) and {rows[0]['events']} events.")
        print("  The seed reached the worker (asserted at build time) and changed nothing.")
        print("  -> independent-seed budget sizing is blocked on Phase B RNG-seed plumbing,")
        print("     not on further measurement. Do not size a campaign from these counts.")
        rc = 0
    else:
        print("VERDICT: SEED AFFECTS THE PHYSICS PAYLOAD (prediction FALSIFIED).")
        print("  Per-seed event counts, which is the yield spread Phase C needs:")
        for sha, members in sorted(groups.items(), key=lambda kv: kv[1][0]["events"]):
            seeds = ", ".join(str(m["seed"]) for m in members)
            print(f"    {members[0]['events']:>7} events   seeds {seeds}   sha {sha[:16]}")
        counts = sorted(r["events"] for r in rows)
        print(f"  min {counts[0]}  max {counts[-1]}  spread {counts[-1] - counts[0]}")
        rc = 0

    if not args.keep:
        for r in rows:
            r["path"].unlink(missing_ok=True)
    else:
        print()
        print(f"[INFO] downloads kept under {work_dir}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
