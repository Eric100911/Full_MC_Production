#!/usr/bin/env python3
"""Generate a synthetic valid LHE file with N events for testing.

Usage:
  python3 generate_synthetic_lhe.py --n-events 100 --output test.lhe
  python3 generate_synthetic_lhe.py --n-events 100 --output test.lhe \
      --beam1 2212 --beam2 2212 --ebeam1 6500 --ebeam2 6500
  python3 generate_synthetic_lhe.py --n-events 100 --output test2.lhe \
      --beam1 11 --beam2 -11 --ebeam1 45.6 --ebeam2 45.6
"""

from __future__ import annotations

import argparse
import sys


def generate(
    output_path: str,
    n_events: int,
    beam1: int = 2212,
    beam2: int = 2212,
    ebeam1: float = 6500.0,
    ebeam2: float = 6500.0,
    pdfgup1: int = -1,
    pdfgup2: int = -1,
    idwtup: int = 3,
    generator_name: str = "synthetic_test",
) -> None:
    with open(output_path, "w") as f:
        f.write('<LesHouchesEvents version="1.0">\n')
        f.write("  <header>\n")
        f.write(f"    <generator_name>{generator_name}</generator_name>\n")
        f.write("  </header>\n")
        f.write("  <init>\n")
        f.write(
            f"  {beam1} {beam2} {ebeam1:.6e} {ebeam2:.6e} "
            f"{pdfgup1} {pdfgup2} {idwtup} 1\n"
        )
        f.write("  1.234000e-04 1.000000e-05 1.000000e+00 1\n")
        f.write("  </init>\n")

        for i in range(n_events):
            weight = 1.0 / (i + 1)
            f.write("  <event>\n")
            f.write(
                f"  2 1 {weight:.6e} 91.234 1.234000e-03 5.670000e-04\n"
            )
            # Two-particle final state (e.g. J/psi + J/psi)
            f.write(
                "       443 2  0  0  0  0  0.00e+00  0.00e+00"
                "  1.500000e+01  1.500000e+01  3.096900e+00  0.00e+00  9.00e+00\n"
            )
            f.write(
                "       443 2  0  0  0  0  0.00e+00  0.00e+00"
                " -1.500000e+01  1.500000e+01  3.096900e+00  0.00e+00  9.00e+00\n"
            )
            f.write("  </event>\n")

        f.write("</LesHouchesEvents>\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a synthetic LHE file for testing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--n-events", type=int, required=True, help="Number of events.")
    parser.add_argument("--output", required=True, help="Output LHE file path.")
    parser.add_argument("--beam1", type=int, default=2212)
    parser.add_argument("--beam2", type=int, default=2212)
    parser.add_argument("--ebeam1", type=float, default=6500.0)
    parser.add_argument("--ebeam2", type=float, default=6500.0)
    parser.add_argument("--pdfgup1", type=int, default=-1)
    parser.add_argument("--pdfgup2", type=int, default=-1)
    parser.add_argument("--idwtup", type=int, default=3)
    parser.add_argument("--generator-name", default="synthetic_test")

    args = parser.parse_args(argv)

    generate(
        output_path=args.output,
        n_events=args.n_events,
        beam1=args.beam1,
        beam2=args.beam2,
        ebeam1=args.ebeam1,
        ebeam2=args.ebeam2,
        pdfgup1=args.pdfgup1,
        pdfgup2=args.pdfgup2,
        idwtup=args.idwtup,
        generator_name=args.generator_name,
    )
    print(f"Wrote {args.n_events} events to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
