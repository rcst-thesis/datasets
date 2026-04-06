#!/usr/bin/env python3
"""
jsonl_to_tsv.py — Convert a JSONL corpus to TSV, extracting src and trgs.

Each line in the JSONL must have:
  "src"  — source string
  "trgs" — list of target strings (first one is used by default)

Usage:
    python jsonl_to_tsv.py input.jsonl
    python jsonl_to_tsv.py input.jsonl -o output.tsv
    python jsonl_to_tsv.py input.jsonl --all-trgs   # one row per target
"""

import argparse
import csv
import json
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert JSONL corpus to TSV (src + trgs).")
    p.add_argument("input", help="Input .jsonl file")
    p.add_argument("-o", "--output", default=None,
                   help="Output .tsv file (default: <input>.tsv)")
    p.add_argument("--all-trgs", action="store_true",
                   help="Emit one row per target instead of only the first")
    p.add_argument("--src-col", default="src", help="Source column header (default: src)")
    p.add_argument("--tgt-col", default="tgt", help="Target column header (default: tgt)")
    p.add_argument("--skip-empty", action="store_true",
                   help="Skip rows where src or trg is blank")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_suffix(".tsv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    written = skipped = 0

    with (
        input_path.open(encoding="utf-8") as infile,
        output_path.open("w", encoding="utf-8", newline="") as outfile,
    ):
        writer = csv.writer(outfile, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        writer.writerow([args.src_col, args.tgt_col])

        for lineno, raw in enumerate(infile, 1):
            raw = raw.strip()
            if not raw:
                continue

            try:
                record = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"  Warning: skipping line {lineno} — {e}", file=sys.stderr)
                skipped += 1
                continue

            src = str(record.get("src", "")).strip()
            trgs = record.get("trgs", [])
            if isinstance(trgs, str):
                trgs = [trgs]

            targets = trgs if args.all_trgs else trgs[:1]

            for tgt in targets:
                tgt = str(tgt).strip()
                if args.skip_empty and (not src or not tgt):
                    skipped += 1
                    continue
                writer.writerow([src, tgt])
                written += 1

    print(f"Done. {written:,} rows written → {output_path}")
    if skipped:
        print(f"       {skipped:,} rows skipped.")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
