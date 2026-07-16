#!/usr/bin/env python3
"""Combine CSV files with matching headers into one CSV."""

import argparse
import csv
from pathlib import Path


def combine_csv(inputs: list[Path], output: Path, has_header: bool = True) -> int:
    if output.resolve() in {path.resolve() for path in inputs}:
        raise ValueError("Output must not be one of the input files")

    header = None
    if has_header:
        headers = []
        for path in inputs:
            with path.open(encoding="utf-8-sig", newline="") as file:
                headers.append(next(csv.reader(file), None))
        if not headers[0]:
            raise ValueError(f"{inputs[0]} is empty")
        if any(item != headers[0] for item in headers[1:]):
            raise ValueError("All input CSV files must have the same header")
        header = headers[0]

    rows_written = 0
    with output.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.writer(destination)
        if header:
            writer.writerow(header)
        for path in inputs:
            with path.open(encoding="utf-8-sig", newline="") as source:
                reader = csv.reader(source)
                if has_header:
                    next(reader)
                for row in reader:
                    writer.writerow(row)
                    rows_written += 1
    return rows_written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="combined CSV to create")
    parser.add_argument("inputs", nargs="+", type=Path, help="CSV files to combine")
    parser.add_argument(
        "--no-header", action="store_true", help="treat every row as data"
    )
    args = parser.parse_args()

    count = combine_csv(args.inputs, args.output, has_header=not args.no_header)
    print(f"Wrote {count} rows to {args.output}")


if __name__ == "__main__":
    main()
