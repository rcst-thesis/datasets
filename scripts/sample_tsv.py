#!/usr/bin/env python3
"""Write a random subset of an existing TSV file."""

import argparse
import csv
import random
from pathlib import Path


def sample_tsv(input_path: Path, output_path: Path, count: int, seed: int) -> None:
    if count < 1:
        raise ValueError("count must be at least 1")
    if input_path.resolve() == output_path.resolve():
        raise ValueError("input and output must be different files")

    rng = random.Random(seed)
    sample: list[list[str]] = []

    with input_path.open(encoding="utf-8", newline="") as source:
        reader = csv.reader(source, delimiter="\t")
        header = next(reader, None)
        if header is None:
            raise ValueError("input TSV is empty")

        for index, row in enumerate(reader):
            if index < count:
                sample.append(row)
            else:
                replacement = rng.randint(0, index)
                if replacement < count:
                    sample[replacement] = row

    if len(sample) < count:
        raise ValueError(f"requested {count:,} rows, but input has only {len(sample):,}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.writer(destination, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(sample)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="source TSV")
    parser.add_argument("count", type=int, help="number of data rows to sample")
    parser.add_argument("-o", "--output", type=Path, required=True, help="output TSV")
    parser.add_argument("--seed", type=int, default=42, help="random seed (default: 42)")
    args = parser.parse_args()

    try:
        sample_tsv(args.input, args.output, args.count, args.seed)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    print(f"Wrote {args.count:,} random rows to {args.output}")


if __name__ == "__main__":
    main()
