"""
Extract Hiligaynon column from a TSV Bible file.
- Removes reference (col 0) and English (col 2)
- Strips footnote references like: # 1:20 ... text ...
- Collapses excess whitespace
- Outputs one sentence per line
"""

import re
import argparse
from pathlib import Path


def clean_footnotes(text: str) -> str:
    # Remove patterns like: # 1:20 some footnote text
    # They repeat, so strip all occurrences
    text = re.sub(r"#\s*\d+:\d+[^#]*", "", text)
    # Collapse excess whitespace
    text = re.sub(r" {2,}", " ", text).strip()
    return text


def extract_hiligaynon(src: Path, dst: Path):
    lines = src.read_text(encoding="utf-8").splitlines()
    results = []

    for i, line in enumerate(lines):
        # Skip header row
        if i == 0:
            continue

        cols = line.split("\t")
        if len(cols) < 2:
            continue

        hil = cols[1].strip()
        hil = clean_footnotes(hil)

        if hil:
            results.append(hil)

    dst.write_text("\n".join(results) + "\n", encoding="utf-8")
    print(f"Extracted {len(results)} lines → {dst}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract Hiligaynon column from TSV and clean footnotes."
    )
    parser.add_argument("input",  type=Path, help="Input .tsv file")
    parser.add_argument("output", type=Path, nargs="?",
                        default=None, help="Output .txt file (default: <input>_hil.txt)")
    args = parser.parse_args()

    out = args.output or args.input.with_name(args.input.stem + "_hil.txt")
    extract_hiligaynon(args.input, out)
