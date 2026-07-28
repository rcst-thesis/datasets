"""
Extract character dialogue from a script file.
Handles two formats:
  - [17] ELLE:  Uy James, paki sira.       (numbered + character prefix)
  - (laughs)  I'm a man.                   (raw lines, no prefix)

Strips: line numbers, character names, parentheticals, excess whitespace.
"""

import re
import argparse
from pathlib import Path


def extract_dialogue(src: Path, dst: Path):
    lines = src.read_text(encoding="utf-8").splitlines()
    results = []

    for line in lines:
        # Remove leading line number e.g. [17]
        line = re.sub(r"^\[\d+\]\s*", "", line).strip()

        # Remove all parentheticals e.g. (laughs), (imitates James)
        line = re.sub(r"\(.*?\)", "", line).strip()

        # Collapse excess whitespace
        line = re.sub(r" {2,}", " ", line).strip()

        if not line:
            continue

        # If line has CHARACTER: prefix, strip it
        match = re.match(r"^[A-Z][A-Z\s]+:\s*(.*)", line)
        dialogue = match.group(1).strip() if match else line

        if dialogue:
            results.append(dialogue)

    dst.write_text("\n".join(results) + "\n", encoding="utf-8")
    print(f"Extracted {len(results)} dialogue lines → {dst}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract and clean dialogue from a script file."
    )
    parser.add_argument("input",  type=Path, help="Input script file")
    parser.add_argument("output", type=Path, nargs="?",
                        default=None, help="Output .txt file (default: <input>_dialogue.txt)")
    args = parser.parse_args()

    out = args.output or args.input.with_name(args.input.stem + "_dialogue.txt")
    extract_dialogue(args.input, out)