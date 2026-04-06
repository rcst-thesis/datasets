#!/usr/bin/env python3
"""
opus_to_tsv.py — Convert OPUS corpus files to a TSV test set.

Supported input formats
-----------------------
  paired txt   Two aligned .txt files (one sentence per line)
               python opus_to_tsv.py -s src.txt -t tgt.txt -o out.tsv

  TMX          Translation Memory eXchange (.tmx)
               python opus_to_tsv.py --tmx corpus.tmx -o out.tsv --src-lang en --tgt-lang hil

  Moses XML    OPUS-style .xml corpus files (requires both src + tgt)
               python opus_to_tsv.py --xml-src en.xml --xml-tgt hil.xml -o out.tsv

Options
-------
  --src-col    Header label for source column  (default: src)
  --tgt-col    Header label for target column  (default: tgt)
  --max        Max number of pairs to write    (default: all)
  --shuffle    Randomly shuffle before slicing (useful for test set sampling)
  --seed       Random seed for --shuffle       (default: 42)
  --skip-empty Skip pairs where either side is blank
"""

import argparse
import csv
import random
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator


# ---------------------------------------------------------------------------
# Readers — each yields (src, tgt) string pairs
# ---------------------------------------------------------------------------

def read_paired_txt(src_path: Path, tgt_path: Path) -> Iterator[tuple[str, str]]:
    with src_path.open(encoding="utf-8") as sf, tgt_path.open(encoding="utf-8") as tf:
        for src_line, tgt_line in zip(sf, tf):
            yield src_line.rstrip("\n"), tgt_line.rstrip("\n")


def read_tmx(tmx_path: Path, src_lang: str, tgt_lang: str) -> Iterator[tuple[str, str]]:
    """Parse a TMX file. Lang matching is case-insensitive prefix (en matches en-US)."""
    src_lang = src_lang.lower()
    tgt_lang = tgt_lang.lower()

    tree = ET.parse(tmx_path)
    root = tree.getroot()

    for tu in root.iter("tu"):
        texts: dict[str, str] = {}
        for tuv in tu.findall("tuv"):
            lang = (tuv.get("lang") or tuv.get("{http://www.w3.org/XML/1998/namespace}lang") or "").lower()
            seg = tuv.find("seg")
            if seg is not None and seg.text:
                texts[lang] = seg.text.strip()

        # Match by prefix so "en" matches "en-US", "en-GB", etc.
        src_text = next((v for k, v in texts.items() if k.startswith(src_lang)), None)
        tgt_text = next((v for k, v in texts.items() if k.startswith(tgt_lang)), None)

        if src_text and tgt_text:
            yield src_text, tgt_text


def _extract_xml_sentences(xml_path: Path) -> list[str]:
    """Extract sentences from an OPUS Moses-style XML file."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    sentences = []
    for s_elem in root.iter("s"):
        tokens = [w.text for w in s_elem.iter("w") if w.text]
        sentence = " ".join(tokens).strip()
        if sentence:
            sentences.append(sentence)
    return sentences


def read_moses_xml(src_xml: Path, tgt_xml: Path) -> Iterator[tuple[str, str]]:
    src_sents = _extract_xml_sentences(src_xml)
    tgt_sents = _extract_xml_sentences(tgt_xml)
    for pair in zip(src_sents, tgt_sents):
        yield pair


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def write_tsv(
    pairs: list[tuple[str, str]],
    output_path: Path,
    src_col: str,
    tgt_col: str,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        writer.writerow([src_col, tgt_col])
        writer.writerows(pairs)
    return len(pairs)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Convert OPUS corpus files to a TSV test set.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Input modes (mutually exclusive groups)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--paired", nargs=2, metavar=("SRC_TXT", "TGT_TXT"),
                      help="Two aligned plain-text files")
    mode.add_argument("--tmx", metavar="TMX_FILE",
                      help="TMX translation memory file")
    mode.add_argument("--xml", nargs=2, metavar=("SRC_XML", "TGT_XML"),
                      help="OPUS Moses-XML corpus pair")

    # TMX language selection
    p.add_argument("--src-lang", default="en",
                   help="Source language code for TMX (default: en)")
    p.add_argument("--tgt-lang", default="hil",
                   help="Target language code for TMX (default: hil)")

    # Output
    p.add_argument("-o", "--output", required=True, metavar="OUT_TSV",
                   help="Output .tsv file path")
    p.add_argument("--src-col", default="src", help="Source column header (default: src)")
    p.add_argument("--tgt-col", default="tgt", help="Target column header (default: tgt)")

    # Sampling
    p.add_argument("--max", type=int, default=None, metavar="N",
                   help="Max sentence pairs to output")
    p.add_argument("--shuffle", action="store_true",
                   help="Shuffle pairs before slicing with --max")
    p.add_argument("--seed", type=int, default=42,
                   help="Random seed for --shuffle (default: 42)")
    p.add_argument("--skip-empty", action="store_true",
                   help="Drop pairs where either side is blank")

    return p.parse_args()


def main() -> None:
    args = parse_args()

    # --- Read ---
    print("Reading corpus...", end=" ", flush=True)
    if args.paired:
        src_path, tgt_path = Path(args.paired[0]), Path(args.paired[1])
        stream = read_paired_txt(src_path, tgt_path)
    elif args.tmx:
        stream = read_tmx(Path(args.tmx), args.src_lang, args.tgt_lang)
    else:
        src_xml, tgt_xml = Path(args.xml[0]), Path(args.xml[1])
        stream = read_moses_xml(src_xml, tgt_xml)

    pairs = list(stream)
    print(f"{len(pairs):,} pairs loaded.")

    # --- Filter ---
    if args.skip_empty:
        before = len(pairs)
        pairs = [(s, t) for s, t in pairs if s.strip() and t.strip()]
        print(f"Skipped {before - len(pairs):,} empty pairs.")

    # --- Sample ---
    if args.shuffle:
        random.seed(args.seed)
        random.shuffle(pairs)
        print(f"Shuffled (seed={args.seed}).")

    if args.max and args.max < len(pairs):
        pairs = pairs[: args.max]
        print(f"Truncated to {args.max:,} pairs.")

    # --- Write ---
    output_path = Path(args.output)
    n = write_tsv(pairs, output_path, args.src_col, args.tgt_col)
    print(f"Wrote {n:,} rows → {output_path}")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ET.ParseError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)