"""
clean_corpus.py — Apply recommendation-based cleaning to a corpus file.

Supports:
  - Plain .txt  (one line per row, no header)
  - .tsv / .csv with any number of columns

Checks applied (in order):
  1. Remove over-long lines       (> max_tokens in ANY column)
  2. Remove too-short lines       (< min_tokens in ALL columns)
  3. Remove length-ratio outliers (only when ncols >= 2)
  4. Deduplicate exact rows
  5. Strip HTML tags              (in-place, rows kept)
  6. Remove punctuation-only / numeric-only rows

Usage:
  python clean_corpus.py corpus.txt
  python clean_corpus.py corpus.tsv --out cleaned.tsv --max-tokens 200 --min-tokens 3
"""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:
    import pandas as pd
except ImportError:
    sys.exit("[ERROR] pandas is required:  pip install pandas")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class Args:
    input:      Path
    output:     Path
    max_tokens: int
    min_tokens: int
    ratio_min:  float
    ratio_max:  float


Step = Callable[[pd.DataFrame, Args], tuple[pd.DataFrame, int]]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TOK_RE    = re.compile(r"\b\w+\b")
_HTML_RE   = re.compile(r"<[^>]+>")
_HAS_ALPHA = re.compile(r"[a-zA-Z\u00C0-\uFFFF]")


def tok(s: str) -> int:
    return len(_TOK_RE.findall(str(s)))

def all_cols(df: pd.DataFrame, fn) -> pd.Series:
    """Boolean mask: fn is True for ALL columns."""
    return pd.concat([df.iloc[:, c].apply(fn) for c in range(df.shape[1])], axis=1).all(axis=1)

def any_col(df: pd.DataFrame, fn) -> pd.Series:
    """Boolean mask: fn is True for ANY column."""
    return pd.concat([df.iloc[:, c].apply(fn) for c in range(df.shape[1])], axis=1).any(axis=1)


# ---------------------------------------------------------------------------
# Cleaning steps
# ---------------------------------------------------------------------------

def remove_long(df: pd.DataFrame, args: Args) -> tuple[pd.DataFrame, int]:
    mask = ~any_col(df, lambda s: tok(s) > args.max_tokens)
    return df[mask], (~mask).sum()


def remove_short(df: pd.DataFrame, args: Args) -> tuple[pd.DataFrame, int]:
    mask = all_cols(df, lambda s: tok(s) >= args.min_tokens)
    return df[mask], (~mask).sum()


def remove_ratio_outliers(df: pd.DataFrame, args: Args) -> tuple[pd.DataFrame, int]:
    if df.shape[1] < 2:
        return df, 0   # skip — ratio only meaningful for parallel columns
    ratio = df.iloc[:, 0].apply(tok) / df.iloc[:, 1].apply(lambda s: max(tok(s), 1))
    mask = (ratio >= args.ratio_min) & (ratio <= args.ratio_max)
    return df[mask], (~mask).sum()


def deduplicate(df: pd.DataFrame, _args: Args) -> tuple[pd.DataFrame, int]:
    before = len(df)
    return df.drop_duplicates(), before - len(df)


def strip_html(df: pd.DataFrame, _args: Args) -> tuple[pd.DataFrame, int]:
    clean = lambda s: _HTML_RE.sub("", str(s)).strip()
    has_html = any_col(df, lambda s: bool(_HTML_RE.search(str(s))))
    n = has_html.sum()
    df = df.copy()
    for c in range(df.shape[1]):
        df.iloc[:, c] = df.iloc[:, c].apply(clean)
    return df, n


def remove_punc_numeric(df: pd.DataFrame, _args: Args) -> tuple[pd.DataFrame, int]:
    has_word = lambda s: bool(_HAS_ALPHA.search(str(s)))
    mask = all_cols(df, has_word)
    return df[mask], (~mask).sum()


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

STEPS: list[tuple[str, Step]] = [
    ("Over-long lines",          remove_long),
    ("Too-short lines",          remove_short),
    ("Length-ratio outliers",    remove_ratio_outliers),
    ("Exact duplicates",         deduplicate),
    ("HTML tags stripped",       strip_html),
    ("Punct/numeric-only lines", remove_punc_numeric),
]


# ---------------------------------------------------------------------------
# File I/O — auto-detect format
# ---------------------------------------------------------------------------

def load(path: Path) -> tuple[pd.DataFrame, bool]:
    """Return (df, is_plain_txt).  Plain txt → single column, no header."""
    if path.suffix.lower() == ".txt":
        lines = path.read_text(encoding="utf-8").splitlines()
        return pd.DataFrame(lines, columns=["text"]), True

    sep = "\t" if path.suffix.lower() == ".tsv" else ","
    return pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False), False


def save(df: pd.DataFrame, path: Path, is_txt: bool) -> None:
    if is_txt:
        path.write_text("\n".join(df.iloc[:, 0].tolist()) + "\n", encoding="utf-8")
    else:
        sep = "\t" if path.suffix.lower() == ".tsv" else ","
        df.to_csv(path, sep=sep, index=False)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(args: Args) -> None:
    print(f"Reading {args.input} …")
    df, is_txt = load(args.input)
    total_in   = len(df)
    ncols      = df.shape[1]
    fmt        = "plain txt" if is_txt else f"{ncols}-column {'TSV' if args.input.suffix == '.tsv' else 'CSV'}"
    print(f"  {total_in:,} lines loaded  ({fmt})\n")

    pad = max(len(name) for name, _ in STEPS)
    for name, step in STEPS:
        df, n_removed = step(df, args)
        status = f"-{n_removed:,}" if n_removed else "  ok"
        print(f"  {name:<{pad}}  {status}")

    print(f"\n  {'Total removed':<{pad}}  -{total_in - len(df):,}")
    print(f"  {'Lines kept':<{pad}}   {len(df):,}")

    save(df, args.output, is_txt)
    print(f"\nSaved → {args.output}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> Args:
    p = argparse.ArgumentParser(description="Clean a corpus txt/tsv/csv file.")
    p.add_argument("input",          type=Path,  help="Input file (.txt / .tsv / .csv)")
    p.add_argument("--out",          type=Path,  default=None)
    p.add_argument("--max-tokens",   type=int,   default=150)
    p.add_argument("--min-tokens",   type=int,   default=3)
    p.add_argument("--ratio-min",    type=float, default=0.5)
    p.add_argument("--ratio-max",    type=float, default=9.0)
    a = p.parse_args()

    return Args(
        input      = a.input,
        output     = a.out or a.input.with_stem(a.input.stem + "_clean"),
        max_tokens = a.max_tokens,
        min_tokens = a.min_tokens,
        ratio_min  = a.ratio_min,
        ratio_max  = a.ratio_max,
    )


if __name__ == "__main__":
    run(parse_args())
