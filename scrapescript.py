"""
Hiligaynon ↔ English Parallel Bible Scraper
For MarianMT / Helsinki-NLP fine-tuning

Sources scraped:
  - Hiligaynon (HIL/APD) : bible.com version 10  (Ang Pulong Sang Dios)
  - English (KJV)         : bible.com version 1

Output files (auto-created):
  - hil_en_parallel.tsv   — verse-aligned TSV  (reference | HIL | EN)
  - train.hil / train.en  — plain-text parallel files for MarianMT
  - dev.hil   / dev.en    — validation split  (last ~5 % of verses)

Usage:
  pip install requests beautifulsoup4 tqdm
  python hil_en_scraper.py                   # full Bible (~31 000 verses)
  python hil_en_scraper.py --books MAT JHN   # specific books only
  python hil_en_scraper.py --resume          # skip already-scraped books
"""

import argparse
import csv
import json
import math
import os
import random
import re
import time
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL  = "https://www.bible.com/bible"
HIL_VER   = 10     # Ang Pulong Sang Dios (Hiligaynon)
EN_VER    = 1      # KJV English
DELAY     = 1.2    # seconds between requests (be polite)
OUT_DIR   = Path("hiligaynon_corpus")
CACHE_DIR = OUT_DIR / ".cache"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── All 66 books with chapter counts ──────────────────────────────────────────
BOOKS = [
    ("GEN",  50), ("EXO",  40), ("LEV",  27), ("NUM",  36), ("DEU",  34),
    ("JOS",  24), ("JDG",  21), ("RUT",   4), ("1SA",  31), ("2SA",  24),
    ("1KI",  22), ("2KI",  25), ("1CH",  29), ("2CH",  36), ("EZR",  10),
    ("NEH",  13), ("EST",  10), ("JOB",  42), ("PSA", 150), ("PRO",  31),
    ("ECC",  12), ("SNG",   8), ("ISA",  66), ("JER",  52), ("LAM",   5),
    ("EZK",  48), ("DAN",  12), ("HOS",  14), ("JOL",   3), ("AMO",   9),
    ("OBA",   1), ("JON",   4), ("MIC",   7), ("NAM",   3), ("HAB",   3),
    ("ZEP",   3), ("HAG",   2), ("ZEC",  14), ("MAL",   4),
    ("MAT",  28), ("MRK",  16), ("LUK",  24), ("JHN",  21), ("ACT",  28),
    ("ROM",  16), ("1CO",  16), ("2CO",  13), ("GAL",   6), ("EPH",   6),
    ("PHP",   4), ("COL",   4), ("1TH",   5), ("2TH",   3), ("1TI",   6),
    ("2TI",   4), ("TIT",   3), ("PHM",   1), ("HEB",  13), ("JAS",   5),
    ("1PE",   5), ("2PE",   3), ("1JN",   5), ("2JN",   1), ("3JN",   1),
    ("JUD",   1), ("REV",  22),
]
BOOK_DICT = {b: c for b, c in BOOKS}


# ── Helpers ────────────────────────────────────────────────────────────────────

def clean(text: str) -> str:
    """Strip verse numbers, footnote markers, extra whitespace."""
    text = re.sub(r"^\s*\d+\s*", "", text)          # leading verse number
    text = re.sub(r"\[\w+\]", "", text)              # footnote refs like [a]
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_chapter(version: int, book: str, chapter: int) -> dict[int, str]:
    """
    Fetch one chapter from bible.com and return {verse_num: text}.
    Uses a simple cache to avoid re-fetching.
    """
    cache_file = CACHE_DIR / f"{version}_{book}_{chapter}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))

    url = f"{BASE_URL}/{version}/{book}.{chapter}.KJV"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    ✗ HTTP error {book} {chapter} (ver {version}): {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    verses: dict[int, str] = {}

    # bible.com renders verses in <span> with data-usfm like "GEN.1.1"
    for span in soup.find_all("span", attrs={"data-usfm": True}):
        usfm = span["data-usfm"]  # e.g. "GEN.1.3"
        parts = usfm.split(".")
        if len(parts) != 3:
            continue
        try:
            v_num = int(parts[2])
        except ValueError:
            continue

        # Collect all text inside this verse span
        text = " ".join(
            s.get_text(" ", strip=True)
            for s in span.find_all(["span", "p"])
            if s.get_text(strip=True)
        ) or span.get_text(" ", strip=True)

        text = clean(text)
        if text:
            verses[v_num] = text

    # Fallback: look for class-based selectors used by older bible.com layouts
    if not verses:
        for tag in soup.select("[class*='verse'] [class*='content']"):
            raw = tag.get_text(" ", strip=True)
            raw = clean(raw)
            if raw:
                # Try to infer verse number from a sibling label
                parent = tag.find_parent()
                label  = parent.find(class_=re.compile(r"label|number")) if parent else None
                try:
                    v_num = int(label.get_text(strip=True)) if label else 0
                except (ValueError, AttributeError):
                    v_num = 0
                if v_num:
                    verses[v_num] = raw

    cache_file.write_text(json.dumps(verses, ensure_ascii=False), encoding="utf-8")
    time.sleep(DELAY)
    return verses


def scrape_parallel(books_to_scrape: list[tuple[str, int]]) -> list[dict]:
    """Scrape Hiligaynon + English for the given books, return aligned pairs."""
    rows: list[dict] = []
    total_chapters = sum(c for _, c in books_to_scrape)
    done = 0

    for book, num_chapters in books_to_scrape:
        print(f"\n📖  {book}  ({num_chapters} chapters)")
        for chap in range(1, num_chapters + 1):
            print(f"    Chapter {chap}/{num_chapters} ", end="", flush=True)

            hil_verses = fetch_chapter(HIL_VER, book, chap)
            en_verses  = fetch_chapter(EN_VER,  book, chap)

            common = sorted(set(hil_verses) & set(en_verses))
            for v in common:
                rows.append({
                    "reference": f"{book}.{chap}.{v}",
                    "hiligaynon": hil_verses[v],
                    "english":    en_verses[v],
                })
            print(f"→ {len(common)} pairs")
            done += 1

    return rows


# ── Output writers ─────────────────────────────────────────────────────────────

def write_tsv(rows: list[dict], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["reference", "hiligaynon", "english"],
                                delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ TSV  → {path}  ({len(rows):,} verse pairs)")


def write_marian_splits(rows: list[dict], out_dir: Path,
                        dev_frac: float = 0.05) -> None:
    """
    Write Helsinki-NLP / MarianMT style plain-text parallel files.

    train.hil + train.en  →  training set
    dev.hil   + dev.en    →  validation set
    """
    random.shuffle(rows)
    split = math.ceil(len(rows) * (1 - dev_frac))
    train, dev = rows[:split], rows[split:]

    for split_name, split_rows in [("train", train), ("dev", dev)]:
        for lang, key in [("hil", "hiligaynon"), ("en", "english")]:
            p = out_dir / f"{split_name}.{lang}"
            p.write_text(
                "\n".join(r[key] for r in split_rows) + "\n",
                encoding="utf-8"
            )
    print(f"  ✓ MarianMT splits:")
    print(f"      train.hil / train.en  — {len(train):,} pairs")
    print(f"      dev.hil   / dev.en    — {len(dev):,} pairs")


def write_json(rows: list[dict], path: Path) -> None:
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  ✓ JSON → {path}  ({len(rows):,} verse pairs)")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Hiligaynon↔English Bible Scraper for MarianMT")
    p.add_argument("--books", nargs="+", metavar="BOOK",
                   help="Limit to specific books e.g. --books MAT JHN REV")
    p.add_argument("--resume", action="store_true",
                   help="Skip books whose cache files already exist")
    p.add_argument("--dev-frac", type=float, default=0.05,
                   help="Fraction of data for validation split (default 0.05)")
    p.add_argument("--no-json", action="store_true",
                   help="Skip JSON output")
    return p.parse_args()


def main():
    args = parse_args()

    # ── Prepare directories ──
    OUT_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)

    # ── Filter books ──
    if args.books:
        requested = [b.upper() for b in args.books]
        unknown   = [b for b in requested if b not in BOOK_DICT]
        if unknown:
            print(f"✗ Unknown book codes: {unknown}")
            sys.exit(1)
        books_to_scrape = [(b, BOOK_DICT[b]) for b in requested]
    else:
        books_to_scrape = BOOKS

    if args.resume:
        # Only keep books where at least one chapter cache is missing
        def is_complete(book, chapters):
            return all(
                (CACHE_DIR / f"{HIL_VER}_{book}_{c}.json").exists() and
                (CACHE_DIR / f"{EN_VER}_{book}_{c}.json").exists()
                for c in range(1, chapters + 1)
            )
        skipped = [b for b, c in books_to_scrape if is_complete(b, c)]
        books_to_scrape = [(b, c) for b, c in books_to_scrape if not is_complete(b, c)]
        if skipped:
            print(f"  ↩  Resuming — skipping already-cached books: {skipped}")

    total_pairs_estimate = sum(c * 25 for _, c in books_to_scrape)
    print(f"\n✝  Hiligaynon↔English Bible Scraper")
    print(f"   Books     : {len(books_to_scrape)}")
    print(f"   Chapters  : {sum(c for _, c in books_to_scrape)}")
    print(f"   Est. pairs: ~{total_pairs_estimate:,}")
    print(f"   Output dir: {OUT_DIR.resolve()}\n")

    # ── Scrape ──
    rows = scrape_parallel(books_to_scrape)

    if not rows:
        print("\n✗ No data collected. Check your internet connection or try again later.")
        sys.exit(1)

    # ── Merge with existing TSV if resuming ──
    tsv_path = OUT_DIR / "hil_en_parallel.tsv"
    if args.resume and tsv_path.exists():
        existing = []
        with open(tsv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            existing = list(reader)
        existing_refs = {r["reference"] for r in existing}
        rows = existing + [r for r in rows if r["reference"] not in existing_refs]
        rows.sort(key=lambda r: r["reference"])
        print(f"\n  Merged with existing data → {len(rows):,} total pairs")

    # ── Write outputs ──
    print(f"\n💾  Writing outputs to {OUT_DIR}/")
    write_tsv(rows, tsv_path)
    write_marian_splits(rows, OUT_DIR, dev_frac=args.dev_frac)
    if not args.no_json:
        write_json(rows, OUT_DIR / "hil_en_parallel.json")

    print(f"\n✅  Done!  {len(rows):,} verse pairs collected.")
    print(f"\n   To train MarianMT (Helsinki-NLP):")
    print(f"   ┌──────────────────────────────────────────────────────┐")
    print(f"   │  pip install transformers sentencepiece sacremoses   │")
    print(f"   │                                                       │")
    print(f"   │  # Use the generated files:                          │")
    print(f"   │  train.hil  train.en  (training)                     │")
    print(f"   │  dev.hil    dev.en    (validation)                   │")
    print(f"   │                                                       │")
    print(f"   │  Fine-tune Helsinki-NLP/opus-mt-tl-en or             │")
    print(f"   │  Helsinki-NLP/opus-mt-en-tl as a starting point      │")
    print(f"   └──────────────────────────────────────────────────────┘")


if __name__ == "__main__":
    main()