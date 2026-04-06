#!/usr/bin/env python3
"""
scrape_bombo.py — Scrape Hiligaynon text from Bombo Radyo Iloilo.

Site    : https://iloilo.bomboradyo.com
Output  : data/bombo_raw.txt   — one sentence per line (monolingual)
          data/bombo.tsv       — src TAB tgt  (tgt empty; no EN parallel)
          data/bombo_log.jsonl — per-article metadata

Usage
-----
    pip install requests beautifulsoup4 langdetect tqdm

    python scrape_bombo.py
    python scrape_bombo.py --categories top-stories balita-espesyal
    python scrape_bombo.py --max-articles 200 --delay 2.0
    python scrape_bombo.py --resume
"""

from __future__ import annotations

import argparse
import email.utils
import json
import re
import sys
import time
import urllib.robotparser
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from langdetect import detect, LangDetectException
    HAS_LANGDETECT = True
except ImportError:
    HAS_LANGDETECT = False
    print("[warn] langdetect not installed — keyword heuristic used instead")

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# ══════════════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://iloilo.bomboradyo.com"

ALL_CATEGORIES = [
    "top-stories",           # Local News — most Hiligaynon
    "balita-espesyal",       # Special bulletins
    "police-report-iloilo",
    "politics",
    "national-news",
    "sports",
    "entertainment",
    "health-news",
    "business-news",
]

# URL path segments that are NOT article slugs
NON_ARTICLE_PATTERNS = re.compile(
    r"/(category|tag|author|page|search|\?)"
)

OUT_DIR     = Path("data")
MIN_WORDS   = 5
MIN_CHARS   = 40
MAX_CHARS   = 600
MAX_BACKOFF = 300

ACCEPTED_LANGS = {"tl", "ceb", "hil", "fil"}
HIL_MARKERS = {
    "ang", "nga", "sang", "kag", "sa", "si", "ni", "mga",
    "isa", "kon", "kay", "siya", "niya", "ining", "gid",
    "man", "lang", "na", "pa", "bangud", "apang", "kundi",
}

HEADERS = {
    "User-Agent": (
        "hil-corpus-scraper/1.2 (academic NLP; non-commercial) "
        "python-requests/2.x"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ══════════════════════════════════════════════════════════════════════════════
# Rate-limit-aware HTTP layer
# ══════════════════════════════════════════════════════════════════════════════

class RateLimitError(Exception):
    pass


def _parse_retry_after(val: str) -> float:
    val = val.strip()
    if val.isdigit():
        return float(val)
    try:
        dt = email.utils.parsedate_to_datetime(val)
        return max((dt - datetime.now(timezone.utc)).total_seconds(), 1.0)
    except Exception:
        return 10.0


def get(
    url: str,
    session: requests.Session,
    base_delay: float = 1.5,
    max_retries: int = 4,
    timeout: int = 20,
) -> Optional[requests.Response]:
    if not hasattr(session, "_rp_cache"):
        session._rp_cache = {}  # type: ignore[attr-defined]

    domain = urlparse(url).scheme + "://" + urlparse(url).netloc
    if domain not in session._rp_cache:  # type: ignore[attr-defined]
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(urljoin(domain, "/robots.txt"))
        try:
            rp.read()
        except Exception:
            pass
        session._rp_cache[domain] = rp  # type: ignore[attr-defined]

    if not session._rp_cache[domain].can_fetch(HEADERS["User-Agent"], url):  # type: ignore[attr-defined]
        print(f"  [robots] blocked: {url}")
        return None

    ts_key  = f"_last_{domain}"
    elapsed = time.time() - getattr(session, ts_key, 0.0)
    if elapsed < base_delay:
        time.sleep(base_delay - elapsed)

    backoff = 10.0
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=timeout)
            setattr(session, ts_key, time.time())

            if r.status_code == 429:
                wait = min(
                    _parse_retry_after(r.headers.get("Retry-After", str(backoff))),
                    MAX_BACKOFF,
                )
                print(f"  [429] waiting {wait:.0f}s (attempt {attempt}/{max_retries})")
                if attempt == max_retries:
                    raise RateLimitError(f"Max retries on {url}")
                time.sleep(wait)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            r.raise_for_status()

            remaining = r.headers.get("X-RateLimit-Remaining")
            if remaining is not None and int(remaining) < 10:
                print(f"  [warn] RateLimit-Remaining={remaining}, slowing down")
                time.sleep(5.0)

            return r

        except RateLimitError:
            raise
        except Exception as e:
            print(f"  [err] {url}: {e}")
            return None

    return None


# ══════════════════════════════════════════════════════════════════════════════
# Text utilities
# ══════════════════════════════════════════════════════════════════════════════

def is_hiligaynon(text: str) -> bool:
    words = re.findall(r"\b\w+\b", text.lower())
    if len(words) < 4:
        return True
    if HAS_LANGDETECT:
        try:
            return detect(text) in ACCEPTED_LANGS
        except LangDetectException:
            pass
    return len(set(words) & HIL_MARKERS) >= 2


def clean(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\[\d+\]", "", text)
    return text.strip()


def split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", text)
    out = []
    for p in parts:
        p = clean(p)
        words = p.split()
        if MIN_WORDS <= len(words) and MIN_CHARS <= len(p) <= MAX_CHARS:
            out.append(p)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Scraping
# ══════════════════════════════════════════════════════════════════════════════

def article_urls_from_category(
    session: requests.Session,
    category: str,
    max_articles: int,
    base_delay: float,
) -> Iterator[str]:
    """
    Paginate /category/<slug>/ and yield article URLs.

    VERIFIED from live HTML: article cards on the listing page use bare
    `h3 a` — there is NO wrapping <article> tag. Previous code used
    `article h3 a` which matched nothing.
    """
    page  = 1
    seen: set[str] = set()
    yielded = 0

    while yielded < max_articles:
        url = (
            f"{BASE_URL}/category/{category}/"
            if page == 1
            else f"{BASE_URL}/category/{category}/page/{page}/"
        )

        r = get(url, session, base_delay=base_delay)
        if r is None:
            break

        soup = BeautifulSoup(r.text, "html.parser")

        hrefs = [
            a.get("href", "").strip()
            for a in soup.select("h3 a")   # ← correct: no article wrapper
            if a.get("href")
        ]

        # Keep only article URLs on this domain (skip category/tag/author pages)
        hrefs = [
            h for h in hrefs
            if BASE_URL in h
            and not NON_ARTICLE_PATTERNS.search(urlparse(h).path)
        ]

        if not hrefs:
            break   # no more pages

        for href in hrefs:
            if href not in seen:
                seen.add(href)
                yield href
                yielded += 1
                if yielded >= max_articles:
                    return

        page += 1


def parse_article(r: requests.Response) -> tuple[str, list[str]]:
    """
    Extract (title, sentences) from an article page.

    VERIFIED from live HTML:
      Title   : first <h1> on the page
      Content : <p> tags inside <article>
                (the XPath div[2]/div[1]/div/div[3] resolves to the <p> block;
                 `article p` is equivalent and survives theme tweaks)
    """
    soup = BeautifulSoup(r.text, "html.parser")

    h1    = soup.find("h1")
    title = clean(h1.get_text()) if h1 else ""

    article = soup.find("article")
    if article is None:
        # Fallback: strip chrome and grab all p tags
        for sel in ("nav", "header", "footer", ".td-header-wrap", ".td-footer-container"):
            for el in soup.select(sel):
                el.decompose()
        paragraphs = soup.find_all("p")
    else:
        paragraphs = article.find_all("p")

    sentences: list[str] = []

    # Title is usually a complete Hiligaynon sentence — include it
    if title:
        sentences.extend(split_sentences(title))

    for p in paragraphs:
        text = clean(p.get_text(separator=" "))
        sentences.extend(split_sentences(text))

    return title, sentences


# ══════════════════════════════════════════════════════════════════════════════
# Checkpoint / resume
# ══════════════════════════════════════════════════════════════════════════════

def load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    urls = {l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip()}
    print(f"  [resume] {len(urls):,} URLs already visited")
    return urls


def save_checkpoint(path: Path, url: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(url + "\n")


def load_seen_sentences(path: Path) -> set[str]:
    if not path.exists():
        return set()
    sents = {l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip()}
    print(f"  [resume] {len(sents):,} sentences already collected")
    return sents


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def scrape(args: argparse.Namespace) -> None:
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)

    raw_txt  = out / "bombo_raw.txt"
    out_tsv  = out / "bombo.tsv"
    log_file = out / "bombo_log.jsonl"
    ckpt     = out / "bombo_checkpoint.txt"

    visited = load_checkpoint(ckpt)        if args.resume else set()
    seen    = load_seen_sentences(raw_txt) if args.resume else set()
    mode    = "a"                          if args.resume else "w"

    session = requests.Session()
    total_articles  = 0
    total_sentences = 0

    with (
        open(raw_txt,  mode, encoding="utf-8") as txt_f,
        open(out_tsv,  mode, encoding="utf-8") as tsv_f,
        open(log_file, mode, encoding="utf-8") as log_f,
    ):
        for cat in args.categories:
            print(f"\n→ {cat}")
            cat_total = 0

            url_iter = article_urls_from_category(
                session, cat, args.max_articles, args.delay
            )
            bar = tqdm(url_iter, desc=f"  {cat}", unit="art") if HAS_TQDM else url_iter

            for url in bar:
                if url in visited:
                    continue

                try:
                    r = get(url, session, base_delay=args.delay)
                except RateLimitError:
                    print("\n  [abort] persistent 429 — re-run with --resume")
                    return

                if r is None:
                    save_checkpoint(ckpt, url)
                    continue

                title, sentences = parse_article(r)

                kept = [
                    s for s in sentences
                    if s not in seen and is_hiligaynon(s)
                ]
                for s in kept:
                    seen.add(s)
                    txt_f.write(s + "\n")
                    tsv_f.write(s + "\t\n")

                log_f.write(json.dumps({
                    "url":      url,
                    "category": cat,
                    "title":    title,
                    "kept":     len(kept),
                    "ts":       time.strftime("%Y-%m-%dT%H:%M:%S"),
                }) + "\n")

                for f in (txt_f, tsv_f, log_f):
                    f.flush()

                save_checkpoint(ckpt, url)
                total_articles  += 1
                total_sentences += len(kept)
                cat_total       += len(kept)

            print(f"     {cat_total:,} sentences")

    print(f"\n{'='*52}")
    print(f"  Articles scraped : {total_articles:>8,}")
    print(f"  Sentences kept   : {total_sentences:>8,}")
    print(f"  Monolingual      : {raw_txt}")
    print(f"  TSV              : {out_tsv}")
    print(f"  Log              : {log_file}")
    print(f"{'='*52}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape Hiligaynon text from Bombo Radyo Iloilo.")
    p.add_argument("--categories", nargs="+", default=ALL_CATEGORIES,
                   choices=ALL_CATEGORIES, metavar="CAT",
                   help="Categories to scrape (default: all)")
    p.add_argument("--max-articles", type=int, default=500,
                   help="Max articles per category (default: 500)")
    p.add_argument("--delay", type=float, default=1.5,
                   help="Seconds between requests (default: 1.5)")
    p.add_argument("--resume", action="store_true",
                   help="Skip already-visited URLs")
    p.add_argument("--out-dir", type=Path, default=OUT_DIR,
                   help=f"Output directory (default: {OUT_DIR})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        scrape(args)
    except KeyboardInterrupt:
        print("\n[interrupted] Re-run with --resume to continue.")
        sys.exit(0)