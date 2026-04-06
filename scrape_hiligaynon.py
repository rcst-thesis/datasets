"""
scrape_hiligaynon.py — Hiligaynon (Ilonggo) corpus scraper
===========================================================
Collects Hiligaynon text from free public sources and outputs
a monolingual .txt file suitable for SentencePiece training,
and a TSV of sentence pairs where English equivalents are found.

Sources (all free, publicly accessible)
----------------------------------------
  1. Hiligaynon Wikipedia API   — hil.wikipedia.org
     Clean, structured, no JS rendering needed. Uses the MediaWiki
     API to pull article text in bulk. Best volume source.

  2. Panay News                 — panaynews.net
     Iloilo-based news outlet. Publishes mostly English but has
     Hiligaynon headlines and occasional full Ilonggo articles.

  3. The Daily Guardian         — dailyguardian.com.ph
     Western Visayas regional paper. Same mix as Panay News.

  4. Bombo Radyo Iloilo         — iloi.bombradyo.com
     Radio station news feed. Short Hiligaynon news blurbs.

Usage
-----
    pip install requests beautifulsoup4 langdetect tqdm

    # Wikipedia only (fastest, most volume)
    python scrape_hiligaynon.py --sources wikipedia

    # All sources
    python scrape_hiligaynon.py

    # Limit Wikipedia articles
    python scrape_hiligaynon.py --wiki-limit 500

    # Resume interrupted run
    python scrape_hiligaynon.py --resume

Output
------
    data/hil_raw.txt           — one sentence per line (monolingual)
    data/hil_scraped.tsv       — hil TAB en  (where EN found; else empty)
    data/scrape_log.jsonl      — per-article metadata for auditing

Notes
-----
  * Respects robots.txt via urllib.robotparser.
  * Rate-limited to 1 req/s per domain (configurable).
  * Skips sentences shorter than MIN_CHARS (filters nav/boilerplate).
  * langdetect used to filter out non-Hiligaynon paragraphs.
    Hiligaynon and Tagalog share many features so langdetect
    often returns "tl" — the filter accepts both "tl" and "ceb"
    in addition to the rare "hil" detection.
  * Sentence splitting is whitespace/punctuation based — no NLTK
    needed, which keeps the dependency list minimal.
"""

from __future__ import annotations

import argparse
import json
import re
import time
import urllib.robotparser
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ── optional deps ─────────────────────────────────────────────────────────────
try:
    from langdetect import detect, LangDetectException
    HAS_LANGDETECT = True
except ImportError:
    HAS_LANGDETECT = False
    print("[warn] langdetect not installed — language filtering disabled")
    print("       pip install langdetect")

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════

OUT_DIR       = Path("data")
RAW_TXT       = OUT_DIR / "hil_raw.txt"
SCRAPED_TSV   = OUT_DIR / "hil_scraped.tsv"
LOG_FILE      = OUT_DIR / "scrape_log.jsonl"

MIN_CHARS     = 40      # ignore sentences shorter than this
MIN_WORDS     = 5       # ignore sentences with fewer words
MAX_CHARS     = 500     # ignore very long lines (likely boilerplate)
RATE_LIMIT    = 1.2     # seconds between requests to the same domain

# langdetect codes we accept as likely Hiligaynon/Visayan
ACCEPTED_LANGS = {"tl", "ceb", "hil", "fil"}

HEADERS = {
    "User-Agent": (
        "maral-hil-corpus-scraper/1.0 "
        "(academic NLP research; contact: thesis project) "
        "requests/2.x python/3.12"
    )
}


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    """Per-domain rate limiter."""
    def __init__(self, delay: float = RATE_LIMIT):
        self.delay   = delay
        self._last: dict[str, float] = {}

    def wait(self, url: str) -> None:
        domain = urlparse(url).netloc
        elapsed = time.time() - self._last.get(domain, 0)
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last[domain] = time.time()


class RobotsCache:
    """Cache robots.txt per domain."""
    def __init__(self):
        self._cache: dict[str, urllib.robotparser.RobotFileParser] = {}

    def allowed(self, url: str) -> bool:
        domain = urlparse(url).scheme + "://" + urlparse(url).netloc
        if domain not in self._cache:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(urljoin(domain, "/robots.txt"))
            try:
                rp.read()
            except Exception:
                pass  # if robots.txt unreachable, allow
            self._cache[domain] = rp
        return self._cache[domain].can_fetch(HEADERS["User-Agent"], url)


_limiter = RateLimiter()
_robots  = RobotsCache()


def get(url: str, session: requests.Session, timeout: int = 15) -> Optional[requests.Response]:
    """Rate-limited GET respecting robots.txt. Returns None on failure."""
    if not _robots.allowed(url):
        print(f"  [robots] blocked: {url}")
        return None
    _limiter.wait(url)
    try:
        r = session.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"  [err] {url}: {e}")
        return None


def is_hiligaynon(text: str) -> bool:
    """
    Returns True if text is likely Hiligaynon/Visayan.
    Accepts tl/ceb/hil/fil from langdetect, plus a keyword
    heuristic as fallback when langdetect is unavailable.
    """
    if len(text.split()) < 4:
        return True  # too short to detect reliably — keep it
    if HAS_LANGDETECT:
        try:
            return detect(text) in ACCEPTED_LANGS
        except LangDetectException:
            pass
    # heuristic: common Hiligaynon/Visayan function words
    markers = {"ang", "nga", "sang", "kag", "sa", "si", "ni", "mga", "isa", "kon"}
    words   = set(re.findall(r"\b\w+\b", text.lower()))
    return len(words & markers) >= 2


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\[[\d]+\]", "", text)   # remove footnote markers [1]
    text = text.strip()
    return text


def split_sentences(text: str) -> list[str]:
    """Naive sentence splitter on . ! ? boundaries."""
    parts = re.split(r"(?<=[.!?])\s+", text)
    out = []
    for p in parts:
        p = clean_text(p)
        words = p.split()
        if (
            MIN_WORDS <= len(words)
            and MIN_CHARS <= len(p) <= MAX_CHARS
            and not p.startswith("http")
        ):
            out.append(p)
    return out


def log_article(log_f, source: str, url: str, n_sentences: int, title: str = "") -> None:
    log_f.write(json.dumps({
        "source":      source,
        "url":         url,
        "title":       title,
        "n_sentences": n_sentences,
        "ts":          time.strftime("%Y-%m-%dT%H:%M:%S"),
    }) + "\n")
    log_f.flush()


# ══════════════════════════════════════════════════════════════════════════════
# Source 1 — Hiligaynon Wikipedia (MediaWiki API)
# ══════════════════════════════════════════════════════════════════════════════

WIKI_API = "https://hil.wikipedia.org/w/api.php"


def wiki_all_titles(session: requests.Session, limit: int) -> list[str]:
    """Fetch up to `limit` article titles via allpages API."""
    titles = []
    params = {
        "action":      "query",
        "list":        "allpages",
        "aplimit":     "500",
        "apnamespace": "0",
        "apfilterredir": "nonredirects",
        "format":      "json",
    }
    print(f"  Fetching article list from Hiligaynon Wikipedia ...")
    while len(titles) < limit:
        _limiter.wait(WIKI_API)
        try:
            r = session.get(WIKI_API, params=params, headers=HEADERS, timeout=15)
            data = r.json()
        except Exception as e:
            print(f"  [err] Wikipedia API: {e}")
            break

        pages = data.get("query", {}).get("allpages", [])
        titles.extend(p["title"] for p in pages)

        cont = data.get("continue", {}).get("apcontinue")
        if not cont or len(titles) >= limit:
            break
        params["apcontinue"] = cont

    return titles[:limit]


def wiki_article_text(session: requests.Session, title: str) -> Optional[str]:
    """Fetch plain text of one article via the parse API."""
    params = {
        "action":  "parse",
        "page":    title,
        "prop":    "text",
        "format":  "json",
    }
    _limiter.wait(WIKI_API)
    try:
        r = session.get(WIKI_API, params=params, headers=HEADERS, timeout=15)
        data = r.json()
        html = data.get("parse", {}).get("text", {}).get("*", "")
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        # remove tables, infoboxes, navboxes
        for tag in soup.select("table, .navbox, .infobox, .mw-editsection"):
            tag.decompose()
        return soup.get_text(separator=" ")
    except Exception as e:
        print(f"  [err] parse {title}: {e}")
        return None


def scrape_wikipedia(
    session:   requests.Session,
    writer_txt,
    writer_tsv,
    log_f,
    seen:      set[str],
    limit:     int,
) -> int:
    titles = wiki_all_titles(session, limit)
    total  = 0
    bar    = tqdm(titles, desc="  Wikipedia") if HAS_TQDM else titles

    for title in bar:
        text = wiki_article_text(session, title)
        if not text:
            continue

        sentences = split_sentences(text)
        kept = []
        for s in sentences:
            if s in seen:
                continue
            if not is_hiligaynon(s):
                continue
            seen.add(s)
            kept.append(s)

        for s in kept:
            writer_txt.write(s + "\n")
            writer_tsv.write(s + "\t\n")   # no EN equivalent from Wikipedia

        log_article(log_f, "wikipedia", f"https://hil.wikipedia.org/wiki/{title}",
                    len(kept), title)
        total += len(kept)

    return total


# ══════════════════════════════════════════════════════════════════════════════
# Source 2 — Panay News  (panaynews.net)
# ══════════════════════════════════════════════════════════════════════════════

PANAYNEWS_SITEMAP = "https://www.panaynews.net/sitemap_index.xml"


def panaynews_article_urls(session: requests.Session, max_urls: int) -> list[str]:
    """Pull article URLs from the sitemap."""
    urls = []
    r = get(PANAYNEWS_SITEMAP, session)
    if not r:
        return urls
    soup = BeautifulSoup(r.text, "xml")
    # sitemap index → individual sitemaps
    for loc in soup.find_all("loc"):
        sub_url = loc.text.strip()
        if "post-sitemap" not in sub_url:
            continue
        rs = get(sub_url, session)
        if not rs:
            continue
        sub = BeautifulSoup(rs.text, "xml")
        for sloc in sub.find_all("loc"):
            urls.append(sloc.text.strip())
            if len(urls) >= max_urls:
                return urls
    return urls


def scrape_news_article(
    session:    requests.Session,
    url:        str,
    source:     str,
    writer_txt,
    writer_tsv,
    log_f,
    seen:       set[str],
) -> int:
    r = get(url, session)
    if not r:
        return 0

    soup  = BeautifulSoup(r.text, "html.parser")
    title = (soup.find("h1") or soup.find("title") or soup.new_tag("x")).get_text(strip=True)

    # try common article body selectors
    body = (
        soup.find("div", class_=re.compile(r"entry-content|article-body|post-content|td-post-content", re.I))
        or soup.find("article")
    )
    if not body:
        return 0

    text      = body.get_text(separator=" ")
    sentences = split_sentences(text)
    kept      = []

    for s in sentences:
        if s in seen:
            continue
        if not is_hiligaynon(s):
            continue
        seen.add(s)
        kept.append(s)

    for s in kept:
        writer_txt.write(s + "\n")
        writer_tsv.write(s + "\t\n")

    log_article(log_f, source, url, len(kept), title)
    return len(kept)


def scrape_panaynews(
    session:    requests.Session,
    writer_txt,
    writer_tsv,
    log_f,
    seen:       set[str],
    max_urls:   int = 300,
) -> int:
    print(f"  Fetching Panay News article list ...")
    urls  = panaynews_article_urls(session, max_urls)
    total = 0
    bar   = tqdm(urls, desc="  Panay News") if HAS_TQDM else urls
    for url in bar:
        total += scrape_news_article(
            session, url, "panaynews", writer_txt, writer_tsv, log_f, seen
        )
    return total


# ══════════════════════════════════════════════════════════════════════════════
# Source 3 — Daily Guardian  (dailyguardian.com.ph)
# ══════════════════════════════════════════════════════════════════════════════

GUARDIAN_BASE = "https://dailyguardian.com.ph"


def guardian_article_urls(session: requests.Session, max_urls: int) -> list[str]:
    urls  = []
    page  = 1
    while len(urls) < max_urls:
        r = get(f"{GUARDIAN_BASE}/page/{page}/", session)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.select("h2.entry-title a, h3.entry-title a")
        if not links:
            break
        for a in links:
            href = a.get("href", "")
            if href and href.startswith("http"):
                urls.append(href)
        page += 1
    return urls[:max_urls]


def scrape_guardian(
    session:    requests.Session,
    writer_txt,
    writer_tsv,
    log_f,
    seen:       set[str],
    max_urls:   int = 200,
) -> int:
    print(f"  Fetching Daily Guardian article list ...")
    urls  = guardian_article_urls(session, max_urls)
    total = 0
    bar   = tqdm(urls, desc="  Daily Guardian") if HAS_TQDM else urls
    for url in bar:
        total += scrape_news_article(
            session, url, "daily_guardian", writer_txt, writer_tsv, log_f, seen
        )
    return total


# ══════════════════════════════════════════════════════════════════════════════
# Source 4 — Bombo Radyo Iloilo  (iloi.bomboradyo.com)
# ══════════════════════════════════════════════════════════════════════════════

BOMBO_BASE = "https://iloi.bomboradyo.com"


def bombo_article_urls(session: requests.Session, max_urls: int) -> list[str]:
    urls = []
    page = 1
    while len(urls) < max_urls:
        r = get(f"{BOMBO_BASE}/page/{page}/", session)
        if not r:
            break
        soup  = BeautifulSoup(r.text, "html.parser")
        links = soup.select("h2 a, h3 a, .entry-title a")
        if not links:
            break
        for a in links:
            href = a.get("href", "")
            if href and BOMBO_BASE in href:
                urls.append(href)
        page += 1
    return urls[:max_urls]


def scrape_bombo(
    session:    requests.Session,
    writer_txt,
    writer_tsv,
    log_f,
    seen:       set[str],
    max_urls:   int = 200,
) -> int:
    print(f"  Fetching Bombo Radyo Iloilo article list ...")
    urls  = bombo_article_urls(session, max_urls)
    total = 0
    bar   = tqdm(urls, desc="  Bombo Radyo") if HAS_TQDM else urls
    for url in bar:
        total += scrape_news_article(
            session, url, "bombo_radyo_iloilo", writer_txt, writer_tsv, log_f, seen
        )
    return total


# ══════════════════════════════════════════════════════════════════════════════
# Resume support
# ══════════════════════════════════════════════════════════════════════════════

def load_seen(txt_path: Path) -> set[str]:
    """Re-read existing output to avoid re-adding duplicates on resume."""
    seen = set()
    if txt_path.exists():
        with open(txt_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    seen.add(line)
        print(f"  [resume] {len(seen):,} sentences already collected")
    return seen


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scrape Hiligaynon text from free public sources"
    )
    p.add_argument(
        "--sources", nargs="+",
        choices=["wikipedia", "panaynews", "guardian", "bombo"],
        default=["wikipedia", "panaynews", "guardian", "bombo"],
        help="Which sources to scrape (default: all)",
    )
    p.add_argument(
        "--wiki-limit", type=int, default=2000,
        help="Max Wikipedia articles to fetch (default: 2000)",
    )
    p.add_argument(
        "--news-limit", type=int, default=300,
        help="Max articles per news source (default: 300)",
    )
    p.add_argument(
        "--resume", action="store_true",
        help="Resume from existing output files",
    )
    p.add_argument(
        "--out-dir", type=Path, default=OUT_DIR,
        help=f"Output directory (default: {OUT_DIR})",
    )
    return p.parse_args()


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    args = parse_args()

    raw_txt     = args.out_dir / "hil_raw.txt"
    scraped_tsv = args.out_dir / "hil_scraped.tsv"
    log_file    = args.out_dir / "scrape_log.jsonl"

    args.out_dir.mkdir(parents=True, exist_ok=True)

    seen = load_seen(raw_txt) if args.resume else set()
    mode = "a" if args.resume else "w"

    session = requests.Session()
    session.headers.update(HEADERS)

    totals: dict[str, int] = {}

    with (
        open(raw_txt,     mode, encoding="utf-8") as txt_f,
        open(scraped_tsv, mode, encoding="utf-8") as tsv_f,
        open(log_file,    mode, encoding="utf-8") as log_f,
    ):
        if "wikipedia" in args.sources:
            print("\n[1/4] Hiligaynon Wikipedia ...")
            totals["wikipedia"] = scrape_wikipedia(
                session, txt_f, tsv_f, log_f, seen, args.wiki_limit
            )
            print(f"  -> {totals['wikipedia']:,} sentences")

        if "panaynews" in args.sources:
            print("\n[2/4] Panay News ...")
            totals["panaynews"] = scrape_panaynews(
                session, txt_f, tsv_f, log_f, seen, args.news_limit
            )
            print(f"  -> {totals['panaynews']:,} sentences")

        if "guardian" in args.sources:
            print("\n[3/4] Daily Guardian ...")
            totals["guardian"] = scrape_guardian(
                session, txt_f, tsv_f, log_f, seen, args.news_limit
            )
            print(f"  -> {totals['guardian']:,} sentences")

        if "bombo" in args.sources:
            print("\n[4/4] Bombo Radyo Iloilo ...")
            totals["bombo"] = scrape_bombo(
                session, txt_f, tsv_f, log_f, seen, args.news_limit
            )
            print(f"  -> {totals['bombo']:,} sentences")

    grand_total = sum(totals.values())
    print("\n" + "=" * 50)
    print("  Scrape complete")
    for src, n in totals.items():
        print(f"  {src:<20} {n:>8,} sentences")
    print(f"  {'TOTAL':<20} {grand_total:>8,} sentences")
    print(f"\n  Monolingual : {raw_txt}")
    print(f"  TSV         : {scraped_tsv}")
    print(f"  Log         : {log_file}")
    print()
    print("  Next steps:")
    print("  1. Use hil_raw.txt to retrain the SentencePiece tokenizer:")
    print("       python train_spm.py")
    print("  2. Merge with your existing corpus:")
    print("       python concat_tsv.py data/en-tl.tsv data/hil_scraped.tsv")
    print("  3. Delete stale dataset cache:")
    print("       rm .dataset_cache_*.pkl")
    print("=" * 50)


if __name__ == "__main__":
    main()
