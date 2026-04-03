#!/usr/bin/env python3
"""
Parallel Translation Dataset Analyzer
Analyzes quality of parallel corpora (CSV, TSV, or split .txt corpus files).
"""

import argparse
import csv
import os
import sys
import re
from collections import Counter
from pathlib import Path


# ─── ANSI colors ────────────────────────────────────────────────────────────

def _supports_color():
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

USE_COLOR = _supports_color()

def c(text, code):
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text

def bold(t):    return c(t, "1")
def green(t):   return c(t, "32")
def yellow(t):  return c(t, "33")
def red(t):     return c(t, "31")
def cyan(t):    return c(t, "36")
def dim(t):     return c(t, "2")


# ─── Loading ─────────────────────────────────────────────────────────────────

def load_csv_tsv(path: str, src_col=None, tgt_col=None, delimiter=None):
    """Load a CSV or TSV file. Returns list of (src, tgt) tuples."""
    if delimiter is None:
        delimiter = "\t" if path.endswith(".tsv") else ","

    pairs = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(4096)
        f.seek(0)

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
            reader = csv.reader(f, dialect)
        except csv.Error:
            reader = csv.reader(f, delimiter=delimiter)

        rows = list(reader)
        if not rows:
            sys.exit(red("Error: file is empty."))

        # Detect header
        header = rows[0]
        data_rows = rows[1:] if any(not h.lstrip("-").replace(".", "").isdigit() for h in header) else rows

        if len(header) < 2:
            sys.exit(red(f"Error: need at least 2 columns, found {len(header)}."))

        # Resolve column indices
        if src_col is not None and tgt_col is not None:
            try:
                si = header.index(src_col) if isinstance(src_col, str) else int(src_col)
                ti = header.index(tgt_col) if isinstance(tgt_col, str) else int(tgt_col)
            except (ValueError, IndexError):
                sys.exit(red(f"Error: could not find columns '{src_col}' / '{tgt_col}'."))
        else:
            si, ti = 0, 1
            if data_rows is not rows:  # has header
                # try to auto-detect by header name
                low = [h.lower() for h in header]
                src_hints = ["source", "src", "en", "input"]
                tgt_hints = ["target", "tgt", "translation", "output"]
                for h in src_hints:
                    if h in low: si = low.index(h); break
                for h in tgt_hints:
                    if h in low: ti = low.index(h); break

        for row in data_rows:
            if len(row) <= max(si, ti):
                pairs.append(("", ""))
            else:
                pairs.append((row[si].strip(), row[ti].strip()))

    return pairs, header


def load_txt_pair(src_path: str, tgt_path: str):
    """Load two parallel .txt files. Returns list of (src, tgt) tuples."""
    def read_lines(p):
        with open(p, encoding="utf-8-sig") as f:
            return [line.rstrip("\n") for line in f]

    src_lines = read_lines(src_path)
    tgt_lines = read_lines(tgt_path)

    if len(src_lines) != len(tgt_lines):
        print(yellow(f"  Warning: line count mismatch — source {len(src_lines)}, target {len(tgt_lines)}"))
        min_len = min(len(src_lines), len(tgt_lines))
        print(yellow(f"           Truncating to {min_len} pairs for analysis.\n"))
        src_lines = src_lines[:min_len]
        tgt_lines = tgt_lines[:min_len]

    return list(zip(src_lines, tgt_lines))


# ─── Tokenizer ───────────────────────────────────────────────────────────────

def tokenize(text: str) -> list:
    """Simple whitespace + punctuation tokenizer (no external deps)."""
    return re.findall(r"\b\w+\b", text.lower())


def char_count(text: str) -> int:
    return len(text.strip())


# ─── Analysis ────────────────────────────────────────────────────────────────

def analyze(pairs: list, args) -> dict:
    total = len(pairs)
    if total == 0:
        sys.exit(red("Error: no pairs found."))

    src_empty, tgt_empty, both_empty = 0, 0, 0
    src_only_punc, tgt_only_punc = 0, 0
    duplicate_pairs = Counter()
    src_seen, tgt_seen = Counter(), Counter()
    length_ratio_issues = []   # (index, ratio, src_len, tgt_len)
    short_pairs = []           # pairs with very short sentences
    long_pairs  = []           # pairs with very long sentences
    src_vocab, tgt_vocab = Counter(), Counter()
    src_lengths, tgt_lengths = [], []
    src_char_lens, tgt_char_lens = [], []
    non_printable = 0
    numeric_only  = 0
    html_tag_re   = re.compile(r"<[^>]+>")
    html_mismatch = 0
    punc_re       = re.compile(r"^[\s\W]+$")

    ratio_min = args.ratio_min
    ratio_max = args.ratio_max
    min_tokens = args.min_tokens
    max_tokens = args.max_tokens

    for i, (src, tgt) in enumerate(pairs):
        # --- emptiness ---
        s_empty = not src.strip()
        t_empty = not tgt.strip()
        if s_empty and t_empty: both_empty += 1
        elif s_empty: src_empty += 1
        elif t_empty: tgt_empty += 1

        if s_empty or t_empty:
            src_lengths.append(0); tgt_lengths.append(0)
            src_char_lens.append(0); tgt_char_lens.append(0)
            continue

        # --- non-printable characters ---
        if any(ord(c) < 32 and c not in "\t\n\r" for c in src + tgt):
            non_printable += 1

        # --- numeric-only ---
        src_tok = tokenize(src)
        tgt_tok = tokenize(tgt)
        if src_tok and all(t.isdigit() for t in src_tok) and \
           tgt_tok and all(t.isdigit() for t in tgt_tok):
            numeric_only += 1

        # --- punctuation-only ---
        if punc_re.match(src): src_only_punc += 1
        if punc_re.match(tgt): tgt_only_punc += 1

        # --- lengths ---
        sl, tl = len(src_tok), len(tgt_tok)
        src_lengths.append(sl); tgt_lengths.append(tl)
        src_char_lens.append(char_count(src)); tgt_char_lens.append(char_count(tgt))

        # --- vocab ---
        src_vocab.update(src_tok); tgt_vocab.update(tgt_tok)

        # --- duplicates ---
        key = (src.strip(), tgt.strip())
        duplicate_pairs[key] += 1
        src_seen[src.strip()] += 1
        tgt_seen[tgt.strip()] += 1

        # --- length ratio ---
        if tl > 0:
            ratio = sl / tl
            if ratio < ratio_min or ratio > ratio_max:
                length_ratio_issues.append((i + 1, round(ratio, 3), sl, tl))
        elif sl > 0:
            length_ratio_issues.append((i + 1, float("inf"), sl, 0))

        # --- too short / too long ---
        if 0 < sl < min_tokens or 0 < tl < min_tokens:
            short_pairs.append((i + 1, sl, tl, src[:80], tgt[:80]))
        if sl > max_tokens or tl > max_tokens:
            long_pairs.append((i + 1, sl, tl))

        # --- HTML tag mismatch ---
        src_tags = html_tag_re.findall(src)
        tgt_tags = html_tag_re.findall(tgt)
        if sorted(src_tags) != sorted(tgt_tags):
            html_mismatch += 1

    # duplicates
    exact_dup_pairs  = sum(1 for v in duplicate_pairs.values() if v > 1)
    exact_dup_count  = sum(v - 1 for v in duplicate_pairs.values() if v > 1)
    src_dup_count    = sum(v - 1 for v in src_seen.values() if v > 1)
    tgt_dup_count    = sum(v - 1 for v in tgt_seen.values() if v > 1)

    # stats helper
    def stats(lst):
        lst = [x for x in lst if x > 0]
        if not lst:
            return {"min": 0, "max": 0, "mean": 0, "median": 0}
        lst.sort()
        n = len(lst)
        return {
            "min":    lst[0],
            "max":    lst[-1],
            "mean":   round(sum(lst) / n, 2),
            "median": lst[n // 2],
        }

    return {
        "total":            total,
        "src_empty":        src_empty,
        "tgt_empty":        tgt_empty,
        "both_empty":       both_empty,
        "src_only_punc":    src_only_punc,
        "tgt_only_punc":    tgt_only_punc,
        "non_printable":    non_printable,
        "numeric_only":     numeric_only,
        "html_mismatch":    html_mismatch,
        "exact_dup_pairs":  exact_dup_pairs,
        "exact_dup_count":  exact_dup_count,
        "src_dup_count":    src_dup_count,
        "tgt_dup_count":    tgt_dup_count,
        "ratio_issues":     length_ratio_issues,
        "short_pairs":      short_pairs,
        "long_pairs":       long_pairs,
        "src_vocab_size":   len(src_vocab),
        "tgt_vocab_size":   len(tgt_vocab),
        "src_token_total":  sum(src_lengths),
        "tgt_token_total":  sum(tgt_lengths),
        "src_len_stats":    stats(src_lengths),
        "tgt_len_stats":    stats(tgt_lengths),
        "src_char_stats":   stats(src_char_lens),
        "tgt_char_stats":   stats(tgt_char_lens),
        "src_top_tokens":   src_vocab.most_common(10),
        "tgt_top_tokens":   tgt_vocab.most_common(10),
        "src_lengths":      src_lengths,
        "tgt_lengths":      tgt_lengths,
    }


# ─── Histogram ───────────────────────────────────────────────────────────────

def ascii_histogram(lengths, label, buckets=10, width=35):
    if not lengths:
        return
    mn, mx = min(lengths), max(lengths)
    if mn == mx:
        print(f"  All sentences: {mn} tokens")
        return
    step = max(1, (mx - mn) // buckets)
    counts = Counter()
    for v in lengths:
        b = ((v - mn) // step) * step + mn
        counts[b] += 1
    max_count = max(counts.values())
    print(f"\n  {bold(label)} token length distribution:")
    for b in sorted(counts):
        bar_len = int(counts[b] / max_count * width)
        bar = "█" * bar_len
        end = b + step - 1
        print(f"  {b:>5}–{end:<5}  {cyan(bar):<{width+10}} {counts[b]}")


# ─── Recommendations ─────────────────────────────────────────────────────────

def print_recommendations(res: dict, args):
    total = res["total"]

    recs = []  # list of (severity, title, where, why, how)

    # ── Too long ──
    n_long = len(res["long_pairs"])
    if n_long > 0:
        pct = n_long / total * 100
        sev = "HIGH" if pct > 5 else "MED"
        lines = ", ".join(str(i) for i, *_ in res["long_pairs"][:5])
        lines_str = f"lines {lines}{'…' if n_long > 5 else ''} ({n_long:,} total)"
        recs.append((sev,
            f"Remove or split {n_long:,} over-long pairs (>{args.max_tokens} tokens)",
            lines_str,
            "Very long pairs are usually merged paragraphs. They make models learn "
            "document-level patterns instead of sentence translation, hurting quality.",
            f"""\
  Option A — Drop them entirely (recommended for MT training):
    import pandas as pd
    df = pd.read_csv('your_file.csv')
    df['src_len'] = df.iloc[:,0].str.split().str.len()
    df['tgt_len'] = df.iloc[:,1].str.split().str.len()
    df = df[(df['src_len'] <= {args.max_tokens}) & (df['tgt_len'] <= {args.max_tokens})]
    df.drop(columns=['src_len','tgt_len']).to_csv('cleaned.csv', index=False)

  Option B — Split on sentence boundaries (keeps more data):
    pip install nltk
    import nltk; nltk.download('punkt')
    # Then split each pair on '. ' and re-align — only safe for well-punctuated text."""
        ))

    # ── Too short ──
    n_short = len(res["short_pairs"])
    if n_short > 0:
        pct = n_short / total * 100
        sev = "HIGH" if pct > 2 else "MED"
        lines = ", ".join(str(i) for i, *_ in res["short_pairs"][:5])
        lines_str = f"lines {lines}{'…' if n_short > 5 else ''} ({n_short:,} total)"
        recs.append((sev,
            f"Remove {n_short:,} too-short pairs (<{args.min_tokens} tokens)",
            lines_str,
            "Single-word or single-symbol pairs (e.g. 'Choice | Pagpipilian', '? | ?') "
            "teach nothing about translation structure and add noise.",
            f"""\
  import pandas as pd
  df = pd.read_csv('your_file.csv')
  src_col, tgt_col = df.columns[0], df.columns[1]
  mask = (df[src_col].str.split().str.len() >= {args.min_tokens}) & \\
         (df[tgt_col].str.split().str.len() >= {args.min_tokens})
  df[mask].to_csv('cleaned.csv', index=False)
  print(f'Kept {{mask.sum():,}} / {{len(df):,}} pairs')"""
        ))

    # ── Length ratio ──
    n_ratio = len(res["ratio_issues"])
    if n_ratio > 0:
        pct = n_ratio / total * 100
        sev = "HIGH" if pct > 2 else "MED"
        lines = ", ".join(str(i) for i, *_ in res["ratio_issues"][:5])
        lines_str = f"lines {lines}{'…' if n_ratio > 5 else ''} ({n_ratio:,} total)"
        recs.append((sev,
            f"Investigate {n_ratio:,} length-ratio outliers (src/tgt outside "
            f"[{args.ratio_min}–{args.ratio_max}])",
            lines_str,
            "A ratio of 0.05 means the source is 20× shorter than the target — "
            "almost certainly a misaligned or corrupted pair. These pairs teach "
            "the model wrong alignments.",
            f"""\
  import pandas as pd, re
  df = pd.read_csv('your_file.csv')
  src_col, tgt_col = df.columns[0], df.columns[1]
  tokenize = lambda s: re.findall(r'\\b\\w+\\b', str(s))
  df['ratio'] = df[src_col].apply(lambda s: len(tokenize(s))) / \\
                df[tgt_col].apply(lambda s: max(len(tokenize(s)), 1))
  bad = df[(df['ratio'] < {args.ratio_min}) | (df['ratio'] > {args.ratio_max})]
  bad.to_csv('ratio_outliers.csv', index=False)   # inspect these
  good = df[(df['ratio'] >= {args.ratio_min}) & (df['ratio'] <= {args.ratio_max})]
  good.drop(columns='ratio').to_csv('cleaned.csv', index=False)"""
        ))

    # ── Duplicates ──
    if res["exact_dup_count"] > 0:
        sev = "MED"
        recs.append((sev,
            f"Deduplicate {res['exact_dup_count']:,} repeated pairs",
            "spread across dataset (second+ occurrences)",
            "Duplicate pairs overrepresent certain phrases, causing the model to "
            "overfit on them. Especially harmful in small datasets.",
            """\
  import pandas as pd
  df = pd.read_csv('your_file.csv')
  before = len(df)
  df = df.drop_duplicates()
  print(f'Removed {before - len(df):,} duplicates')
  df.to_csv('cleaned.csv', index=False)"""
        ))

    if res["src_dup_count"] > res["exact_dup_count"]:
        extra = res["src_dup_count"] - res["exact_dup_count"]
        recs.append(("LOW",
            f"Review {extra:,} pairs with duplicate source but different target",
            "source column",
            "Same source sentence mapping to different translations — could be "
            "legitimate variation or a data collection error worth checking.",
            """\
  import pandas as pd
  df = pd.read_csv('your_file.csv')
  src_col = df.columns[0]
  dups = df[df.duplicated(subset=[src_col], keep=False)]
  dups.sort_values(src_col).to_csv('src_duplicates.csv', index=False)
  # Review this file — keep intentional variations, remove errors"""
        ))

    # ── HTML tags ──
    if res["html_mismatch"] > 0:
        pct = res["html_mismatch"] / total * 100
        sev = "MED" if pct > 1 else "LOW"
        recs.append((sev,
            f"Fix or strip HTML tag mismatches in {res['html_mismatch']:,} pairs",
            "both columns (tags present on one side only)",
            "Mismatched tags like <b> or <br> on one side only confuse subword "
            "tokenizers and may appear verbatim in model output.",
            """\
  import pandas as pd, re
  df = pd.read_csv('your_file.csv')
  strip_tags = lambda s: re.sub(r'<[^>]+>', '', str(s)).strip()
  df.iloc[:,0] = df.iloc[:,0].apply(strip_tags)
  df.iloc[:,1] = df.iloc[:,1].apply(strip_tags)
  df.to_csv('cleaned.csv', index=False)"""
        ))

    # ── Punctuation / numeric only ──
    n_punc = max(res["src_only_punc"], res["tgt_only_punc"])
    n_num  = res["numeric_only"]
    if n_punc + n_num > 0:
        recs.append(("LOW",
            f"Remove {n_punc + n_num:,} punctuation-only / numeric-only pairs",
            "mixed across dataset",
            "Pairs like '... | ...' or '2024 | 2024' carry no translatable content "
            "and add noise to vocabulary statistics.",
            """\
  import pandas as pd, re
  df = pd.read_csv('your_file.csv')
  src_col, tgt_col = df.columns[0], df.columns[1]
  has_word = lambda s: bool(re.search(r'[a-zA-Z\\u00C0-\\uFFFF]', str(s)))
  mask = df[src_col].apply(has_word) & df[tgt_col].apply(has_word)
  print(f'Removing {(~mask).sum():,} non-text pairs')
  df[mask].to_csv('cleaned.csv', index=False)"""
        ))

    # ── Mean/median skew ──
    sl = res["src_len_stats"]
    if sl["mean"] > sl["median"] * 3:
        recs.append(("MED",
            "Address heavy length skew (mean >> median)",
            "length distribution — check histograms above",
            f"Mean ({sl['mean']} tokens) is {round(sl['mean']/max(sl['median'],1), 1)}× "
            f"the median ({sl['median']} tokens). This usually means a small number of "
            "very long pairs are distorting averages — often whole documents or "
            "multi-sentence blocks mixed in.",
            f"""\
  # First check the worst offenders:
  import pandas as pd
  df = pd.read_csv('your_file.csv')
  df['src_len'] = df.iloc[:,0].str.split().str.len()
  df.nlargest(20, 'src_len')[['src_len', df.columns[0], df.columns[1]]] \\
    .to_csv('longest_pairs.csv', index=False)
  # Then decide threshold — your median is {sl['median']}, a safe max might be
  # 3–5× that = {sl['median']*3}–{sl['median']*5} tokens"""
        ))

    # ── All clear ──
    if not recs:
        print(f"\n{bold('Recommendations')}")
        print(f"  {green('✓')}  No significant issues found. Dataset looks clean.")
        return

    # ── Print recs ──
    sev_color = {"HIGH": red, "MED": yellow, "LOW": cyan}
    sev_order = {"HIGH": 0, "MED": 1, "LOW": 2}
    recs.sort(key=lambda r: sev_order[r[0]])

    print(f"\n{bold('Recommendations')}  ({len(recs)} issue{'s' if len(recs)!=1 else ''})\n")

    for i, (sev, title, where, why, how) in enumerate(recs, 1):
        sc = sev_color.get(sev, dim)
        print(f"  {bold(str(i))}. [{sc(sev)}] {bold(title)}")
        print(f"     {yellow('Where')} : {where}")
        print(f"     {yellow('Why')}   : {why}")
        print(f"     {yellow('How')}   :")
        for line in how.splitlines():
            print(f"     {dim(line)}")
        print()


# ─── Report ──────────────────────────────────────────────────────────────────

def fmt_issue(n, total, label, threshold_pct=1.0):
    pct = n / total * 100
    flag = red("✗") if pct > threshold_pct else (yellow("!") if n > 0 else green("✓"))
    return f"  {flag}  {label:<40}  {n:>7,}  ({pct:.2f}%)"

def print_report(res: dict, args):
    total = res["total"]
    print()
    print(bold("═" * 60))
    print(bold("  PARALLEL DATASET QUALITY REPORT"))
    print(bold("═" * 60))

    # ── Overview ──
    print(f"\n{bold('Overview')}")
    print(f"  Total pairs          : {total:,}")
    print(f"  Source vocab size    : {res['src_vocab_size']:,} unique tokens")
    print(f"  Target vocab size    : {res['tgt_vocab_size']:,} unique tokens")
    print(f"  Source total tokens  : {res['src_token_total']:,}")
    print(f"  Target total tokens  : {res['tgt_token_total']:,}")

    sl, tl = res["src_len_stats"], res["tgt_len_stats"]
    print(f"\n{bold('Sentence length (tokens)')}")
    print(f"  {'':20} {'Source':>10}  {'Target':>10}")
    print(f"  {'Min':20} {sl['min']:>10}  {tl['min']:>10}")
    print(f"  {'Max':20} {sl['max']:>10}  {tl['max']:>10}")
    print(f"  {'Mean':20} {sl['mean']:>10}  {tl['mean']:>10}")
    print(f"  {'Median':20} {sl['median']:>10}  {tl['median']:>10}")

    # ── Issues ──
    print(f"\n{bold('Quality issues')}")
    print(f"  {'':2}  {'Issue':<40}  {'Count':>7}  {'%'}")
    print(f"  {'─' * 58}")

    print(fmt_issue(res["src_empty"],       total, "Source-side empty"))
    print(fmt_issue(res["tgt_empty"],       total, "Target-side empty"))
    print(fmt_issue(res["both_empty"],      total, "Both sides empty"))
    print(fmt_issue(res["src_only_punc"],   total, "Source punctuation-only"))
    print(fmt_issue(res["tgt_only_punc"],   total, "Target punctuation-only"))
    print(fmt_issue(res["numeric_only"],    total, "Numeric-only pairs"))
    print(fmt_issue(res["non_printable"],   total, "Contains non-printable chars"))
    print(fmt_issue(res["html_mismatch"],   total, "HTML tag mismatch"))
    print(fmt_issue(len(res["ratio_issues"]), total,
                    f"Length ratio outside [{args.ratio_min}–{args.ratio_max}]"))
    print(fmt_issue(len(res["short_pairs"]), total,
                    f"Too short (< {args.min_tokens} tokens)"))
    print(fmt_issue(len(res["long_pairs"]),  total,
                    f"Too long  (> {args.max_tokens} tokens)"))

    print(f"\n{bold('Duplicates')}")
    print(fmt_issue(res["exact_dup_count"],  total, "Exact duplicate pairs"))
    print(fmt_issue(res["src_dup_count"],    total, "Duplicate source sentences"))
    print(fmt_issue(res["tgt_dup_count"],    total, "Duplicate target sentences"))

    # ── Overall health score ──
    weights = {
        "src_empty":        (res["src_empty"],          5),
        "tgt_empty":        (res["tgt_empty"],          5),
        "both_empty":       (res["both_empty"],         5),
        "ratio_issues":     (len(res["ratio_issues"]),  2),
        "exact_dup_count":  (res["exact_dup_count"],    2),
        "short_pairs":      (len(res["short_pairs"]),   1),
    }
    penalty = sum(
        min((count / total) * weight * 100, weight * 10)
        for count, weight in weights.values()
    )
    score = max(0, round(100 - penalty))
    color_fn = green if score >= 80 else (yellow if score >= 50 else red)
    print(f"\n{bold('Health score')}")
    bar_len = score // 2
    bar = "█" * bar_len + "░" * (50 - bar_len)
    print(f"  {color_fn(bar)}  {color_fn(bold(str(score) + '/100'))}")

    # ── Recommendations ──
    print_recommendations(res, args)

    # ── Samples of flagged pairs ──
    if args.samples and res["ratio_issues"]:
        n = min(args.samples, len(res["ratio_issues"]))
        print(f"\n{bold(f'Length ratio outliers — first {n} samples')}")
        for idx, ratio, sl, tl in res["ratio_issues"][:n]:
            print(f"  Line {idx:>6}: ratio={ratio:<6}  src={sl} tok  tgt={tl} tok")

    if args.samples and res["short_pairs"]:
        n = min(args.samples, len(res["short_pairs"]))
        print(f"\n{bold(f'Short pairs — first {n} samples')}")
        for idx, sl, tl, src, tgt in res["short_pairs"][:n]:
            print(f"  Line {idx:>6}: [{sl} | {tl}] {dim(src[:60])} | {dim(tgt[:60])}")

    # ── Histograms ──
    if not args.no_histogram:
        ascii_histogram(res["src_lengths"], "Source")
        ascii_histogram(res["tgt_lengths"], "Target")

    # ── Top tokens ──
    if args.top_tokens:
        print(f"\n{bold('Top 10 source tokens')}")
        for tok, freq in res["src_top_tokens"]:
            print(f"  {tok:<20} {freq:,}")
        print(f"\n{bold('Top 10 target tokens')}")
        for tok, freq in res["tgt_top_tokens"]:
            print(f"  {tok:<20} {freq:,}")

    print()
    print(bold("═" * 60))
    print()


# ─── Export ──────────────────────────────────────────────────────────────────

def export_flagged(pairs, res, out_path):
    """Write a TSV of all flagged pair indices with issue types."""
    flagged = {}
    def add(idx, reason):
        flagged.setdefault(idx - 1, set()).add(reason)

    # empty
    for i, (s, t) in enumerate(pairs):
        if not s.strip(): add(i + 1, "src_empty")
        if not t.strip(): add(i + 1, "tgt_empty")

    for idx, *_ in res["ratio_issues"]:
        add(idx, "ratio")
    for idx, *_ in res["short_pairs"]:
        add(idx, "short")
    for idx, *_ in res["long_pairs"]:
        add(idx, "long")

    # duplicates — flag second+ occurrence
    seen = {}
    for i, (s, t) in enumerate(pairs):
        key = (s.strip(), t.strip())
        if key in seen:
            add(i + 1, "duplicate")
        else:
            seen[key] = i

    if not flagged:
        print(yellow("  No issues found — nothing to export."))
        return

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["line", "source", "target", "issues"])
        for idx in sorted(flagged):
            s, t = pairs[idx]
            writer.writerow([idx + 1, s, t, "|".join(sorted(flagged[idx]))])

    print(green(f"  Flagged pairs exported → {out_path}  ({len(flagged):,} rows)"))


# ─── CLI ─────────────────────────────────────────────────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        prog="parallel_analyzer",
        description="Analyze quality of parallel translation datasets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # CSV file (auto-detects first two columns)
  python parallel_analyzer.py data.csv

  # TSV with named columns
  python parallel_analyzer.py data.tsv --src-col en --tgt-col tl

  # Two separate corpus files
  python parallel_analyzer.py --src corpus_en.txt --tgt corpus_tg.txt

  # Full analysis with sample output and flagged export
  python parallel_analyzer.py data.csv --samples 10 --top-tokens --export flagged.tsv
""",
    )

    # Input
    inp = p.add_argument_group("Input (pick one)")
    inp.add_argument("file", nargs="?", metavar="FILE",
                     help="CSV or TSV file path")
    inp.add_argument("--src", metavar="FILE",
                     help="Source corpus .txt file")
    inp.add_argument("--tgt", metavar="FILE",
                     help="Target corpus .txt file")
    inp.add_argument("--src-col", metavar="COL",
                     help="Source column name or index (for CSV/TSV, default: 0)")
    inp.add_argument("--tgt-col", metavar="COL",
                     help="Target column name or index (for CSV/TSV, default: 1)")

    # Thresholds
    thr = p.add_argument_group("Thresholds")
    thr.add_argument("--ratio-min", type=float, default=0.2, metavar="N",
                     help="Min src/tgt token length ratio (default: 0.2)")
    thr.add_argument("--ratio-max", type=float, default=5.0, metavar="N",
                     help="Max src/tgt token length ratio (default: 5.0)")
    thr.add_argument("--min-tokens", type=int, default=3, metavar="N",
                     help="Flag pairs shorter than N tokens (default: 3)")
    thr.add_argument("--max-tokens", type=int, default=200, metavar="N",
                     help="Flag pairs longer than N tokens (default: 200)")

    # Output
    out = p.add_argument_group("Output")
    out.add_argument("--samples", type=int, default=5, metavar="N",
                     help="Show N sample flagged pairs per issue (default: 5)")
    out.add_argument("--no-histogram", action="store_true",
                     help="Skip ASCII length histograms")
    out.add_argument("--top-tokens", action="store_true",
                     help="Show top 10 tokens per side")
    out.add_argument("--export", metavar="FILE",
                     help="Export flagged pairs to a TSV file")
    out.add_argument("--no-color", action="store_true",
                     help="Disable ANSI colors")

    return p


def main():
    global USE_COLOR
    parser = build_parser()
    args = parser.parse_args()

    if args.no_color:
        USE_COLOR = False

    # ── Load data ──
    if args.file:
        path = args.file
        if not os.path.isfile(path):
            sys.exit(red(f"Error: file not found: {path}"))
        ext = Path(path).suffix.lower()
        if ext not in (".csv", ".tsv", ".txt"):
            print(yellow(f"Warning: unrecognized extension '{ext}', treating as CSV."))
        if ext == ".txt":
            sys.exit(red(
                "Error: for a single .txt file use --src / --tgt.\n"
                "  Example: --src corpus_en.txt --tgt corpus_tg.txt"
            ))
        print(f"\n{bold('Loading')} {cyan(path)} …")
        pairs, header = load_csv_tsv(path, args.src_col, args.tgt_col)
        print(f"  Detected {len(pairs):,} pairs  |  header: {header[:4]}")

    elif args.src and args.tgt:
        for p in (args.src, args.tgt):
            if not os.path.isfile(p):
                sys.exit(red(f"Error: file not found: {p}"))
        print(f"\n{bold('Loading')} {cyan(args.src)} + {cyan(args.tgt)} …")
        pairs = load_txt_pair(args.src, args.tgt)
        print(f"  Loaded {len(pairs):,} pairs")

    else:
        parser.print_help()
        print(red("\nError: provide a CSV/TSV file, or --src and --tgt corpus files."))
        sys.exit(1)

    # ── Analyze ──
    print(f"  Analyzing …\n")
    results = analyze(pairs, args)

    # ── Report ──
    print_report(results, args)

    # ── Export ──
    if args.export:
        export_flagged(pairs, results, args.export)
        print()


if __name__ == "__main__":
    main()