#!/usr/bin/env python3
"""
Parallel Translation Dataset Analyzer
Analyzes quality of parallel corpora (CSV, TSV, or split .txt corpus files).
Also supports monolingual analysis when only --src is provided.
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

        header = rows[0]
        data_rows = rows[1:] if any(
            not h.lstrip("-").replace(".", "").isdigit() for h in header
        ) else rows

        if len(header) < 2:
            sys.exit(red(f"Error: need at least 2 columns, found {len(header)}."))

        if src_col is not None and tgt_col is not None:
            try:
                si = header.index(src_col) if isinstance(src_col, str) else int(src_col)
                ti = header.index(tgt_col) if isinstance(tgt_col, str) else int(tgt_col)
            except (ValueError, IndexError):
                sys.exit(red(f"Error: could not find columns '{src_col}' / '{tgt_col}'."))
        else:
            si, ti = 0, 1
            if data_rows is not rows:
                low = [h.lower() for h in header]
                for h in ["source", "src", "en", "input"]:
                    if h in low: si = low.index(h); break
                for h in ["target", "tgt", "translation", "output"]:
                    if h in low: ti = low.index(h); break

        for row in data_rows:
            if len(row) <= max(si, ti):
                pairs.append(("", ""))
            else:
                pairs.append((row[si].strip(), row[ti].strip()))

    return pairs, header


def load_txt_pair(src_path: str, tgt_path: str):
    """Load two parallel .txt files."""
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


def load_txt_mono(src_path: str) -> list[str]:
    """Load a single .txt file for monolingual analysis."""
    with open(src_path, encoding="utf-8-sig") as f:
        lines = [line.rstrip("\n") for line in f]
    return lines


# ─── Tokenizer ───────────────────────────────────────────────────────────────

def tokenize(text: str) -> list:
    return re.findall(r"\b\w+\b", text.lower())


def char_count(text: str) -> int:
    return len(text.strip())


# ─── Monolingual analysis ─────────────────────────────────────────────────────

def analyze_mono(lines: list[str], args) -> dict:
    """
    Analyze a single .txt file — no pair-level checks.
    Reports length stats, vocab, duplicates, short/long lines.
    """
    total = len(lines)
    if total == 0:
        sys.exit(red("Error: file is empty."))

    empty        = 0
    only_punc    = 0
    numeric_only = 0
    non_printable = 0
    short_lines  = []
    long_lines   = []
    lengths      = []
    char_lens    = []
    vocab        = Counter()
    seen         = Counter()
    punc_re      = re.compile(r"^[\s\W]+$")

    for i, line in enumerate(lines):
        stripped = line.strip()

        if not stripped:
            empty += 1
            lengths.append(0)
            char_lens.append(0)
            continue

        if any(ord(ch) < 32 and ch not in "\t\n\r" for ch in stripped):
            non_printable += 1

        toks = tokenize(stripped)
        if punc_re.match(stripped):
            only_punc += 1
        if toks and all(t.isdigit() for t in toks):
            numeric_only += 1

        n = len(toks)
        lengths.append(n)
        char_lens.append(char_count(stripped))
        vocab.update(toks)
        seen[stripped] += 1

        if 0 < n < args.min_tokens:
            short_lines.append((i + 1, n, stripped[:80]))
        if n > args.max_tokens:
            long_lines.append((i + 1, n))

    dup_count = sum(v - 1 for v in seen.values() if v > 1)
    dup_types = sum(1 for v in seen.values() if v > 1)

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
        "mode":         "mono",
        "total":        total,
        "empty":        empty,
        "only_punc":    only_punc,
        "numeric_only": numeric_only,
        "non_printable": non_printable,
        "short_lines":  short_lines,
        "long_lines":   long_lines,
        "dup_count":    dup_count,
        "dup_types":    dup_types,
        "vocab_size":   len(vocab),
        "token_total":  sum(lengths),
        "len_stats":    stats(lengths),
        "char_stats":   stats(char_lens),
        "top_tokens":   vocab.most_common(10),
        "lengths":      lengths,
    }


def print_report_mono(res: dict, args):
    total = res["total"]
    print()
    print(bold("═" * 60))
    print(bold("  MONOLINGUAL CORPUS QUALITY REPORT"))
    print(bold("═" * 60))

    print(f"\n{bold('Overview')}")
    print(f"  Total lines          : {total:,}")
    print(f"  Vocab size           : {res['vocab_size']:,} unique tokens")
    print(f"  Total tokens         : {res['token_total']:,}")

    ls = res["len_stats"]
    print(f"\n{bold('Line length (tokens)')}")
    print(f"  Min     : {ls['min']}")
    print(f"  Max     : {ls['max']}")
    print(f"  Mean    : {ls['mean']}")
    print(f"  Median  : {ls['median']}")

    def fmt(n, label, threshold_pct=1.0):
        pct = n / total * 100 if total else 0
        flag = red("✗") if pct > threshold_pct else (yellow("!") if n > 0 else green("✓"))
        return f"  {flag}  {label:<40}  {n:>7,}  ({pct:.2f}%)"

    print(f"\n{bold('Quality issues')}")
    print(f"  {'':2}  {'Issue':<40}  {'Count':>7}  {'%'}")
    print(f"  {'─' * 58}")
    print(fmt(res["empty"],          "Empty lines"))
    print(fmt(res["only_punc"],      "Punctuation-only lines"))
    print(fmt(res["numeric_only"],   "Numeric-only lines"))
    print(fmt(res["non_printable"],  "Contains non-printable chars"))
    print(fmt(len(res["short_lines"]), f"Too short (< {args.min_tokens} tokens)"))
    print(fmt(len(res["long_lines"]),  f"Too long  (> {args.max_tokens} tokens)"))

    print(f"\n{bold('Duplicates')}")
    print(fmt(res["dup_count"], "Duplicate lines (extra occurrences)"))
    print(f"  {'':3} {res['dup_types']:,} unique lines appear more than once")

    # health score
    weights = {
        "empty":       (res["empty"],               5),
        "dup_count":   (res["dup_count"],            2),
        "short_lines": (len(res["short_lines"]),     1),
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

    # recommendations
    recs = []
    if res["dup_count"] > 0:
        recs.append((
            "MED", f"Deduplicate {res['dup_count']:,} repeated lines",
            """\
  sort -u hil_raw.txt > hil_deduped.txt
  # or in Python:
  lines = open('hil_raw.txt').read().splitlines()
  open('hil_deduped.txt','w').write('\\n'.join(dict.fromkeys(lines)))"""
        ))
    if len(res["short_lines"]) > 0:
        recs.append((
            "MED", f"Remove {len(res['short_lines']):,} too-short lines (< {args.min_tokens} tokens)",
            f"""\
  lines = open('hil_raw.txt').read().splitlines()
  import re
  kept = [l for l in lines if len(re.findall(r'\\b\\w+\\b', l)) >= {args.min_tokens}]
  open('hil_filtered.txt','w').write('\\n'.join(kept))"""
        ))
    if len(res["long_lines"]) > 0:
        recs.append((
            "LOW", f"Split or remove {len(res['long_lines']):,} over-long lines (> {args.max_tokens} tokens)",
            """\
  # Split on sentence boundaries:
  import re
  lines = open('hil_raw.txt').read().splitlines()
  out = []
  for line in lines:
      parts = re.split(r'(?<=[.!?])\\s+', line)
      out.extend(parts)
  open('hil_split.txt','w').write('\\n'.join(out))"""
        ))

    if recs:
        sev_color = {"HIGH": red, "MED": yellow, "LOW": cyan}
        print(f"\n{bold('Recommendations')}  ({len(recs)} issue{'s' if len(recs)!=1 else ''})\n")
        for i, (sev, title, how) in enumerate(recs, 1):
            sc = sev_color.get(sev, dim)
            print(f"  {bold(str(i))}. [{sc(sev)}] {bold(title)}")
            print(f"     {yellow('How')}:")
            for line in how.splitlines():
                print(f"     {dim(line)}")
            print()
    else:
        print(f"\n{bold('Recommendations')}")
        print(f"  {green('✓')}  No significant issues found.")

    # samples
    if args.samples and res["short_lines"]:
        n = min(args.samples, len(res["short_lines"]))
        print(f"\n{bold(f'Short lines — first {n} samples')}")
        for idx, toks, text in res["short_lines"][:n]:
            print(f"  Line {idx:>6}: [{toks} tok]  {dim(text)}")

    if args.samples and res["long_lines"]:
        n = min(args.samples, len(res["long_lines"]))
        print(f"\n{bold(f'Long lines — first {n} samples')}")
        for idx, toks in res["long_lines"][:n]:
            print(f"  Line {idx:>6}: [{toks} tok]")

    if not args.no_histogram:
        ascii_histogram(res["lengths"], "Line")

    if args.top_tokens:
        print(f"\n{bold('Top 10 tokens')}")
        for tok, freq in res["top_tokens"]:
            print(f"  {tok:<20} {freq:,}")

    print()
    print(bold("═" * 60))
    print()


# ─── Parallel analysis (unchanged) ───────────────────────────────────────────

def analyze(pairs: list, args) -> dict:
    total = len(pairs)
    if total == 0:
        sys.exit(red("Error: no pairs found."))

    src_empty, tgt_empty, both_empty = 0, 0, 0
    src_only_punc, tgt_only_punc = 0, 0
    duplicate_pairs = Counter()
    src_seen, tgt_seen = Counter(), Counter()
    length_ratio_issues = []
    short_pairs = []
    long_pairs  = []
    src_vocab, tgt_vocab = Counter(), Counter()
    src_lengths, tgt_lengths = [], []
    src_char_lens, tgt_char_lens = [], []
    non_printable = 0
    numeric_only  = 0
    html_tag_re   = re.compile(r"<[^>]+>")
    html_mismatch = 0
    punc_re       = re.compile(r"^[\s\W]+$")

    ratio_min  = args.ratio_min
    ratio_max  = args.ratio_max
    min_tokens = args.min_tokens
    max_tokens = args.max_tokens

    for i, (src, tgt) in enumerate(pairs):
        s_empty = not src.strip()
        t_empty = not tgt.strip()
        if s_empty and t_empty: both_empty += 1
        elif s_empty: src_empty += 1
        elif t_empty: tgt_empty += 1

        if s_empty or t_empty:
            src_lengths.append(0); tgt_lengths.append(0)
            src_char_lens.append(0); tgt_char_lens.append(0)
            continue

        if any(ord(c) < 32 and c not in "\t\n\r" for c in src + tgt):
            non_printable += 1

        src_tok = tokenize(src)
        tgt_tok = tokenize(tgt)
        if src_tok and all(t.isdigit() for t in src_tok) and \
           tgt_tok and all(t.isdigit() for t in tgt_tok):
            numeric_only += 1

        if punc_re.match(src): src_only_punc += 1
        if punc_re.match(tgt): tgt_only_punc += 1

        sl, tl = len(src_tok), len(tgt_tok)
        src_lengths.append(sl); tgt_lengths.append(tl)
        src_char_lens.append(char_count(src)); tgt_char_lens.append(char_count(tgt))

        src_vocab.update(src_tok); tgt_vocab.update(tgt_tok)

        key = (src.strip(), tgt.strip())
        duplicate_pairs[key] += 1
        src_seen[src.strip()] += 1
        tgt_seen[tgt.strip()] += 1

        if tl > 0:
            ratio = sl / tl
            if ratio < ratio_min or ratio > ratio_max:
                length_ratio_issues.append((i + 1, round(ratio, 3), sl, tl))
        elif sl > 0:
            length_ratio_issues.append((i + 1, float("inf"), sl, 0))

        if 0 < sl < min_tokens or 0 < tl < min_tokens:
            short_pairs.append((i + 1, sl, tl, src[:80], tgt[:80]))
        if sl > max_tokens or tl > max_tokens:
            long_pairs.append((i + 1, sl, tl))

        src_tags = html_tag_re.findall(src)
        tgt_tags = html_tag_re.findall(tgt)
        if sorted(src_tags) != sorted(tgt_tags):
            html_mismatch += 1

    exact_dup_pairs = sum(1 for v in duplicate_pairs.values() if v > 1)
    exact_dup_count = sum(v - 1 for v in duplicate_pairs.values() if v > 1)
    src_dup_count   = sum(v - 1 for v in src_seen.values() if v > 1)
    tgt_dup_count   = sum(v - 1 for v in tgt_seen.values() if v > 1)

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
        "mode":             "parallel",
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
        print(f"  All lines: {mn} tokens")
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


# ─── Parallel report + recommendations (unchanged from original) ──────────────

def print_recommendations(res: dict, args):
    total = res["total"]
    recs  = []

    n_long = len(res["long_pairs"])
    if n_long > 0:
        pct = n_long / total * 100
        sev = "HIGH" if pct > 5 else "MED"
        lines = ", ".join(str(i) for i, *_ in res["long_pairs"][:5])
        lines_str = f"lines {lines}{'…' if n_long > 5 else ''} ({n_long:,} total)"
        recs.append((sev,
            f"Remove or split {n_long:,} over-long pairs (>{args.max_tokens} tokens)",
            lines_str,
            "Very long pairs are usually merged paragraphs.",
            f"""\
  import pandas as pd
  df = pd.read_csv('your_file.tsv', sep='\\t')
  df['src_len'] = df.iloc[:,0].str.split().str.len()
  df['tgt_len'] = df.iloc[:,1].str.split().str.len()
  df = df[(df['src_len'] <= {args.max_tokens}) & (df['tgt_len'] <= {args.max_tokens})]
  df.drop(columns=['src_len','tgt_len']).to_csv('cleaned.tsv', sep='\\t', index=False)"""
        ))

    n_short = len(res["short_pairs"])
    if n_short > 0:
        pct = n_short / total * 100
        sev = "HIGH" if pct > 2 else "MED"
        lines = ", ".join(str(i) for i, *_ in res["short_pairs"][:5])
        lines_str = f"lines {lines}{'…' if n_short > 5 else ''} ({n_short:,} total)"
        recs.append((sev,
            f"Remove {n_short:,} too-short pairs (<{args.min_tokens} tokens)",
            lines_str,
            "Single-word pairs add noise and teach nothing about translation structure.",
            f"""\
  import pandas as pd, re
  df = pd.read_csv('your_file.tsv', sep='\\t')
  src_col, tgt_col = df.columns[0], df.columns[1]
  tok = lambda s: len(re.findall(r'\\b\\w+\\b', str(s)))
  mask = (df[src_col].apply(tok) >= {args.min_tokens}) & (df[tgt_col].apply(tok) >= {args.min_tokens})
  df[mask].to_csv('cleaned.tsv', sep='\\t', index=False)"""
        ))

    n_ratio = len(res["ratio_issues"])
    if n_ratio > 0:
        pct = n_ratio / total * 100
        sev = "HIGH" if pct > 2 else "MED"
        lines = ", ".join(str(i) for i, *_ in res["ratio_issues"][:5])
        lines_str = f"lines {lines}{'…' if n_ratio > 5 else ''} ({n_ratio:,} total)"
        recs.append((sev,
            f"Investigate {n_ratio:,} length-ratio outliers ([{args.ratio_min}–{args.ratio_max}])",
            lines_str,
            "Extreme ratio usually means a misaligned or corrupted pair.",
            f"""\
  import pandas as pd, re
  df = pd.read_csv('your_file.tsv', sep='\\t')
  tok = lambda s: len(re.findall(r'\\b\\w+\\b', str(s)))
  df['ratio'] = df.iloc[:,0].apply(tok) / df.iloc[:,1].apply(lambda s: max(tok(s), 1))
  good = df[(df['ratio'] >= {args.ratio_min}) & (df['ratio'] <= {args.ratio_max})]
  good.drop(columns='ratio').to_csv('cleaned.tsv', sep='\\t', index=False)"""
        ))

    if res["exact_dup_count"] > 0:
        recs.append(("MED",
            f"Deduplicate {res['exact_dup_count']:,} repeated pairs",
            "spread across dataset",
            "Duplicate pairs overfit certain phrases.",
            """\
  import pandas as pd
  df = pd.read_csv('your_file.tsv', sep='\\t')
  df.drop_duplicates().to_csv('cleaned.tsv', sep='\\t', index=False)"""
        ))

    if res["html_mismatch"] > 0:
        pct = res["html_mismatch"] / total * 100
        sev = "MED" if pct > 1 else "LOW"
        recs.append((sev,
            f"Strip HTML tags from {res['html_mismatch']:,} mismatched pairs",
            "both columns",
            "Mismatched tags appear verbatim in model output.",
            """\
  import pandas as pd, re
  df = pd.read_csv('your_file.tsv', sep='\\t')
  strip = lambda s: re.sub(r'<[^>]+>', '', str(s)).strip()
  df.iloc[:,0] = df.iloc[:,0].apply(strip)
  df.iloc[:,1] = df.iloc[:,1].apply(strip)
  df.to_csv('cleaned.tsv', sep='\\t', index=False)"""
        ))

    n_punc = max(res["src_only_punc"], res["tgt_only_punc"])
    n_num  = res["numeric_only"]
    if n_punc + n_num > 0:
        recs.append(("LOW",
            f"Remove {n_punc + n_num:,} punctuation-only / numeric-only pairs",
            "mixed across dataset",
            "These pairs carry no translatable content.",
            """\
  import pandas as pd, re
  df = pd.read_csv('your_file.tsv', sep='\\t')
  has_word = lambda s: bool(re.search(r'[a-zA-Z\\u00C0-\\uFFFF]', str(s)))
  mask = df.iloc[:,0].apply(has_word) & df.iloc[:,1].apply(has_word)
  df[mask].to_csv('cleaned.tsv', sep='\\t', index=False)"""
        ))

    sl = res["src_len_stats"]
    if sl["mean"] > sl["median"] * 3:
        recs.append(("MED",
            "Address heavy length skew (mean >> median)",
            "length distribution",
            f"Mean ({sl['mean']} tok) is {round(sl['mean']/max(sl['median'],1),1)}× the median ({sl['median']} tok).",
            f"""\
  import pandas as pd
  df = pd.read_csv('your_file.tsv', sep='\\t')
  df['src_len'] = df.iloc[:,0].str.split().str.len()
  df.nlargest(20, 'src_len').to_csv('longest_pairs.csv', index=False)"""
        ))

    if not recs:
        print(f"\n{bold('Recommendations')}")
        print(f"  {green('✓')}  No significant issues found. Dataset looks clean.")
        return

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

    print(f"\n{bold('Quality issues')}")
    print(f"  {'':2}  {'Issue':<40}  {'Count':>7}  {'%'}")
    print(f"  {'─' * 58}")
    print(fmt_issue(res["src_empty"],            total, "Source-side empty"))
    print(fmt_issue(res["tgt_empty"],            total, "Target-side empty"))
    print(fmt_issue(res["both_empty"],           total, "Both sides empty"))
    print(fmt_issue(res["src_only_punc"],        total, "Source punctuation-only"))
    print(fmt_issue(res["tgt_only_punc"],        total, "Target punctuation-only"))
    print(fmt_issue(res["numeric_only"],         total, "Numeric-only pairs"))
    print(fmt_issue(res["non_printable"],        total, "Contains non-printable chars"))
    print(fmt_issue(res["html_mismatch"],        total, "HTML tag mismatch"))
    print(fmt_issue(len(res["ratio_issues"]),    total,
                    f"Length ratio outside [{args.ratio_min}–{args.ratio_max}]"))
    print(fmt_issue(len(res["short_pairs"]),     total,
                    f"Too short (< {args.min_tokens} tokens)"))
    print(fmt_issue(len(res["long_pairs"]),      total,
                    f"Too long  (> {args.max_tokens} tokens)"))

    print(f"\n{bold('Duplicates')}")
    print(fmt_issue(res["exact_dup_count"],  total, "Exact duplicate pairs"))
    print(fmt_issue(res["src_dup_count"],    total, "Duplicate source sentences"))
    print(fmt_issue(res["tgt_dup_count"],    total, "Duplicate target sentences"))

    weights = {
        "src_empty":    (res["src_empty"],          5),
        "tgt_empty":    (res["tgt_empty"],          5),
        "both_empty":   (res["both_empty"],         5),
        "ratio_issues": (len(res["ratio_issues"]),  2),
        "exact_dup":    (res["exact_dup_count"],    2),
        "short_pairs":  (len(res["short_pairs"]),   1),
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

    print_recommendations(res, args)

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

    if not args.no_histogram:
        ascii_histogram(res["src_lengths"], "Source")
        ascii_histogram(res["tgt_lengths"], "Target")

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
    flagged = {}

    def add(idx, reason):
        flagged.setdefault(idx - 1, set()).add(reason)

    for i, (s, t) in enumerate(pairs):
        if not s.strip(): add(i + 1, "src_empty")
        if not t.strip(): add(i + 1, "tgt_empty")

    for idx, *_ in res["ratio_issues"]:
        add(idx, "ratio")
    for idx, *_ in res["short_pairs"]:
        add(idx, "short")
    for idx, *_ in res["long_pairs"]:
        add(idx, "long")

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
        description="Analyze quality of parallel translation datasets (or monolingual corpora).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parallel CSV/TSV
  python parallel_analyzer.py data.tsv
  python parallel_analyzer.py data.tsv --src-col en --tgt-col tl

  # Two parallel corpus files
  python parallel_analyzer.py --src corpus_en.txt --tgt corpus_tl.txt

  # Monolingual file (single --src, no --tgt)
  python parallel_analyzer.py --src hil.txt
  python parallel_analyzer.py --src data/hil_raw.txt --min-tokens 5

  # Full parallel analysis with export
  python parallel_analyzer.py data.tsv --samples 10 --top-tokens --export flagged.tsv
""",
    )

    inp = p.add_argument_group("Input (pick one)")
    inp.add_argument("file", nargs="?", metavar="FILE",
                     help="CSV or TSV file path")
    inp.add_argument("--src", metavar="FILE",
                     help="Source corpus .txt (or only file for monolingual mode)")
    inp.add_argument("--tgt", metavar="FILE",
                     help="Target corpus .txt (omit for monolingual mode)")
    inp.add_argument("--src-col", metavar="COL",
                     help="Source column name or index (CSV/TSV, default: 0)")
    inp.add_argument("--tgt-col", metavar="COL",
                     help="Target column name or index (CSV/TSV, default: 1)")

    thr = p.add_argument_group("Thresholds")
    thr.add_argument("--ratio-min",   type=float, default=0.2,  metavar="N")
    thr.add_argument("--ratio-max",   type=float, default=5.0,  metavar="N")
    thr.add_argument("--min-tokens",  type=int,   default=3,    metavar="N")
    thr.add_argument("--max-tokens",  type=int,   default=200,  metavar="N")

    out = p.add_argument_group("Output")
    out.add_argument("--samples",      type=int, default=5, metavar="N")
    out.add_argument("--no-histogram", action="store_true")
    out.add_argument("--top-tokens",   action="store_true")
    out.add_argument("--export",       metavar="FILE",
                     help="Export flagged pairs to TSV (parallel mode only)")
    out.add_argument("--no-color",     action="store_true")

    return p


def main():
    global USE_COLOR
    parser = build_parser()
    args   = parser.parse_args()

    if args.no_color:
        USE_COLOR = False

    # ── Monolingual mode: --src only, no --tgt, no positional file ────────────
    if args.src and not args.tgt and not args.file:
        if not os.path.isfile(args.src):
            sys.exit(red(f"Error: file not found: {args.src}"))
        print(f"\n{bold('Mode')}     : {cyan('monolingual')}")
        print(f"{bold('Loading')} : {cyan(args.src)} …")
        lines = load_txt_mono(args.src)
        print(f"  Loaded {len(lines):,} lines")
        print(f"  Analyzing …\n")
        res = analyze_mono(lines, args)
        print_report_mono(res, args)
        return

    # ── Parallel modes ────────────────────────────────────────────────────────
    if args.file:
        path = args.file
        if not os.path.isfile(path):
            sys.exit(red(f"Error: file not found: {path}"))
        ext = Path(path).suffix.lower()
        if ext == ".txt":
            sys.exit(red(
                "Error: for a single .txt file use --src (monolingual) or "
                "--src + --tgt (parallel).\n"
                "  Monolingual: python parallel_analyzer.py --src hil.txt\n"
                "  Parallel:    python parallel_analyzer.py --src en.txt --tgt tl.txt"
            ))
        print(f"\n{bold('Mode')}     : {cyan('parallel (CSV/TSV)')}")
        print(f"{bold('Loading')} : {cyan(path)} …")
        pairs, header = load_csv_tsv(path, args.src_col, args.tgt_col)
        print(f"  Detected {len(pairs):,} pairs  |  header: {header[:4]}")

    elif args.src and args.tgt:
        for p in (args.src, args.tgt):
            if not os.path.isfile(p):
                sys.exit(red(f"Error: file not found: {p}"))
        print(f"\n{bold('Mode')}     : {cyan('parallel (txt pair)')}")
        print(f"{bold('Loading')} : {cyan(args.src)} + {cyan(args.tgt)} …")
        pairs = load_txt_pair(args.src, args.tgt)
        print(f"  Loaded {len(pairs):,} pairs")

    else:
        parser.print_help()
        print(red(
            "\nError: provide a CSV/TSV file, --src + --tgt for parallel, "
            "or just --src for monolingual."
        ))
        sys.exit(1)

    print(f"  Analyzing …\n")
    results = analyze(pairs, args)
    print_report(results, args)

    if args.export:
        export_flagged(pairs, results, args.export)
        print()


if __name__ == "__main__":
    main()