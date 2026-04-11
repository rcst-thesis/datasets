#!/usr/bin/env python3
"""
Hiligaynon/Ilonggo City-Dialect Converter
Uses barseghyanartur/transliterate framework for letter-level rules,
plus CSV-driven word/verb/phrase pipeline layers.

Framework: https://github.com/barseghyanartur/transliterate
Install:   pip install transliterate
"""

import csv
import re
import sys
from pathlib import Path

from transliterate.base import TranslitLanguagePack, registry
from transliterate import translit


# ═══════════════════════════════════════════════════════════════════
# LAYER 1 — transliterate language pack (letter-level dialect shifts)
# ═══════════════════════════════════════════════════════════════════

class HiligaynonPack(TranslitLanguagePack):
    """
    Custom transliterate language pack for Iloilo city-style shifts.
    Handles the final letter-level pass (l→r before vowels, etc.)
    via pre_processor_mapping for multi-char patterns.

    To add more letter rules, just add entries to pre_processor_mapping.
    """
    language_code = "hil"
    language_name = "Hiligaynon"

    # Single-char mapping: (source_chars, target_chars)
    # Kept minimal — most work is done at word/phrase level.
    mapping = (
        u"",   # source chars
        u"",   # target chars
    )

    # Multi-character pattern replacements applied in order
    # before single-char mapping. These catch l→r shifts.
    pre_processor_mapping = {
        u"la": u"ra", u"le": u"re", u"li": u"ri",
        u"lo": u"ro", u"lu": u"ru",
    }


# Register once
try:
    registry.register(HiligaynonPack)
except Exception:
    pass


# ═══════════════════════════════════════════════════════════════════
# LAYER 2 — CSV loaders
# ═══════════════════════════════════════════════════════════════════

def load_dict(filepath):
    """Load a two-column CSV (base_word,target_word) into a dict."""
    mapping = {}
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[row["base_word"].strip().lower()] = row["target_word"].strip()
    return mapping


def load_phrases(filepath):
    """Load phrase CSV (pattern,replacement) into [(compiled_regex, replacement)]."""
    phrases = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pattern = row["pattern"].strip()
            replacement = row["replacement"].strip()
            phrases.append((re.compile(pattern, re.IGNORECASE), replacement))
    return phrases


# ═══════════════════════════════════════════════════════════════════
# LAYER 3 — Pipeline steps
# ═══════════════════════════════════════════════════════════════════

def normalize(text):
    return re.sub(r"\s+", " ", text).strip()


def apply_phrase_map(text, phrases):
    """Phase 1: Full phrase replacements (highest priority)."""
    for pattern, replacement in phrases:
        def _repl(m, r=replacement):
            if m.group(0)[0].isupper():
                return r[0].upper() + r[1:]
            return r
        text = pattern.sub(_repl, text)
    return text


def apply_word_mapping(text, word_map):
    """Phase 2/3: Replace individual words via dictionary lookup."""
    def replacer(match):
        word = match.group(0)
        lower = word.lower()
        if lower in word_map:
            target = word_map[lower]
            if word[0].isupper():
                return target[0].upper() + target[1:]
            return target
        return word
    return re.sub(r"[\w'-]+", replacer, text)


def apply_sentence_rules(text):
    """Phase 4: Structural adjustments."""
    # Remove ay inversion
    text = re.sub(r"\b((?:si|ang)\s+[\w\s]+?)\s+ay\s+", r"\1 ", text, flags=re.IGNORECASE)
    # ba → bala
    text = re.sub(r"\bba\b", "bala", text)
    return text


# Words our pipeline produces that the letter-pass must not touch
_PROTECTED = {
    "bala", "balay", "kahibalo", "malaba", "dalagan", "magdalagan",
    "nagdalagan", "magadalagan", "palangga", "maglakat", "naglakat",
    "malakat", "magalakat", "lakat", "paligo", "magpaligo", "dali",
    "makaon", "nagkaon", "magakaon", "kan-on", "baklon", "hatagan",
    "malipayon", "malain", "maluya", "mahilom", "matinlo",
}


def apply_letter_rules(text, word_map, verb_map):
    """
    Phase 5: Use transliterate framework's HiligaynonPack for
    letter-level shifts on words NOT already converted.
    """
    known = (
        set(word_map.values()) | set(verb_map.values()) |
        set(word_map.keys()) | set(verb_map.keys()) | _PROTECTED
    )

    def replacer(match):
        word = match.group(0)
        lower = word.lower()
        if len(word) < 4 or lower in known:
            return word
        # Apply transliterate pack per-word
        result = translit(lower, "hil")
        if word[0].isupper():
            result = result[0].upper() + result[1:]
        return result

    return re.sub(r"\b[\w'-]+\b", replacer, text)


# ═══════════════════════════════════════════════════════════════════
# CONVERTER CLASS
# ═══════════════════════════════════════════════════════════════════

class HiligaynonConverter:
    """
    Pipeline:
      1. Phrase-level   (CSV: phrases.csv)
      2. Verb forms     (CSV: verbs.csv)
      3. Word-level     (CSV: words.csv)
      4. Sentence rules (hardcoded regex)
      5. Letter shifts  (transliterate framework — HiligaynonPack)

    All dictionaries are plain CSVs — edit in any spreadsheet app.
    """

    def __init__(self, data_dir=None):
        if data_dir is None:
            data_dir = Path(__file__).parent / "data"
        data_dir = Path(data_dir)

        self.word_map = load_dict(data_dir / "words.csv")
        self.verb_map = load_dict(data_dir / "verbs.csv")
        self.phrases = load_phrases(data_dir / "phrases.csv")

    def convert(self, text):
        if not text.strip():
            return ""
        text = normalize(text)
        text = apply_phrase_map(text, self.phrases)
        text = apply_word_mapping(text, self.verb_map)
        text = apply_word_mapping(text, self.word_map)
        text = apply_sentence_rules(text)
        text = apply_letter_rules(text, self.word_map, self.verb_map)
        return text


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    converter = HiligaynonConverter()

    if len(sys.argv) > 1 and sys.argv[1] not in ("-h", "--help"):
        infile = sys.argv[1]
        outfile = sys.argv[3] if len(sys.argv) > 3 and sys.argv[2] == "-o" else None
        lines = []
        with open(infile, encoding="utf-8") as f:
            for line in f:
                lines.append(converter.convert(line.strip()))
        output = "\n".join(lines)
        if outfile:
            with open(outfile, "w", encoding="utf-8") as f:
                f.write(output)
            print(f"Wrote {len(lines)} lines to {outfile}")
        else:
            print(output)
        return

    if len(sys.argv) > 1:
        print("Usage:")
        print("  python converter.py                    # interactive mode")
        print("  python converter.py input.txt          # file → stdout")
        print("  python converter.py input.txt -o out   # file → file")
        return

    print("=== Tagalog → Hiligaynon (Iloilo City-Style) ===")
    print("Framework: transliterate (github.com/barseghyanartur/transliterate)")
    print("Type a Tagalog sentence, or 'q' to quit.\n")
    while True:
        try:
            text = input("Tagalog > ")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if text.strip().lower() in ("q", "quit", "exit"):
            break
        print(f"Ilonggo > {converter.convert(text)}\n")


if __name__ == "__main__":
    main()