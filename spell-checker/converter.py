#!/usr/bin/env python3
"""
Hiligaynon/Ilonggo City-Dialect Converter
Rule-based Tagalog → Iloilo-style Hiligaynon rewriter.
"""

import csv
import re
import sys
import os
from pathlib import Path


# ── LOADERS ──

def load_dict(filepath):
    """Load a two-column CSV (base_word,target_word) into a dict."""
    mapping = {}
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[row["base_word"].strip().lower()] = row["target_word"].strip()
    return mapping


def load_phrases(filepath):
    """Load phrase CSV (pattern,replacement) into list of (compiled regex, replacement)."""
    phrases = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pattern = row["pattern"].strip()
            replacement = row["replacement"].strip()
            phrases.append((re.compile(pattern, re.IGNORECASE), replacement))
    return phrases


# ── PIPELINE STEPS ──

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
            # preserve leading uppercase
            if word[0].isupper():
                return target[0].upper() + target[1:]
            return target
        return word

    return re.sub(r"[\w'-]+", replacer, text)


def apply_sentence_rules(text):
    """Phase 4: Structural adjustments (ay-inversion, ba→bala, etc.)."""
    # Remove ay inversion: "Si/Ang X ... ay Y" → "Si/Ang X ... Y"
    text = re.sub(r"\b((?:si|ang)\s+[\w\s]+?)\s+ay\s+", r"\1 ", text, flags=re.IGNORECASE)
    # ba → bala (standalone)
    text = re.sub(r"\bba\b", "bala", text)
    return text


# Words produced by our pipeline that the letter-rule must never touch
_PROTECTED = {"bala", "balay", "kahibalo", "malaba", "dalagan", "magdalagan",
              "nagdalagan", "magadalagan", "palangga", "maglakat", "naglakat",
              "malakat", "magalakat", "lakat", "paligo", "magpaligo"}


def apply_letter_rules(text, word_map, verb_map):
    """Phase 5: Conservative l→r before vowels on unknown words."""
    known = set(word_map.values()) | set(verb_map.values()) | set(word_map.keys()) | set(verb_map.keys()) | _PROTECTED

    def replacer(match):
        word = match.group(0)
        if len(word) < 4:
            return word
        if word.lower() in known:
            return word
        # l→r before a vowel, but not at word start
        return re.sub(r"(?<=\w)l(?=[aeiou])", lambda m: "R" if m.group().isupper() else "r", word, flags=re.IGNORECASE)

    return re.sub(r"\b[\w'-]+\b", replacer, text)


# ── MAIN CONVERTER ──

class HiligaynonConverter:
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
        text = apply_word_mapping(text, self.verb_map)   # verbs first (more specific)
        text = apply_word_mapping(text, self.word_map)    # then general words
        text = apply_sentence_rules(text)
        text = apply_letter_rules(text, self.word_map, self.verb_map)
        return text


# ── CLI ──

def main():
    converter = HiligaynonConverter()

    # File mode: python converter.py input.txt
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                print(converter.convert(line.strip()))
        return

    # Interactive mode
    print("=== Tagalog → Hiligaynon (Iloilo City-Style) ===")
    print("Type a Tagalog sentence, or 'q' to quit.\n")
    while True:
        try:
            text = input("Tagalog > ")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if text.strip().lower() in ("q", "quit", "exit"):
            break
        result = converter.convert(text)
        print(f"Ilonggo > {result}\n")


if __name__ == "__main__":
    main()
