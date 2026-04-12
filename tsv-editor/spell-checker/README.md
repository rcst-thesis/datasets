# Hiligaynon/Ilonggo City-Dialect Converter

Rule-based Tagalog → Iloilo city-style Hiligaynon rewriter. No AI — just dictionaries, verb patterns, and letter-level shifts.

## Framework

Uses [`transliterate`](https://github.com/barseghyanartur/transliterate) by barseghyanartur for the letter-level dialect shift layer (l→r before vowels via a custom `HiligaynonPack` language pack). Word/verb/phrase layers are CSV-driven.

## Setup

```bash
pip install transliterate
```

## Usage

```bash
# Interactive
python converter.py

# File → stdout
python converter.py input.txt

# File → file
python converter.py input.txt -o output.txt
```

```python
# As library
from converter import HiligaynonConverter
c = HiligaynonConverter()
print(c.convert("Matutulog na ako."))  # → Magturog na ako.
```

## Pipeline

| Step | Source | What it does |
|------|--------|-------------|
| 1. Phrases | `data/phrases.csv` | Regex phrase replacements (e.g. "Magandang umaga" → "Maayo nga aga") |
| 2. Verbs | `data/verbs.csv` | Verb form mappings (e.g. "kumain" → "magkaon") |
| 3. Words | `data/words.csv` | Word-level dictionary (e.g. "bahay" → "balay") |
| 4. Syntax | hardcoded | Removes `ay` inversion, `ba` → `bala` |
| 5. Letters | `transliterate` | l→r shifts on remaining unknown words |

## Expanding the dictionaries

All data files are plain CSVs — open in Excel, Google Sheets, or any text editor. Just add rows:

- **words.csv**: `base_word,target_word` — one Tagalog→Hiligaynon pair per line
- **verbs.csv**: same format, for conjugated verb forms
- **phrases.csv**: `pattern,replacement` — regex patterns for multi-word expressions
