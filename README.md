---
language:
  - hil
  - en
task_categories:
  - translation
  - text-generation
pretty_name: Hiligaynon-English Dataset Collection
tags:
  - hiligaynon
  - parallel-corpus
---

# Hiligaynon-English Dataset Collection

A working collection of Hiligaynon and English parallel, monolingual, and
instruction-tuning data for machine-translation research.

## Repository layout

```text
.
├── data/
│   ├── external/    # third-party datasets, unchanged
│   ├── raw/         # source captures, dictionaries, and phrase sheets
│   ├── interim/     # pipeline intermediates
│   ├── processed/   # model-ready and derived files
│   └── releases/    # packaged dataset snapshots
├── docs/references/ # linguistic references and source documents
├── mideval/         # protected evaluation datasets
├── notebooks/       # exploratory notebooks
├── scripts/         # scraping, conversion, validation, and sampling CLIs
├── tests/           # standard-library unit tests
└── tsv-editor/      # browser-based corpus editor (nested repository)
```

See [data/README.md](data/README.md) for the stage definitions.

## Main datasets

| Path | Content |
| --- | --- |
| `data/processed/parallel-149k/` | `source,target` training and validation CSV files |
| `data/processed/en-hil/` | Hiligaynon-English TSV and JSONL exports |
| `data/raw/bible/` | Verse-aligned Hiligaynon-English corpus and scraper cache |
| `data/external/tagalog-filipino-english/` | External Tagalog-English train/test data |
| `data/processed/pretraining/` | Cleaned, balanced, and shuffled pretraining corpora |
| `data/processed/instruction/` | Hiligaynon instruction-tuning data |
| `mideval/` | Evaluation-only data; intentionally kept separate |

Schemas are not yet normalized across sources. Inspect each file's header before
combining datasets.

## Common commands

Run commands from the repository root.

```bash
# Sample exactly 1,000 data rows while preserving the TSV header
just sample INPUT.tsv 1000 -o sample.tsv --seed 42

# Analyze a parallel dataset
python scripts/parallel_analyzer.py data/processed/en-hil/en_hil.tsv

# Convert formats
python scripts/csv_to_tsv.py INPUT.csv OUTPUT.tsv
python scripts/jsonl_to_tsv.py INPUT.jsonl -o OUTPUT.tsv
python scripts/converter.py INPUT.parquet OUTPUT.tsv

# Build a balanced pretraining corpus
python scripts/corpus_prep.py EN.txt HIL.txt

# Run tests (or: python -m unittest discover -s tests -v)
just test
```

### Scrapers

```bash
python scripts/scrape_bombo.py --resume
python scripts/scrapescript.py --resume
```

Scraper outputs default to `data/raw/bombo/` and `data/raw/bible/`.

### TSV editor

```bash
python -m pip install -r tsv-editor/requirements.txt
python tsv-editor/app.py
```

The editor scans this repository by default. Set `TSV_DATA_DIR` to limit it to
another directory.

## Data provenance and licensing

This repository aggregates files from several sources, including web scrapes,
reference documents, spreadsheets, and external corpora. A single repository
license is not currently documented. Verify the provenance, terms, and
redistribution rights of each source before publishing or using it beyond
research.

The data may contain scraping errors, synthetic translations, duplicates,
inconsistent schemas, and culturally or linguistically inaccurate text.
Evaluation data in `mideval/` should not be mixed into training data.
