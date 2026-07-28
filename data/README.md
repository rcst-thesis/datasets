# Data layout

- `external/`: third-party datasets retained in their original form.
- `raw/`: source captures, phrase sheets, dictionaries, and scraper caches.
- `interim/`: intermediate files used by preparation pipelines.
- `processed/`: model-ready and derived datasets.
- `releases/`: packaged snapshots retained for reproducibility.

`mideval/` remains at the repository root as a protected evaluation collection.
Files are moved between stages only when their provenance and processing status
are known; formats and contents are otherwise preserved.
