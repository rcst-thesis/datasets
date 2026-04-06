# Getting started

### Convert your csv to tsv
```
CSV to TSV Converter CLI

Usage: python csv_to_tsv.py <input.csv> [output.tsv]

Examples:
    python csv_to_tsv.py data.csv           # Creates data.tsv
    python csv_to_tsv.py data.csv out.tsv   # Creates out.tsv
```
This will convert any valid `.csv` file into equivalent `.tsv`

### Validating the quality of a dataset
```
 python parallel_analyzer.py tagalog-filipino-english-translation/train_data.csv
```
Should specify the valid `.tsv` as the target file

### Scraping dataset
Install this deps
```
pip install requests beautifulsoup4 langdetect tqdm lxml
```

Running the scraper
```
# Start fresh, all categories
python scrape_bombo.py

# Just the most Hiligaynon-dense categories
python scrape_bombo.py --categories top-stories balita-espesyal

# If you start getting 429s, raise the delay and resume
python scrape_bombo.py --delay 3.0 --resume
```

### Converting opus datasets
OPUS corpora come in a few formats — let me make it handle all the common ones.Three input modes depending on what OPUS gave you:

```bash
# Most common — paired plain text files
python opus_to_tsv.py --paired en.txt hil.txt -o test.tsv

# TMX translation memory
python opus_to_tsv.py --tmx corpus.tmx --src-lang en --tgt-lang hil -o test.tsv

# Moses-style XML (the .xml files from OPUS downloads)
python opus_to_tsv.py --xml en.xml hil.xml -o test.tsv
```

Sampling flags for building a proper test split:
```bash
# Grab 1000 random pairs as your test set
python opus_to_tsv.py --paired en.txt hil.txt -o test.tsv --shuffle --max 1000
```

Output is a two-column TSV with a `src`/`tgt` header by default — pass `--src-col` / `--tgt-col` to rename them. `--skip-empty` drops any pairs where either side is blank, which is worth doing before evaluation.

### JSONL to TSV
```bash
# Output defaults to same name as input with .tsv extension
python jsonl_to_tsv.py corpus.jsonl

# Explicit output path
python jsonl_to_tsv.py corpus.jsonl -o data/test.tsv
```

Your sample data would produce:
```
src	tgt
Hiligaynon	Hiligaynon
I	Ako
```

One note on `trgs` — it's a list, so by default only the first translation is taken. If you want every target as its own row (useful if entries have multiple valid translations), pass `--all-trgs`.