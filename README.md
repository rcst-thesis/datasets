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