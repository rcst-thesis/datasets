default:
    @just --list

# Sample random rows from a TSV: just sample INPUT.tsv COUNT -o OUTPUT.tsv [--seed N]
sample *args:
    python3 scripts/sample_tsv.py {{ args }}

# Run the unit tests
test:
    python3 -m unittest discover -s tests -v
