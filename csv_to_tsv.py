#!/usr/bin/env python3
"""
CSV to TSV Converter CLI

Usage: python csv_to_tsv.py <input.csv> [output.tsv]

Examples:
    python csv_to_tsv.py data.csv           # Creates data.tsv
    python csv_to_tsv.py data.csv out.tsv   # Creates out.tsv
"""

import csv
import sys
from pathlib import Path


def csv_to_tsv(input_file: str, output_file: str = None) -> str:
    """
    Convert a CSV file to TSV format.
    """
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"Error: File not found: {input_file}", file=sys.stderr)
        sys.exit(1)
    
    if output_file is None:
        output_path = input_path.with_suffix('.tsv')
    else:
        output_path = Path(output_file)
    
    try:
        with open(input_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as tsvfile:
            writer = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')
            writer.writerows(rows)
        
        print(f"✓ Converted: {input_file} → {output_path}")
        print(f"  Rows: {len(rows)}")
        
        return str(output_path)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    csv_to_tsv(input_file, output_file)


if __name__ == "__main__":
    main()