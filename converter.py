import pandas as pd
import sys
from pathlib import Path

def parquet_to_tsv(input_path: str, output_path: str = None):
    input_file = Path(input_path)
    
    if output_path is None:
        output_path = input_file.with_suffix(".tsv")
    
    df = pd.read_parquet(input_file)
    df.to_csv(output_path, sep="\t", index=False)
    
    print(f"Converted: {input_file} → {output_path}")
    print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python converter.py <input.parquet> [output.tsv]")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    parquet_to_tsv(input_path, output_path)