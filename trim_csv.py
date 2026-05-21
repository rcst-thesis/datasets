import csv
import sys
import math

def trim_csv(input_path, output_path=None):
    if output_path is None:
        output_path = input_path  # overwrite in place

    with open(input_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    rows_to_delete = math.ceil(len(rows) * 0.20)
    trimmed_rows = rows[rows_to_delete:]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(trimmed_rows)

    print(f"Total data rows: {len(rows)}")
    print(f"Deleted first {rows_to_delete} rows (20%)")
    print(f"Remaining rows: {len(trimmed_rows)}")
    print(f"Output saved to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python trim_csv.py <input.csv> [output.csv]")
        print("  If output.csv is omitted, the input file is overwritten.")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    trim_csv(input_file, output_file)
