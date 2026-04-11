"""
Combines en.txt and hil.txt into a single TSV file.
Each line in en.txt is paired with the corresponding line in hil.txt.
Output: combined.tsv with columns: en  hil
"""

import csv

# EN_FILE = "corpus_out/en_clean.txt"
# HIL_FILE = "corpus_out/hiligaynon.txt"
# OUT_FILE = "corpus_out/combined-set-1.tsv"

EN_FILE = "corpus_out/hil_clean.txt"
HIL_FILE = "corpus_out/english.txt"
OUT_FILE = "corpus_out/combined-set-2.tsv"


def combine(en_path: str, hil_path: str, out_path: str) -> None:
    with open(en_path, encoding="utf-8") as en_f, \
         open(hil_path, encoding="utf-8") as hil_f:
        en_lines = [line.rstrip("\n") for line in en_f]
        hil_lines = [line.rstrip("\n") for line in hil_f]

    if len(en_lines) != len(hil_lines):
        print(
            f"Warning: line count mismatch — en.txt has {len(en_lines)} lines, "
            f"hil.txt has {len(hil_lines)} lines. "
            "Extra lines in the longer file will be left blank in the other column."
        )

    length = max(len(en_lines), len(hil_lines))

    with open(out_path, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(out_f, delimiter="\t")
        writer.writerow(["en", "hil"])  # header row
        for i in range(length):
            en_val = en_lines[i] if i < len(en_lines) else ""
            hil_val = hil_lines[i] if i < len(hil_lines) else ""
            writer.writerow([en_val, hil_val])

    print(f"Done! {length} rows written to '{out_path}'.")


if __name__ == "__main__":
    combine(EN_FILE, HIL_FILE, OUT_FILE)
