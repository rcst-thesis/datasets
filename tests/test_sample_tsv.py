import csv
import tempfile
import unittest
from pathlib import Path

from scripts.sample_tsv import sample_tsv


class SampleTsvTest(unittest.TestCase):
    def test_writes_exact_reproducible_sample_with_header(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.tsv"
            first = root / "first.tsv"
            second = root / "second.tsv"
            source.write_text(
                "src\ttgt\n" + "".join(f"s{i}\tt{i}\n" for i in range(20)),
                encoding="utf-8",
            )

            sample_tsv(source, first, 5, seed=7)
            sample_tsv(source, second, 5, seed=7)

            with first.open(encoding="utf-8", newline="") as file:
                rows = list(csv.reader(file, delimiter="\t"))
            self.assertEqual(["src", "tgt"], rows[0])
            self.assertEqual(5, len(rows) - 1)
            self.assertEqual(first.read_bytes(), second.read_bytes())


if __name__ == "__main__":
    unittest.main()
