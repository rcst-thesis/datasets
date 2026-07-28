import csv
import tempfile
import unittest
from pathlib import Path

from scripts.combine_csv import combine_csv


class CombineCsvTest(unittest.TestCase):
    def test_combines_one_header_and_all_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            inputs = [directory / "a.csv", directory / "b.csv"]
            inputs[0].write_text("id,text\n1,hello\n", encoding="utf-8")
            inputs[1].write_text("id,text\n2,world\n", encoding="utf-8")
            output = directory / "combined.csv"

            self.assertEqual(combine_csv(inputs, output), 2)
            with output.open(newline="", encoding="utf-8") as file:
                self.assertEqual(
                    list(csv.reader(file)),
                    [["id", "text"], ["1", "hello"], ["2", "world"]],
                )

    def test_combines_headerless_files_without_dropping_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            inputs = [directory / "a.csv", directory / "b.csv"]
            inputs[0].write_text("1,hello\n", encoding="utf-8")
            inputs[1].write_text("2,world\n", encoding="utf-8")
            output = directory / "combined.csv"

            self.assertEqual(combine_csv(inputs, output, has_header=False), 2)
            self.assertEqual(output.read_text(encoding="utf-8"), "1,hello\n2,world\n")


if __name__ == "__main__":
    unittest.main()
