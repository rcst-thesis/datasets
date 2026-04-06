#!/usr/bin/env python3
"""
ocr_pdf.py — OCR a scanned PDF to text, one page at a time.

Why images? Scanned PDFs have no text layer — each page is a raster image
embedded in a PDF wrapper. pdf2image extracts those images so Tesseract can
read them. Processing one page at a time keeps memory flat regardless of
how large the PDF is.

Usage:
    python ocr_pdf.py input.pdf output.txt
    python ocr_pdf.py input.pdf output.txt --dpi 200 --lang eng
"""

import argparse
import sys
from pathlib import Path

from pdf2image import convert_from_path
import pytesseract


def ocr_pdf_to_text(
    pdf_path: str | Path,
    output_text_path: str | Path,
    dpi: int = 300,
    lang: str = "eng",
) -> None:
    pdf_path = Path(pdf_path)
    output_text_path = Path(output_text_path)

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    output_text_path.parent.mkdir(parents=True, exist_ok=True)

    # Get total page count without loading any images
    from pdf2image.pdf2image import pdfinfo_from_path
    info = pdfinfo_from_path(pdf_path)
    total_pages = info["Pages"]
    print(f"Found {total_pages} page(s) in {pdf_path.name}")

    with open(output_text_path, "w", encoding="utf-8") as out:
        for page_num in range(1, total_pages + 1):
            print(f"  OCR page {page_num}/{total_pages}...", end="\r")

            # Load exactly one page — avoids holding the whole PDF in memory
            images = convert_from_path(
                pdf_path,
                dpi=dpi,
                first_page=page_num,
                last_page=page_num,
            )

            text = pytesseract.image_to_string(images[0], lang=lang)

            out.write(f"--- PAGE {page_num} ---\n")
            out.write(text.strip() + "\n\n")

            # images[0] goes out of scope here → freed immediately

    print(f"\nDone. Text saved to: {output_text_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OCR a scanned PDF to a text file.")
    parser.add_argument("input_pdf", help="Path to the scanned PDF")
    parser.add_argument(
        "output_txt",
        nargs="?",           # optional positional
        default=None,
        help="Path for the output .txt file (default: <input_pdf>.txt)",
    )
    parser.add_argument("--dpi", type=int, default=300, help="Render DPI (default: 300)")
    # Hiligaynon uses the Latin alphabet — eng handles it fine.
    # Pass e.g. --lang tgl for Tagalog if Tesseract has that pack installed.
    parser.add_argument("--lang", default="eng", help="Tesseract language code (default: eng)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    input_pdf = Path(args.input_pdf)
    output_txt = Path(args.output_txt) if args.output_txt else input_pdf.with_suffix(".txt")
    try:
        ocr_pdf_to_text(
            pdf_path=input_pdf,
            output_text_path=output_txt,
            dpi=args.dpi,
            lang=args.lang,
        )
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)