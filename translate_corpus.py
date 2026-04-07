#!/usr/bin/env python3
"""
translate_corpus.py — Batch EN → HIL translation of a monolingual corpus
using Helsinki-NLP/opus-mt-en-hil (MarianMT).

Install
-------
pip install transformers sentencepiece torch tqdm

Usage
-----
python translate_corpus.py input.txt output.hil.txt
python translate_corpus.py input.txt output.hil.txt --batch-size 64 --device cuda
python translate_corpus.py input.txt output.hil.txt --resume   # skip already-done lines
"""

import argparse
import sys
from pathlib import Path

import torch
from tqdm import tqdm
from transformers import MarianMTModel, MarianTokenizer

MODELS = {
    "en2hil": "Helsinki-NLP/opus-mt-en-hil",
    "hil2en": "Helsinki-NLP/opus-mt-hil-en",
}
MAX_LEN = 512


# ── Model ─────────────────────────────────────────────────────────────────────

def load_model(device: str, model_id: str):
    print(f"Loading {model_id} onto {device} …")
    tok   = MarianTokenizer.from_pretrained(model_id)
    model = MarianMTModel.from_pretrained(model_id).to(device)
    model.eval()
    print("Model ready.\n")
    return tok, model


# ── Batched translation ────────────────────────────────────────────────────────

def translate_batch(lines: list[str], tok, model, device: str) -> list[str]:
    inputs = tok(
        lines,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=MAX_LEN,
    ).to(device)

    with torch.no_grad():
        out = model.generate(
            **inputs,
            num_beams=4,
            max_length=MAX_LEN,
        )

    return [tok.decode(ids, skip_special_tokens=True) for ids in out]


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Batch EN→HIL corpus translator")
    parser.add_argument("input",  type=Path, help="Source English corpus (one sentence per line)")
    parser.add_argument("output", type=Path, help="Destination HIL file")
    parser.add_argument("--batch-size", type=int, default=32,
                        help="Sentences per forward pass (default 32; lower if OOM)")
    parser.add_argument("--device", type=str,
                        default="cuda" if torch.cuda.is_available() else "cpu",
                        help="Torch device (default: cuda if available, else cpu)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip lines already written to the output file")
    parser.add_argument("--direction", choices=["en2hil", "hil2en"], default="en2hil",
                        help="Translation direction (default: en2hil)")
    parser.add_argument("--num-beams", type=int, default=4,
                        help="Beam width (default 4; use 1 for greedy/fastest)")
    args = parser.parse_args()

    # ── Count source lines ────────────────────────────────────────────────────
    with args.input.open(encoding="utf-8") as f:
        total = sum(1 for _ in f)

    # ── Resume: count already-translated lines ────────────────────────────────
    done = 0
    if args.resume and args.output.exists():
        with args.output.open(encoding="utf-8") as f:
            done = sum(1 for _ in f)
        if done >= total:
            print(f"Output already complete ({done} lines). Nothing to do.")
            sys.exit(0)
        print(f"Resuming from line {done + 1} / {total}")

    model_id = MODELS[args.direction]
    tok, model = load_model(args.device, model_id)

    # Patch beam width into generate if changed
    def translate_batch_beams(lines):
        inputs = tok(
            lines,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=MAX_LEN,
        ).to(args.device)
        with torch.no_grad():
            out = model.generate(**inputs, num_beams=args.num_beams, max_length=MAX_LEN)
        return [tok.decode(ids, skip_special_tokens=True) for ids in out]

    # ── Stream through corpus ─────────────────────────────────────────────────
    mode   = "a" if (args.resume and done > 0) else "w"
    errors = 0

    with (
        args.input.open(encoding="utf-8")           as src,
        args.output.open(mode, encoding="utf-8")    as dst,
        tqdm(total=total, initial=done, unit="sent", dynamic_ncols=True) as bar,
    ):
        # Skip already-translated lines when resuming
        for _ in range(done):
            next(src)

        batch_src: list[str] = []
        batch_idx: list[int] = []

        def flush():
            nonlocal errors
            if not batch_src:
                return
            try:
                translations = translate_batch_beams(batch_src)
            except Exception as e:
                # Fall back to single-sentence translation on batch error
                translations = []
                for line in batch_src:
                    try:
                        translations.extend(translate_batch_beams([line]))
                    except Exception:
                        translations.append("")
                        errors += 1
            for t in translations:
                dst.write(t + "\n")
            dst.flush()
            bar.update(len(batch_src))
            batch_src.clear()
            batch_idx.clear()

        for line in src:
            sentence = line.rstrip("\n")
            # Pass through blank lines as-is to preserve alignment
            if not sentence.strip():
                flush()
                dst.write("\n")
                dst.flush()
                bar.update(1)
                continue

            batch_src.append(sentence)
            if len(batch_src) >= args.batch_size:
                flush()

        flush()  # Final partial batch

    print(f"\nDone [{args.direction}]. {total - done} sentences translated → {args.output}")
    if errors:
        print(f"  ⚠  {errors} sentence(s) failed and were written as empty lines.")


if __name__ == "__main__":
    main()