"""
Bilingual corpus prep pipeline (English + Hiligaynon)
Steps: inspect → clean → balance → concatenate → shuffle → tokenize
"""

import re
import random
import argparse
import subprocess
from pathlib import Path


# ── 1. Inspect ────────────────────────────────────────────────────────────────

def count_lines_and_words(path: Path) -> tuple[int, int]:
    lines = path.read_text(encoding="utf-8").splitlines()
    lines = [l for l in lines if l.strip()]
    words = sum(len(l.split()) for l in lines)
    return len(lines), words


def inspect(en_path: Path, hil_path: Path):
    en_lines, en_words = count_lines_and_words(en_path)
    hil_lines, hil_words = count_lines_and_words(hil_path)

    print("=== Corpus Stats ===")
    print(f"  English    : {en_lines:>10,} lines | {en_words:>12,} words")
    print(f"  Hiligaynon : {hil_lines:>10,} lines | {hil_words:>12,} words")
    ratio = en_lines / hil_lines if hil_lines else float("inf")
    print(f"  EN/HIL ratio: {ratio:.2f}x")
    print()
    return en_lines, hil_lines


# ── 2. Clean ──────────────────────────────────────────────────────────────────

def clean_line(line: str) -> str:
    line = re.sub(r"\s+", " ", line)   # collapse whitespace
    line = line.strip()
    return line


def clean_file(src: Path, dst: Path):
    lines = src.read_text(encoding="utf-8").splitlines()
    cleaned = [clean_line(l) for l in lines if clean_line(l)]
    dst.write_text("\n".join(cleaned) + "\n", encoding="utf-8")
    print(f"  Cleaned  : {src.name} → {dst.name}  ({len(cleaned):,} lines)")


# ── 3. Balance ────────────────────────────────────────────────────────────────

def downsample(src: Path, dst: Path, n: int, seed: int = 42):
    """Randomly sample n lines from src (without replacement)."""
    lines = src.read_text(encoding="utf-8").splitlines()
    lines = [l for l in lines if l.strip()]
    random.seed(seed)
    sampled = random.sample(lines, min(n, len(lines)))
    dst.write_text("\n".join(sampled) + "\n", encoding="utf-8")
    print(f"  Downsample: {src.name} → {dst.name}  ({len(sampled):,} lines)")


def upsample(src: Path, dst: Path, n: int, seed: int = 42):
    """Repeat lines from src (with replacement) until n lines."""
    lines = src.read_text(encoding="utf-8").splitlines()
    lines = [l for l in lines if l.strip()]
    random.seed(seed)
    upsampled = random.choices(lines, k=n)
    dst.write_text("\n".join(upsampled) + "\n", encoding="utf-8")
    print(f"  Upsample  : {src.name} → {dst.name}  ({len(upsampled):,} lines)")


# ── 4. Concatenate & Shuffle ──────────────────────────────────────────────────

def concatenate(paths: list[Path], dst: Path):
    all_lines = []
    for p in paths:
        lines = p.read_text(encoding="utf-8").splitlines()
        all_lines.extend([l for l in lines if l.strip()])
    dst.write_text("\n".join(all_lines) + "\n", encoding="utf-8")
    print(f"  Concat    : {[p.name for p in paths]} → {dst.name}  ({len(all_lines):,} lines)")


def shuffle_file(src: Path, dst: Path, seed: int = 42):
    lines = src.read_text(encoding="utf-8").splitlines()
    lines = [l for l in lines if l.strip()]
    random.seed(seed)
    random.shuffle(lines)
    dst.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Shuffle   : {src.name} → {dst.name}")


# ── 5. SentencePiece tokenizer ────────────────────────────────────────────────

def train_tokenizer(corpus: Path, model_prefix: str, vocab_size: int = 8000):
    try:
        import sentencepiece as spm
    except ImportError:
        print("  [skip] sentencepiece not installed. Run: pip install sentencepiece")
        return

    spm.SentencePieceTrainer.train(
        input=str(corpus),
        model_prefix=model_prefix,
        vocab_size=vocab_size,
        character_coverage=1.0,
        model_type="unigram",
        pad_id=3,
    )
    print(f"  Tokenizer : trained → {model_prefix}.model / {model_prefix}.vocab")


def encode_corpus(corpus: Path, model_prefix: str, dst: Path):
    try:
        import sentencepiece as spm
    except ImportError:
        print("  [skip] sentencepiece not installed.")
        return

    sp = spm.SentencePieceProcessor(model_file=f"{model_prefix}.model")
    lines = corpus.read_text(encoding="utf-8").splitlines()
    encoded = [" ".join(sp.encode(l, out_type=str)) for l in lines if l.strip()]
    dst.write_text("\n".join(encoded) + "\n", encoding="utf-8")
    print(f"  Encoded   : {corpus.name} → {dst.name}")


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run(
    en_path: Path,
    hil_path: Path,
    out_dir: Path,
    strategy: str = "downsample_en",   # or "upsample_hil"
    vocab_size: int = 8000,
    seed: int = 42,
    tokenize: bool = False,
):
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\n── Step 1: Inspect ─────────────────────────────────────────────")
    en_lines, hil_lines = inspect(en_path, hil_path)

    print("── Step 2: Clean ───────────────────────────────────────────────")
    en_clean  = out_dir / "en_clean.txt"
    hil_clean = out_dir / "hil_clean.txt"
    clean_file(en_path,  en_clean)
    clean_file(hil_path, hil_clean)

    print("── Step 3: Balance ─────────────────────────────────────────────")
    en_bal  = out_dir / "en_balanced.txt"
    hil_bal = out_dir / "hil_balanced.txt"
    target  = min(en_lines, hil_lines)   # common target

    if strategy == "downsample_en":
        downsample(en_clean,  en_bal,  n=hil_lines, seed=seed)
        hil_bal.write_text(hil_clean.read_text())      # Hiligaynon unchanged (copy, don't move)
    elif strategy == "upsample_hil":
        upsample(hil_clean, hil_bal, n=en_lines, seed=seed)
        en_bal.write_text(en_clean.read_text())        # English unchanged (copy, don't move)
    else:
        # balanced: downsample both to the smaller
        downsample(en_clean,  en_bal,  n=target, seed=seed)
        downsample(hil_clean, hil_bal, n=target, seed=seed)

    print("── Step 4: Concatenate & Shuffle ───────────────────────────────")
    mixed          = out_dir / "mixed.txt"
    mixed_shuffled = out_dir / "mixed_shuffled.txt"
    concatenate([en_bal, hil_bal], mixed)
    shuffle_file(mixed, mixed_shuffled, seed=seed)

    if tokenize:
        print("── Step 5: Tokenizer ────────────────────────────────────────────")
        model_prefix = str(out_dir / "sp_model")
        train_tokenizer(mixed_shuffled, model_prefix, vocab_size)
        encoded = out_dir / "mixed_shuffled.sp.txt"
        encode_corpus(mixed_shuffled, model_prefix, encoded)

    print("\n── Done ────────────────────────────────────────────────────────")
    print(f"  Output directory: {out_dir.resolve()}")
    print(f"  Pretraining corpus: {mixed_shuffled.name}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Balance & concatenate English + Hiligaynon corpora for pretraining."
    )
    parser.add_argument("en",  type=Path, help="English monolingual file (one sentence/line)")
    parser.add_argument("hil", type=Path, help="Hiligaynon monolingual file (one sentence/line)")
    parser.add_argument("--out",      type=Path, default=Path("corpus_out"),
                        help="Output directory (default: corpus_out/)")
    parser.add_argument("--strategy", choices=["downsample_en", "upsample_hil", "balanced"],
                        default="downsample_en",
                        help="Balancing strategy (default: downsample_en)")
    parser.add_argument("--vocab",    type=int, default=8000,
                        help="SentencePiece vocab size (default: 8000)")
    parser.add_argument("--seed",     type=int, default=42)
    parser.add_argument("--tokenize", action="store_true",
                        help="Also train a SentencePiece tokenizer and encode the corpus")
    args = parser.parse_args()

    run(
        en_path=args.en,
        hil_path=args.hil,
        out_dir=args.out,
        strategy=args.strategy,
        vocab_size=args.vocab,
        seed=args.seed,
        tokenize=args.tokenize,
    )