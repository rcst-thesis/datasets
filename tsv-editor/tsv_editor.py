"""
TSV Editor — Flask web app for viewing and editing parallel-corpus TSV files.
Run:  python tsv_editor.py
Then open http://localhost:5000
"""

import csv
import os
import re
import unicodedata
from pathlib import Path
from flask import Flask, jsonify, render_template_string, request

BASE_DIR  = Path(__file__).parent
SPELL_DIR = BASE_DIR / "spell-checker"
app = Flask(__name__)

# ── Supabase / database ───────────────────────────────────────────────────────
_sb_instance = None
DB_OK        = False

try:
    from supabase import create_client as _sb_create
    _SB_PKG = True
except ImportError:
    _SB_PKG = False
    print("[warn] supabase not installed — pip install supabase")

def _init_db() -> bool:
    global _sb_instance, DB_OK
    if not _SB_PKG:
        return False
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_KEY", "").strip()
    if not url or not key:
        print("[info] SUPABASE_URL / SUPABASE_KEY not set — DB mode disabled")
        return False
    try:
        _sb_instance = _sb_create(url, key)
        DB_OK = True
        print(f"[info] Supabase connected → {url}")
        return True
    except Exception as e:
        print(f"[warn] Supabase init failed: {e}")
        return False

_init_db()

def sb():
    if not DB_OK:
        raise RuntimeError("Database not configured. Set SUPABASE_URL and SUPABASE_KEY, then restart.")
    return _sb_instance

def _db_err():
    """Return 503 JSON if DB unavailable, else None."""
    if not DB_OK:
        return jsonify({"error": "Database not configured — set SUPABASE_URL and SUPABASE_KEY"}), 503
    return None

# ── load cleaner ──────────────────────────────────────────────────────────────
import importlib.util as _ilu
_clean_spec = _ilu.spec_from_file_location("clean", BASE_DIR / "clean.py")
_clean_mod  = _ilu.module_from_spec(_clean_spec)
try:
    _clean_spec.loader.exec_module(_clean_mod)
    CLEANER_OK = True
except Exception as _e:
    CLEANER_OK = False
    print(f"[warn] cleaner unavailable: {_e}")

# ── helpers ──────────────────────────────────────────────────────────────────

def find_tsv_files() -> list[dict]:
    files = []
    for p in sorted(BASE_DIR.rglob("*.tsv")):
        rel = p.relative_to(BASE_DIR)
        files.append({"path": str(rel), "name": p.name, "dir": str(rel.parent)})
    return files


def safe_path(rel: str) -> Path | None:
    """Resolve a relative path; return None if it escapes BASE_DIR."""
    try:
        resolved = (BASE_DIR / rel).resolve()
        resolved.relative_to(BASE_DIR.resolve())
        return resolved
    except (ValueError, Exception):
        return None


def read_tsv(path: Path) -> tuple[list[str], list[list[str]]]:
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        rows = list(reader)
    if not rows:
        return [], []
    headers = rows[0]
    data = rows[1:]
    ncols = len(headers)
    data = [r + [""] * (ncols - len(r)) if len(r) < ncols else r[:ncols] for r in data]
    return headers, data


def write_tsv(path: Path, headers: list[str], data: list[list[str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(headers)
        writer.writerows(data)


# ── spellcheck ────────────────────────────────────────────────────────────────

def strip_diacritics(text: str) -> str:
    """Normalize Unicode diacritics (e.g. á → a, í → i)."""
    nfd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn").lower()


def build_dialect_map() -> tuple[dict[str, str], set[str]]:
    """
    Build:
      - dialect_map : {normalized_source_word → ilonggo_form}
                      Maps non-standard / non-Ilonggo forms to their
                      proper Iloilo city-dialect equivalents.
      - ilonggo_vocab : set of normalized words already in Ilonggo
                        (derived from the target_word column).

    A source word is excluded from dialect_map if it also appears as a
    proper Ilonggo word — those words are valid in the dialect even if
    they look like a non-standard form (e.g. 'daan' = old/former in Ilonggo).
    """
    raw:          dict[str, str] = {}
    ilonggo_vocab: set[str]      = set()

    for fname in ("words.csv", "verbs.csv"):
        fp = SPELL_DIR / fname
        if not fp.exists():
            continue
        with open(fp, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                base   = strip_diacritics(row["base_word"].strip())
                target = row["target_word"].strip()
                if base and target:
                    raw[base] = target
                    # Every token in target is a confirmed Ilonggo word
                    for tok in re.split(r"\s+", target.lower()):
                        if tok:
                            ilonggo_vocab.add(strip_diacritics(tok))

    # Keep only entries where the source word is NOT already a proper Ilonggo word
    dialect_map = {
        base: target
        for base, target in raw.items()
        if base != strip_diacritics(target)    # skip identical pairs (mga→mga)
        and base not in ilonggo_vocab          # skip words native to Ilonggo (daan, etc.)
    }
    return dialect_map, ilonggo_vocab


# Load once at startup
DIALECT_MAP, ILONGGO_VOCAB = build_dialect_map()

# ── custom user dictionary ────────────────────────────────────────────────────
CUSTOM_DICT_FILE = SPELL_DIR / "custom_words.csv"


def load_custom_dict() -> set[str]:
    """Load user-added words that should not be flagged (normalized, no diacritics)."""
    if not CUSTOM_DICT_FILE.exists():
        return set()
    with open(CUSTOM_DICT_FILE, encoding="utf-8", newline="") as f:
        return {strip_diacritics(row["word"].strip())
                for row in csv.DictReader(f) if row.get("word", "").strip()}


def save_custom_dict(words: set[str]) -> None:
    with open(CUSTOM_DICT_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["word"])
        w.writeheader()
        for word in sorted(words):
            w.writerow({"word": word})


CUSTOM_DICT: set[str] = load_custom_dict()


# ── English grammar (LanguageTool) ───────────────────────────────────────────
try:
    import language_tool_python as _lt_mod
    _EN_TOOL      = None   # lazy-init on first request
    EN_GRAMMAR_OK = True
except ImportError:
    EN_GRAMMAR_OK = False
    print("[warn] language_tool_python not installed — pip install language_tool_python")


def _get_en_tool():
    global _EN_TOOL
    if _EN_TOOL is None:
        _EN_TOOL = _lt_mod.LanguageTool('en-US')
    return _EN_TOOL


def en_grammar_check(text: str) -> list[dict]:
    """Run LanguageTool on English text. Returns list of issue dicts."""
    if not text.strip():
        return []
    tool = _get_en_tool()
    issues = []
    for m in tool.check(text):
        issues.append({
            "start":        m.offset,
            "end":          m.offset + m.error_length,
            "message":      m.message,
            "replacements": list(m.replacements)[:3],
            "rule_id":      m.rule_id,
            "category":     m.category,
        })
    return issues


# ── grammar checker ───────────────────────────────────────────────────────────

def load_grammar_phrases() -> list:
    """Load phrase-level patterns from spell-checker/phrases.csv."""
    fp = SPELL_DIR / "phrases.csv"
    if not fp.exists():
        return []
    phrases = []
    with open(fp, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pat = row["pattern"].strip()
            rep = row["replacement"].strip()
            if pat and rep:
                phrases.append((re.compile(pat, re.IGNORECASE), rep))
    return phrases


HIL_PHRASES = load_grammar_phrases()


def grammar_correct(text: str) -> str:
    """
    Apply the full Hiligaynon→Ilonggo normalization pipeline
    (phrase + word + sentence rules).
    Does NOT apply letter-shift rules (those are handled by the converter).
    Uses DIALECT_MAP which already excludes words native to Ilonggo.
    """
    if not text.strip():
        return text

    # Phase 1: multi-word phrase patterns (highest priority)
    for pattern, replacement in HIL_PHRASES:
        def _repl(m, r=replacement):
            return (r[0].upper() + r[1:]) if m.group(0) and m.group(0)[0].isupper() else r
        text = pattern.sub(_repl, text)

    # Phase 2: word-level corrections
    def _word(m):
        word   = m.group(0)
        target = DIALECT_MAP.get(strip_diacritics(word.lower()))
        if not target:
            return word
        return (target[0].upper() + target[1:]) if word[0].isupper() else target
    text = re.sub(r"[\w'-]+", _word, text)

    # Phase 3: sentence structure rules
    # ay-inversion: "Si/Ang X ay Y" → "Si/Ang X Y"
    text = re.sub(
        r"\b((?:si|ang)\s+[\w\s]+?)\s+ay\s+", r"\1 ", text, flags=re.IGNORECASE
    )
    # ba → bala (standalone particle)
    text = re.sub(r"\bba\b", "bala", text, flags=re.IGNORECASE)

    return re.sub(r"\s+", " ", text).strip()


def grammar_check_sentence(row_idx: int, text: str) -> dict | None:
    """
    Return a correction dict if the text can be improved, else None.
    Diff marks tokens in the corrected sentence that were NOT present
    in the original (set-based, so positional shifts don't cause false marks).
    """
    corrected = grammar_correct(text)
    if corrected == text:
        return None

    corr_set  = {strip_diacritics(t) for t in re.split(r"\W+", corrected.lower()) if t}
    orig_set  = {strip_diacritics(t) for t in re.split(r"\W+", text.lower()) if t}

    # diff: tokens in the corrected sentence that are new (for sidebar display)
    corr_toks = corrected.split()
    diff = [
        {
            "token":   tok,
            "changed": strip_diacritics(re.sub(r"\W+", "", tok.lower())) not in orig_set,
        }
        for tok in corr_toks
    ]

    # orig_diff: tokens in the ORIGINAL sentence that will change (for cell highlighting)
    orig_diff = [
        {
            "token":   tok,
            "changed": strip_diacritics(re.sub(r"\W+", "", tok.lower())) not in corr_set,
        }
        for tok in text.split()
    ]

    return {
        "row":       row_idx,
        "original":  text,
        "corrected": corrected,
        "diff":      diff,
        "orig_diff": orig_diff,
    }


def spellcheck_text(text: str) -> list[dict]:
    """
    Check `text` for non-Ilonggo words and suggest their proper Ilonggo forms.
    Returns list of {word, start, end, suggestion, type}.
    """
    issues = []
    for m in re.finditer(r"[\w'-]+", text):
        word = m.group()
        # Skip numbers, very short tokens, and proper nouns (capitalized)
        if word.isdigit() or len(word) < 2 or word[0].isupper():
            continue
        norm = strip_diacritics(word)
        if norm in CUSTOM_DICT:
            continue
        if norm in DIALECT_MAP:
            issues.append({
                "word":       word,
                "start":      m.start(),
                "end":        m.end(),
                "suggestion": DIALECT_MAP[norm],
                "type":       "dialect",
            })
    return issues


# ── routes ───────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.get("/api/files")
def api_files():
    return jsonify(find_tsv_files())


@app.get("/api/tsv")
def api_get_tsv():
    rel  = request.args.get("path", "")
    path = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404
    headers, data = read_tsv(path)
    return jsonify({"headers": headers, "rows": data, "total": len(data)})


@app.post("/api/tsv/cell")
def api_save_cell():
    body = request.json
    rel  = body.get("path", "")
    row  = body.get("row")
    col  = body.get("col")
    val  = body.get("value", "")
    path = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404
    headers, data = read_tsv(path)
    if row < 0 or row >= len(data) or col < 0 or col >= len(headers):
        return jsonify({"error": "Index out of range"}), 400
    data[row][col] = val
    write_tsv(path, headers, data)
    return jsonify({"ok": True})


@app.post("/api/tsv/row/delete")
def api_delete_row():
    body = request.json
    rel  = body.get("path", "")
    row  = body.get("row")
    path = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404
    headers, data = read_tsv(path)
    if row < 0 or row >= len(data):
        return jsonify({"error": "Index out of range"}), 400
    data.pop(row)
    write_tsv(path, headers, data)
    return jsonify({"ok": True, "total": len(data)})


@app.post("/api/tsv/rows/delete")
def api_delete_rows():
    body = request.json
    rel  = body.get("path", "")
    rows = body.get("rows", [])
    path = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404
    headers, data = read_tsv(path)
    rows_set = set(rows)
    data = [r for i, r in enumerate(data) if i not in rows_set]
    write_tsv(path, headers, data)
    return jsonify({"ok": True, "total": len(data)})


@app.post("/api/tsv/rows/move")
def api_move_rows():
    body      = request.json
    rel       = body.get("path", "")
    rows      = sorted(body.get("rows", []))
    direction = body.get("direction", "up")
    path      = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404
    headers, data = read_tsv(path)
    rows_set = set(rows)
    if direction == "up":
        for i in rows:
            if i > 0 and (i - 1) not in rows_set:
                data[i], data[i - 1] = data[i - 1], data[i]
    else:
        for i in reversed(rows):
            if i < len(data) - 1 and (i + 1) not in rows_set:
                data[i], data[i + 1] = data[i + 1], data[i]
    write_tsv(path, headers, data)
    return jsonify({"ok": True})


@app.post("/api/tsv/row/insert")
def api_insert_row():
    body   = request.json
    rel    = body.get("path", "")
    after  = body.get("after", -1)   # -1 means prepend
    path   = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404
    headers, data = read_tsv(path)
    insert_at = max(0, min(after + 1, len(data)))
    data.insert(insert_at, [""] * len(headers))
    write_tsv(path, headers, data)
    return jsonify({"ok": True, "total": len(data), "row": insert_at})


@app.post("/api/tsv/row/add")
def api_add_row():
    body = request.json
    rel  = body.get("path", "")
    path = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404
    headers, data = read_tsv(path)
    data.append([""] * len(headers))
    write_tsv(path, headers, data)
    return jsonify({"ok": True, "total": len(data), "row": len(data) - 1})


@app.post("/api/spellcheck/batch")
def api_spellcheck_batch():
    """
    Check a batch of rows (current page only).
    Body: { rows: [{row: int, text: str}, ...] }
    """
    body = request.json or {}
    rows = body.get("rows", [])
    results = []
    for item in rows:
        issues = spellcheck_text(item.get("text", ""))
        if issues:
            results.append({"row": item["row"], "issues": issues})
    return jsonify({"issues": results})


@app.post("/api/grammar/batch")
def api_grammar_batch():
    """
    Run grammar correction on a batch of rows.
    Body: { rows: [{row: int, text: str}, ...] }
    Returns { corrections: [{row, original, corrected, diff}, ...] }
    """
    body = request.json or {}
    rows = body.get("rows", [])
    results = []
    for item in rows:
        result = grammar_check_sentence(item["row"], item.get("text", ""))
        if result:
            results.append(result)
    return jsonify({"corrections": results})


@app.post("/api/en-grammar/batch")
def api_en_grammar_batch():
    """
    Run LanguageTool English grammar check on a batch of rows.
    Body: { rows: [{row: int, text: str}, ...] }
    Returns { issues: [{row, issues:[{start,end,message,replacements,rule_id,category}]}] }
    """
    if not EN_GRAMMAR_OK:
        return jsonify({"error": "language_tool_python not installed — pip install language_tool_python"}), 503
    body = request.json or {}
    rows = body.get("rows", [])
    results = []
    for item in rows:
        issues = en_grammar_check(item.get("text", ""))
        if issues:
            results.append({"row": item["row"], "issues": issues})
    return jsonify({"issues": results})


@app.get("/api/dialect")
def api_get_dialect():
    """Return all dialect map entries as [{base, target, source}]."""
    entries = []
    for fname in ("words.csv", "verbs.csv"):
        fp = SPELL_DIR / fname
        if not fp.exists():
            continue
        with open(fp, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                base   = row.get("base_word", "").strip()
                target = row.get("target_word", "").strip()
                if base and target:
                    entries.append({"base": base, "target": target, "source": fname})
    entries.sort(key=lambda e: e["base"].lower())
    return jsonify({"entries": entries, "total": len(entries)})


@app.post("/api/dialect/add")
def api_add_dialect():
    """Add a base→target mapping to words.csv and reload DIALECT_MAP."""
    body   = request.json or {}
    base   = body.get("base", "").strip()
    target = body.get("target", "").strip()
    if not base or not target:
        return jsonify({"error": "base and target required"}), 400

    fp = SPELL_DIR / "words.csv"
    write_header = not fp.exists()
    with open(fp, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["base_word", "target_word"])
        if write_header:
            w.writeheader()
        w.writerow({"base_word": base, "target_word": target})

    global DIALECT_MAP, ILONGGO_VOCAB
    DIALECT_MAP, ILONGGO_VOCAB = build_dialect_map()
    return jsonify({"ok": True, "total": len(DIALECT_MAP)})


@app.post("/api/dialect/remove")
def api_remove_dialect():
    """Remove all rows where base_word matches from words.csv and verbs.csv, reload map."""
    base = strip_diacritics((request.json or {}).get("base", "").strip().lower())
    if not base:
        return jsonify({"error": "base required"}), 400

    for fname in ("words.csv", "verbs.csv"):
        fp = SPELL_DIR / fname
        if not fp.exists():
            continue
        with open(fp, encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        kept = [r for r in rows if strip_diacritics(r.get("base_word", "").strip().lower()) != base]
        if len(kept) != len(rows):
            with open(fp, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["base_word", "target_word"])
                w.writeheader()
                w.writerows(kept)

    global DIALECT_MAP, ILONGGO_VOCAB
    DIALECT_MAP, ILONGGO_VOCAB = build_dialect_map()
    return jsonify({"ok": True, "total": len(DIALECT_MAP)})


@app.get("/api/dictionary")
def api_get_dictionary():
    return jsonify({"words": sorted(CUSTOM_DICT)})


@app.post("/api/dictionary/add")
def api_add_word():
    word = (request.json or {}).get("word", "").strip()
    if not word:
        return jsonify({"error": "No word provided"}), 400
    norm = strip_diacritics(word.lower())
    CUSTOM_DICT.add(norm)
    save_custom_dict(CUSTOM_DICT)
    return jsonify({"ok": True, "word": norm, "total": len(CUSTOM_DICT)})


@app.post("/api/dictionary/remove")
def api_remove_word():
    word = (request.json or {}).get("word", "").strip()
    norm = strip_diacritics(word.lower())
    CUSTOM_DICT.discard(norm)
    save_custom_dict(CUSTOM_DICT)
    return jsonify({"ok": True, "total": len(CUSTOM_DICT)})


@app.post("/api/clean")
def api_clean():
    """
    Run clean.py's validation pipeline on a TSV file.
    Body: { path, dry_run (bool), max_tokens, min_tokens, ratio_min, ratio_max }
    Returns:
      steps          – [{step, count, type}]
      removed_rows   – [{row (0-based data idx), data: [...]}]  (up to 100)
      modified_cells – [{row, col, old, new}]
      total_removed  – int
      total_kept     – int
    When dry_run=false the file is overwritten with the cleaned data.
    """
    if not CLEANER_OK:
        return jsonify({"error": "pandas not available — pip install pandas"}), 503

    import pandas as pd

    body    = request.json or {}
    rel     = body.get("path", "")
    dry_run = body.get("dry_run", True)
    path    = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404

    headers, data = read_tsv(path)
    if not data:
        return jsonify({"steps": [], "removed_rows": [], "modified_cells": [],
                        "total_removed": 0, "total_kept": 0})

    df = pd.DataFrame(data, columns=headers)
    args = _clean_mod.Args(
        input      = path,
        output     = path,
        max_tokens = int(body.get("max_tokens", 150)),
        min_tokens = int(body.get("min_tokens", 3)),
        ratio_min  = float(body.get("ratio_min", 0.5)),
        ratio_max  = float(body.get("ratio_max", 9.0)),
    )

    # Insert a sentinel column to track original row indices through the pipeline
    df_work = df.copy()
    df_work.insert(0, "__orig", range(len(df)))

    to_delete:   set[int] = set()
    mod_cells:   list     = []
    steps_out:   list     = []

    for name, step_fn in _clean_mod.STEPS:
        df_data = df_work.drop(columns=["__orig"])

        if name == "HTML tags stripped":
            df_after, n = step_fn(df_data, args)
            # Collect cells that changed value
            for ci, col_name in enumerate(headers):
                mask = df_data.iloc[:, ci] != df_after.iloc[:, ci]
                for idx in df_data.index[mask]:
                    orig_i = int(df_work.at[idx, "__orig"])
                    if orig_i not in to_delete:
                        mod_cells.append({
                            "row": orig_i, "col": ci,
                            "old": df_data.at[idx, col_name],
                            "new": df_after.at[idx, col_name],
                        })
            # Update data columns in-place
            for ci, col_name in enumerate(headers):
                df_work[col_name] = df_after.iloc[:, ci].values
            steps_out.append({"step": name, "count": int(n), "type": "modify"})

        else:
            df_after, n = step_fn(df_data, args)
            dropped = set(df_data.index) - set(df_after.index)
            for idx in dropped:
                to_delete.add(int(df_work.at[idx, "__orig"]))
            # Keep only surviving rows (preserves __orig column)
            df_work = df_work.loc[list(df_after.index)].reset_index(drop=True)
            steps_out.append({"step": name, "count": int(n), "type": "delete"})

    removed_rows = [
        {"row": int(i), "data": list(df.iloc[i])}
        for i in sorted(to_delete)
    ]

    if not dry_run:
        # Apply HTML modifications then drop deleted rows
        df_out = df.copy()
        for mc in mod_cells:
            df_out.iat[mc["row"], mc["col"]] = mc["new"]
        df_out = df_out.drop(index=list(to_delete)).reset_index(drop=True)
        write_tsv(path, headers, df_out.values.tolist())

    return jsonify({
        "steps":          steps_out,
        "removed_rows":   removed_rows[:100],
        "modified_cells": mod_cells[:50],
        "total_removed":  len(to_delete),
        "total_kept":     len(df) - len(to_delete),
    })


# ── DB routes ────────────────────────────────────────────────────────────────

@app.get("/api/db/status")
def api_db_status():
    return jsonify({"ok": DB_OK, "pkg": _SB_PKG})


@app.get("/api/db/datasets")
def api_db_list():
    g = _db_err()
    if g: return g
    try:
        res = sb().table("datasets") \
            .select("id,name,headers,row_count,created_at") \
            .order("created_at", desc=True).execute()
        return jsonify({"datasets": res.data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/api/db/datasets")
def api_db_create():
    g = _db_err()
    if g: return g
    body    = request.json or {}
    name    = body.get("name", "").strip()
    headers = body.get("headers", [])
    if not name:
        return jsonify({"error": "name required"}), 400
    try:
        res = sb().table("datasets") \
            .insert({"name": name, "headers": headers, "row_count": 0}).execute()
        return jsonify({"dataset": res.data[0]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.patch("/api/db/datasets/<ds_id>")
def api_db_update(ds_id):
    g = _db_err()
    if g: return g
    body   = request.json or {}
    update = {k: body[k] for k in ("name", "headers") if k in body}
    if not update:
        return jsonify({"error": "nothing to update"}), 400
    try:
        res = sb().table("datasets").update(update).eq("id", ds_id).execute()
        return jsonify({"dataset": res.data[0] if res.data else {}})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.delete("/api/db/datasets/<ds_id>")
def api_db_delete(ds_id):
    g = _db_err()
    if g: return g
    try:
        sb().table("datasets").delete().eq("id", ds_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/db/datasets/<ds_id>/rows")
def api_db_rows(ds_id):
    g = _db_err()
    if g: return g
    page = max(0, int(request.args.get("page", 0)))
    size = min(max(1, int(request.args.get("size", 100))), 500)
    start, end = page * size, page * size + size - 1
    try:
        res = sb().table("corpus_rows") \
            .select("id,position,data") \
            .eq("dataset_id", ds_id) \
            .order("position") \
            .range(start, end).execute()
        return jsonify({"rows": res.data, "page": page, "size": size})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/api/db/datasets/<ds_id>/rows/save")
def api_db_save_rows(ds_id):
    """Batch upsert dirty rows. Each item: {id, position, data}."""
    g = _db_err()
    if g: return g
    rows = (request.json or {}).get("rows", [])
    if not rows:
        return jsonify({"ok": True, "count": 0})
    try:
        payload = [
            {"id": r["id"], "dataset_id": ds_id,
             "position": r["position"], "data": r["data"]}
            for r in rows
        ]
        sb().table("corpus_rows").upsert(payload, on_conflict="id").execute()
        return jsonify({"ok": True, "count": len(payload)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/api/db/datasets/<ds_id>/rows/delete")
def api_db_delete_rows(ds_id):
    g = _db_err()
    if g: return g
    ids = (request.json or {}).get("ids", [])
    if not ids:
        return jsonify({"ok": True})
    try:
        sb().table("corpus_rows").delete().in_("id", ids).execute()
        cnt = sb().table("corpus_rows").select("id", count="exact") \
            .eq("dataset_id", ds_id).execute()
        sb().table("datasets").update({"row_count": cnt.count or 0}) \
            .eq("id", ds_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/api/db/datasets/<ds_id>/import")
def api_db_import(ds_id):
    """
    Bulk insert rows parsed from TSV (already mapped to header keys client-side).
    Body: { rows: [{header: value, ...}, ...] }
    Sends in caller-defined batches; call multiple times for large files.
    """
    g = _db_err()
    if g: return g
    rows_data = (request.json or {}).get("rows", [])
    if not rows_data:
        return jsonify({"ok": True, "inserted": 0})
    try:
        pos_res = sb().table("corpus_rows") \
            .select("position").eq("dataset_id", ds_id) \
            .order("position", desc=True).limit(1).execute()
        next_pos = (pos_res.data[0]["position"] + 1) if pos_res.data else 0

        payload = [
            {"dataset_id": ds_id, "position": next_pos + i, "data": row}
            for i, row in enumerate(rows_data)
        ]
        sb().table("corpus_rows").insert(payload).execute()
        # Update cached row_count
        sb().table("datasets") \
            .update({"row_count": next_pos + len(payload)}) \
            .eq("id", ds_id).execute()
        return jsonify({"ok": True, "inserted": len(payload)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/db/datasets/<ds_id>/export")
def api_db_export(ds_id):
    """Stream entire dataset as a TSV download."""
    g = _db_err()
    if g: return g
    try:
        ds   = sb().table("datasets").select("name,headers").eq("id", ds_id).single().execute().data
        hdrs = ds.get("headers", [])
        lines = ["\t".join(hdrs)]
        offset, chunk = 0, 1000
        while True:
            res = sb().table("corpus_rows") \
                .select("data").eq("dataset_id", ds_id) \
                .order("position").range(offset, offset + chunk - 1).execute()
            for r in res.data:
                lines.append("\t".join(str(r["data"].get(h, "")) for h in hdrs))
            if len(res.data) < chunk:
                break
            offset += chunk
        fname = re.sub(r"[^\w\-.]", "_", ds["name"]) + ".tsv"
        return app.response_class(
            response="\n".join(lines),
            status=200,
            mimetype="text/tab-separated-values",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'}
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── HTML / CSS / JS (single-file SPA) ────────────────────────────────────────

HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TSV Editor</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
:root {
  --bg: #0f1117; --surface: #1a1d27; --border: #2d3148;
  --accent: #6c63ff; --accent2: #ff6584;
  --text: #e2e8f0; --muted: #718096; --danger: #fc8181;
  --row-hover: #1e2235; --edit-bg: #252a3d;
  --spell-warn: #f6ad55; --spell-fix: #68d391;
  font-size: 14px;
}
body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; height: 100dvh; display: flex; flex-direction: column; overflow: hidden; }

/* ── top bar ── */
header { display: flex; align-items: center; gap: 12px; padding: 10px 16px; background: var(--surface); border-bottom: 1px solid var(--border); flex-shrink: 0; flex-wrap: wrap; }
header h1 { font-size: 1.05rem; font-weight: 600; color: var(--accent); white-space: nowrap; }
#file-select { flex: 1; max-width: 420px; background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 6px; padding: 5px 10px; font-size: 0.9rem; }
#search-box { flex: 1; max-width: 240px; background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 6px; padding: 5px 10px; font-size: 0.9rem; }
#search-box::placeholder { color: var(--muted); }
.badge { background: var(--border); color: var(--muted); border-radius: 999px; padding: 2px 10px; font-size: 0.78rem; white-space: nowrap; }
.btn { padding: 5px 14px; border-radius: 6px; border: none; cursor: pointer; font-size: 0.85rem; font-weight: 500; transition: opacity .15s; }
.btn:hover { opacity: .8; }
.btn-primary { background: var(--accent); color: #fff; }
.btn-danger  { background: var(--danger); color: #fff; }
.btn-spell   { background: #744210; color: #fbd38d; border: 1px solid #975a16; }
.btn-spell.active { background: #975a16; }

/* ── pagination ── */
#pager { display: flex; align-items: center; gap: 8px; padding: 6px 16px; background: var(--surface); border-bottom: 1px solid var(--border); flex-shrink: 0; }
#pager span { color: var(--muted); font-size: 0.82rem; }
#pager button { background: var(--border); color: var(--text); border: none; border-radius: 5px; padding: 3px 10px; cursor: pointer; font-size: 0.82rem; }
#pager button:disabled { opacity: .35; cursor: default; }
#page-size { background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 5px; padding: 2px 6px; font-size: 0.82rem; }

/* ── main content area (table + sidebar) ── */
#main-content { flex: 1; display: flex; overflow: hidden; }

/* ── table area ── */
#table-wrap { flex: 1; overflow: auto; }
table { width: 100%; border-collapse: collapse; table-layout: fixed; }
thead th { position: sticky; top: 0; background: var(--surface); border-bottom: 2px solid var(--accent); padding: 8px 10px; text-align: left; font-weight: 600; color: var(--accent); white-space: nowrap; z-index: 2; }
thead th.col-row-num { width: 56px; color: var(--muted); font-weight: 400; }
thead th.col-cb { width: 36px; }
thead th.col-actions { width: 52px; }
tbody tr:hover { background: var(--row-hover); }
tbody tr.changed { border-left: 3px solid var(--accent2); }
tbody tr.row-selected { background: rgba(108,99,255,.18) !important; }
td { padding: 5px 10px; border-bottom: 1px solid var(--border); vertical-align: top; word-break: break-word; }
td.row-num { color: var(--muted); font-size: 0.78rem; text-align: right; user-select: none; cursor: pointer; border-radius: 4px; transition: color .15s, background .15s; }
td.row-num:hover { color: var(--accent); background: rgba(108,99,255,.12); }
td.col-cb { text-align: center; width: 36px; }
td.cell-editable { cursor: text; white-space: pre-wrap; min-height: 28px; }
td.cell-editable:focus { outline: 2px solid var(--accent); background: var(--edit-bg); border-radius: 3px; }
td.cell-editable.saving { opacity: .5; }
td.col-actions { text-align: center; }
.del-btn { background: transparent; border: none; cursor: pointer; color: var(--muted); font-size: 1rem; line-height: 1; padding: 2px 5px; border-radius: 4px; }
.del-btn:hover { color: var(--danger); background: rgba(252,129,129,.1); }
#empty-msg { padding: 60px; text-align: center; color: var(--muted); }
input[type=checkbox].row-cb { cursor: pointer; accent-color: var(--accent); width: 14px; height: 14px; }

/* ── bulk toolbar ── */
#bulk-toolbar { display: none; align-items: center; gap: 8px; padding: 5px 16px; background: rgba(108,99,255,.15); border-bottom: 1px solid var(--accent); flex-shrink: 0; }
#bulk-toolbar.visible { display: flex; }
#bulk-count { font-size: 0.82rem; color: var(--accent); font-weight: 600; white-space: nowrap; }
.btn-bulk { padding: 3px 11px; border-radius: 5px; border: 1px solid var(--border); cursor: pointer; font-size: 0.8rem; font-weight: 500; background: var(--surface); color: var(--text); transition: background .15s; white-space: nowrap; }
.btn-bulk:hover { background: var(--border); }
.btn-bulk-danger { border-color: #742a2a; color: var(--danger); }
.btn-bulk-danger:hover { background: rgba(252,129,129,.12); }
#bulk-toolbar .sep { width: 1px; height: 18px; background: var(--border); margin: 0 2px; }

/* ── spell issue highlights ── */
td.hil-cell-issue { box-shadow: inset 3px 0 0 var(--spell-warn); }
span.spell-err {
  text-decoration: underline wavy #fc8181;
  text-underline-offset: 3px;
  cursor: help;
  color: inherit;
}

/* ── spell sidebar ── */
#spell-sidebar { width: 300px; flex-shrink: 0; background: var(--surface); border-left: 1px solid var(--border); display: flex; flex-direction: column; overflow: hidden; transition: width .2s ease; }
#spell-sidebar.hidden { width: 0; border-left: none; }
#spell-sidebar-header { display: flex; align-items: center; gap: 8px; padding: 10px 12px; border-bottom: 1px solid var(--border); flex-shrink: 0; min-width: 300px; }
#spell-sidebar-header h2 { font-size: 0.85rem; font-weight: 600; color: var(--spell-warn); flex: 1; white-space: nowrap; }
#run-check-btn { padding: 3px 10px; border-radius: 5px; border: none; cursor: pointer; font-size: 0.78rem; background: rgba(246,173,85,.2); color: var(--spell-warn); font-weight: 500; white-space: nowrap; }
#run-check-btn:hover { background: rgba(246,173,85,.35); }
#run-check-btn:disabled { opacity: .4; cursor: default; }
#spell-close-btn { background: transparent; border: none; color: var(--muted); cursor: pointer; font-size: 1.1rem; line-height: 1; padding: 2px 5px; border-radius: 4px; }
#spell-close-btn:hover { color: var(--text); }
#spell-status { padding: 6px 12px; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid var(--border); flex-shrink: 0; min-width: 300px; }
#spell-sidebar-body { flex: 1; overflow-y: auto; min-width: 300px; }
#spell-issue-list { padding: 8px; }
#gram-correction-list { padding: 8px; }
#en-gram-list { padding: 8px; }
.en-card { background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; transition: border-color .15s, background .15s; }
.en-card:hover { border-color: #63b3ed; }
.en-card.active { border-color: #63b3ed; background: rgba(99,179,237,.13); box-shadow: inset 3px 0 0 #63b3ed; }
.en-card .row-ref { font-size: 0.7rem; color: var(--muted); margin-bottom: 4px; }
.en-card .en-msg { font-size: 0.82rem; color: var(--text); margin-bottom: 6px; line-height: 1.4; cursor: pointer; }
.en-card .en-msg:hover { color: #63b3ed; }
.en-card .en-orig { font-size: 0.82rem; color: var(--danger); text-decoration: line-through; margin-right: 4px; }
.en-card .en-fix  { font-size: 0.82rem; color: var(--spell-fix); font-weight: 500; }
.spell-card { background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; transition: border-color .15s, background .15s; }
.spell-card:hover { border-color: var(--spell-warn); }
.spell-card.active { border-color: var(--spell-warn); background: rgba(246,173,85,.13); box-shadow: inset 3px 0 0 var(--spell-warn); }
.spell-card .row-ref { font-size: 0.7rem; color: var(--muted); margin-bottom: 5px; }
.spell-card .issue-body { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; font-size: 0.85rem; cursor: pointer; padding: 2px 0; border-radius: 4px; }
.spell-card .issue-body:hover .word-error { text-decoration-color: #fc8181; }
@keyframes spell-flash { 0%,100%{background:transparent;outline:none} 30%{background:rgba(252,129,129,.35);outline:2px solid #fc8181;border-radius:2px} }
span.spell-err.flash { animation: spell-flash .7s ease; }
span.gram-err {
  text-decoration: underline wavy #f6ad55;
  text-underline-offset: 3px;
  cursor: help;
  color: inherit;
}
@keyframes gram-flash { 0%,100%{background:transparent;outline:none} 30%{background:rgba(246,173,85,.35);outline:2px solid #f6ad55;border-radius:2px} }
span.gram-err.flash { animation: gram-flash .7s ease; }
span.en-err {
  text-decoration: underline wavy #63b3ed;
  text-underline-offset: 3px;
  cursor: help;
  color: inherit;
}
@keyframes en-flash { 0%,100%{background:transparent;outline:none} 30%{background:rgba(99,179,237,.3);outline:2px solid #63b3ed;border-radius:2px} }
span.en-err.flash { animation: en-flash .7s ease; }
td.en-cell-issue { box-shadow: inset 3px 0 0 #63b3ed; }
@keyframes cell-glow {
  0%   { background: transparent; }
  25%  { background: rgba(108,99,255,.28); outline: 2px solid var(--accent); border-radius: 3px; }
  75%  { background: rgba(108,99,255,.18); outline: 2px solid var(--accent); border-radius: 3px; }
  100% { background: transparent; outline: none; }
}
td.cell-glow { animation: cell-glow .9s ease; }
.spell-card .word-error { color: var(--danger); text-decoration: underline wavy var(--danger); text-underline-offset: 3px; }
.spell-card .arrow { color: var(--muted); }
.spell-card .word-fix { color: var(--spell-fix); font-weight: 500; }
.spell-card .card-actions { display: flex; gap: 6px; margin-top: 8px; }
.accept-btn { flex: 1; padding: 4px 8px; border-radius: 5px; border: none; cursor: pointer; font-size: 0.75rem; font-weight: 500; background: rgba(104,211,145,.18); color: var(--spell-fix); transition: background .15s; }
.accept-btn:hover { background: rgba(104,211,145,.35); }
.reject-btn { flex: 1; padding: 4px 8px; border-radius: 5px; border: none; cursor: pointer; font-size: 0.75rem; background: var(--border); color: var(--muted); transition: color .15s; }
.reject-btn:hover { color: var(--text); }
#spell-empty { padding: 24px 16px; text-align: center; color: var(--muted); font-size: 0.82rem; }

/* ── sidebar section dividers ── */
.sidebar-section-title {
  font-size: 0.68rem; font-weight: 700; letter-spacing: .08em; text-transform: uppercase;
  color: var(--muted); padding: 10px 12px 4px; border-top: 1px solid var(--border);
  margin-top: 4px; min-width: 300px;
}
.sidebar-section-title:first-child { border-top: none; margin-top: 0; }

/* ── grammar cards ── */
.gram-card { background: var(--bg); border: 1px solid #2d4a3e; border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; transition: border-color .15s, background .15s; }
.gram-card:hover { border-color: #48bb78; }
.gram-card.active { border-color: #48bb78; background: rgba(72,187,120,.13); box-shadow: inset 3px 0 0 #48bb78; }
.gram-card .row-ref { font-size: 0.7rem; color: var(--muted); margin-bottom: 6px; }
.gram-orig { font-size: 0.78rem; color: var(--muted); margin-bottom: 4px; white-space: pre-wrap; word-break: break-word; }
.gram-orig span.gram-del { color: var(--danger); text-decoration: line-through; }
.gram-fix  { font-size: 0.85rem; color: var(--text); white-space: pre-wrap; word-break: break-word; }
.gram-fix  span.gram-ins { color: #68d391; font-weight: 500; }
.gram-card .card-actions { display: flex; gap: 6px; margin-top: 8px; }
.gram-accept-btn { flex: 1; padding: 4px 8px; border-radius: 5px; border: none; cursor: pointer; font-size: 0.75rem; font-weight: 500; background: rgba(72,187,120,.18); color: #68d391; transition: background .15s; }
.gram-accept-btn:hover { background: rgba(72,187,120,.35); }
.gram-reject-btn { flex: 1; padding: 4px 8px; border-radius: 5px; border: none; cursor: pointer; font-size: 0.75rem; background: var(--border); color: var(--muted); transition: color .15s; }
.gram-reject-btn:hover { color: var(--text); }

/* ── clean modal ── */
#clean-backdrop { display:none; position:fixed; inset:0; background:rgba(0,0,0,.6); backdrop-filter:blur(3px); z-index:300; align-items:center; justify-content:center; }
#clean-backdrop.open { display:flex; }
#clean-modal { background:var(--surface); border:1px solid var(--border); border-radius:12px; width:min(640px,94vw); max-height:85vh; display:flex; flex-direction:column; box-shadow:0 24px 64px rgba(0,0,0,.6); }
#clean-modal-header { display:flex; align-items:center; padding:14px 18px; border-bottom:1px solid var(--border); flex-shrink:0; }
#clean-modal-header h2 { flex:1; font-size:1rem; font-weight:600; color:var(--accent); }
#clean-modal-close { background:transparent; border:none; color:var(--muted); font-size:1.3rem; cursor:pointer; padding:2px 6px; border-radius:4px; }
#clean-modal-close:hover { color:var(--text); }
#clean-modal-body { overflow-y:auto; padding:16px 18px; flex:1; display:flex; flex-direction:column; gap:12px; }
.clean-step-row { display:flex; align-items:center; gap:10px; font-size:0.85rem; padding:6px 10px; border-radius:6px; background:var(--bg); }
.clean-step-name { flex:1; color:var(--text); }
.clean-step-count { font-weight:600; }
.clean-step-count.zero  { color:var(--spell-fix); }
.clean-step-count.nonzero { color:var(--spell-warn); }
.clean-step-type { font-size:0.72rem; color:var(--muted); background:var(--border); border-radius:4px; padding:1px 6px; }
#clean-removed-list { display:flex; flex-direction:column; gap:4px; }
.clean-removed-row { font-size:0.78rem; background:rgba(252,129,129,.07); border:1px solid rgba(252,129,129,.2); border-radius:6px; padding:6px 10px; }
.clean-removed-row .clean-row-num { color:var(--muted); font-size:0.7rem; margin-bottom:3px; }
.clean-removed-row .clean-row-data { color:var(--danger); word-break:break-all; }
.clean-modified-row { font-size:0.78rem; background:rgba(246,173,85,.07); border:1px solid rgba(246,173,85,.2); border-radius:6px; padding:6px 10px; }
.clean-modified-row .clean-row-num { color:var(--muted); font-size:0.7rem; margin-bottom:3px; }
.clean-section-title { font-size:0.72rem; font-weight:700; letter-spacing:.07em; text-transform:uppercase; color:var(--muted); }
#clean-modal-footer { display:flex; align-items:center; gap:10px; padding:12px 18px; border-top:1px solid var(--border); flex-shrink:0; }
#clean-modal-status { flex:1; font-size:0.82rem; color:var(--muted); }
.btn-clean { background:#744210; color:#fbd38d; border:1px solid #975a16; }
.btn-clean:hover { opacity:.85; }

/* ── row modal ── */
#modal-backdrop { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.6); backdrop-filter: blur(3px); z-index: 200; align-items: center; justify-content: center; }
#modal-backdrop.open { display: flex; }
#modal { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; width: min(680px, 94vw); max-height: 88vh; display: flex; flex-direction: column; box-shadow: 0 24px 64px rgba(0,0,0,.6); }
#modal-header { display: flex; align-items: center; gap: 10px; padding: 14px 18px; border-bottom: 1px solid var(--border); flex-shrink: 0; }
#modal-title { font-weight: 600; color: var(--accent); flex: 1; }
#modal-nav { display: flex; align-items: center; gap: 6px; }
#modal-nav button { background: var(--border); color: var(--text); border: none; border-radius: 5px; padding: 3px 10px; cursor: pointer; font-size: 0.82rem; }
#modal-nav button:disabled { opacity: .35; cursor: default; }
#modal-nav span { color: var(--muted); font-size: 0.82rem; min-width: 70px; text-align: center; }
#modal-close { background: transparent; border: none; color: var(--muted); font-size: 1.3rem; cursor: pointer; line-height: 1; padding: 2px 6px; border-radius: 4px; }
#modal-close:hover { color: var(--text); }
#modal-body { overflow-y: auto; padding: 18px; display: flex; flex-direction: column; gap: 16px; flex: 1; }
.modal-field label { display: block; font-size: 0.78rem; font-weight: 600; color: var(--accent); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
.modal-field textarea { width: 100%; background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 6px; padding: 10px 12px; font-size: 0.95rem; font-family: inherit; line-height: 1.6; resize: vertical; min-height: 80px; transition: border-color .15s; }
.modal-field textarea:focus { outline: none; border-color: var(--accent); }
#modal-footer { display: flex; align-items: center; justify-content: flex-end; gap: 10px; padding: 12px 18px; border-top: 1px solid var(--border); flex-shrink: 0; }
#modal-status { color: var(--muted); font-size: 0.82rem; flex: 1; }

/* ── save indicator ── */
#save-indicator { font-size: 0.78rem; padding: 3px 10px; border-radius: 999px; white-space: nowrap; transition: background .2s, color .2s; }
#save-indicator.saved    { background: rgba(104,211,145,.15); color: #68d391; }
#save-indicator.saving   { background: rgba(246,173,85,.15);  color: #f6ad55; }
#save-indicator.unsaved  { background: rgba(252,129,129,.15); color: var(--danger); }

/* ── find/replace panel ── */
#find-panel { display: none; flex-shrink: 0; background: var(--surface); border-bottom: 1px solid var(--border); padding: 6px 14px; gap: 8px; align-items: center; flex-wrap: wrap; }
#find-panel.open { display: flex; }
#find-panel input { background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 5px; padding: 4px 9px; font-size: 0.85rem; min-width: 200px; }
#find-panel input:focus { outline: none; border-color: var(--accent); }
#find-panel input.has-error { border-color: var(--danger); }
.find-btn { padding: 3px 10px; border-radius: 5px; border: 1px solid var(--border); background: var(--bg); color: var(--text); cursor: pointer; font-size: 0.8rem; white-space: nowrap; }
.find-btn:hover { background: var(--border); }
.find-btn.active { background: var(--accent); color: #fff; border-color: var(--accent); }
#find-match-count { font-size: 0.78rem; color: var(--muted); white-space: nowrap; min-width: 70px; }
#find-close-btn { background: transparent; border: none; color: var(--muted); cursor: pointer; font-size: 1.1rem; line-height: 1; padding: 2px 5px; margin-left: auto; }
#find-close-btn:hover { color: var(--text); }
.find-sep { width: 1px; height: 18px; background: var(--border); }
td.find-match { background: rgba(246,173,85,.25) !important; outline: 1px solid #f6ad55; }
td.find-match-active { background: rgba(108,99,255,.35) !important; outline: 2px solid var(--accent); }

/* ── settings modal ── */
#settings-backdrop { display:none; position:fixed; inset:0; background:rgba(0,0,0,.6); backdrop-filter:blur(3px); z-index:400; align-items:flex-start; justify-content:center; padding-top:5vh; }
#settings-backdrop.open { display:flex; }
#settings-modal { background:var(--surface); border:1px solid var(--border); border-radius:12px; width:min(640px,95vw); max-height:88vh; display:flex; flex-direction:column; box-shadow:0 24px 64px rgba(0,0,0,.6); }
#settings-header { display:flex; align-items:center; gap:10px; padding:14px 18px; border-bottom:1px solid var(--border); flex-shrink:0; }
#settings-header h2 { font-size:1rem; font-weight:600; color:var(--accent); flex:1; }
#settings-close { background:transparent; border:none; color:var(--muted); font-size:1.3rem; cursor:pointer; line-height:1; padding:2px 6px; border-radius:4px; }
#settings-close:hover { color:var(--text); }
#settings-tabs { display:flex; gap:2px; padding:0 18px; border-bottom:1px solid var(--border); flex-shrink:0; }
.settings-tab { padding:9px 16px; font-size:0.85rem; font-weight:500; color:var(--muted); cursor:pointer; border:none; background:transparent; border-bottom:2px solid transparent; margin-bottom:-1px; transition:color .15s, border-color .15s; }
.settings-tab:hover { color:var(--text); }
.settings-tab.active { color:var(--accent); border-bottom-color:var(--accent); }
#settings-body { overflow-y:auto; padding:18px; flex:1; display:flex; flex-direction:column; gap:16px; }
.settings-panel { display:none; flex-direction:column; gap:14px; }
.settings-panel.active { display:flex; }
.settings-section h3 { font-size:0.78rem; font-weight:700; text-transform:uppercase; letter-spacing:.07em; color:var(--muted); margin-bottom:8px; }
.settings-section p { font-size:0.82rem; color:var(--muted); margin-bottom:10px; line-height:1.5; }
#dict-add-row { display:flex; gap:8px; margin-bottom:12px; }
#dict-add-input { flex:1; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:6px 10px; font-size:0.88rem; }
#dict-add-input:focus { outline:none; border-color:var(--accent); }
#dict-add-btn { padding:6px 14px; border-radius:6px; border:none; cursor:pointer; font-size:0.85rem; font-weight:500; background:var(--accent); color:#fff; white-space:nowrap; }
#dict-add-btn:hover { opacity:.85; }
#dict-list { display:flex; flex-direction:column; gap:4px; max-height:340px; overflow-y:auto; }
.dict-item { display:flex; align-items:center; gap:8px; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:6px; font-size:0.88rem; }
.dict-item span { flex:1; font-family:monospace; color:var(--text); }
.dict-item button { background:transparent; border:none; color:var(--muted); cursor:pointer; font-size:1rem; line-height:1; padding:1px 5px; border-radius:3px; }
.dict-item button:hover { color:var(--danger); background:rgba(252,129,129,.1); }
#dict-empty { padding:16px; text-align:center; color:var(--muted); font-size:0.82rem; }
#dict-stats { font-size:0.75rem; color:var(--muted); margin-top:6px; }
.btn-dict { flex:1; padding:4px 8px; border-radius:5px; border:1px solid rgba(99,179,237,.4); cursor:pointer; font-size:0.72rem; font-weight:500; background:rgba(99,179,237,.1); color:#63b3ed; transition:background .15s; white-space:nowrap; }
.btn-dict:hover { background:rgba(99,179,237,.25); }
#dialect-filter { width:100%; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:6px 10px; font-size:0.85rem; margin-bottom:10px; }
#dialect-filter:focus { outline:none; border-color:var(--accent); }
#dialect-add-grid { display:grid; grid-template-columns:1fr 1fr auto; gap:6px; margin-bottom:10px; }
#dialect-add-grid input { background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:6px 10px; font-size:0.85rem; }
#dialect-add-grid input:focus { outline:none; border-color:var(--accent); }
#dialect-add-grid button { padding:6px 14px; border-radius:6px; border:none; cursor:pointer; font-size:0.85rem; font-weight:500; background:var(--accent); color:#fff; white-space:nowrap; }
#dialect-add-grid button:hover { opacity:.85; }
#dialect-list { display:flex; flex-direction:column; gap:3px; max-height:340px; overflow-y:auto; }
.dialect-item { display:grid; grid-template-columns:1fr 1fr auto auto; align-items:center; gap:6px; padding:5px 10px; background:var(--bg); border:1px solid var(--border); border-radius:6px; font-size:0.83rem; }
.dialect-item .di-base { font-family:monospace; color:var(--danger); }
.dialect-item .di-arr  { color:var(--muted); text-align:center; }
.dialect-item .di-tgt  { font-family:monospace; color:var(--spell-fix); }
.dialect-item .di-src  { font-size:0.68rem; color:var(--muted); text-align:right; white-space:nowrap; }
.dialect-item button   { background:transparent; border:none; color:var(--muted); cursor:pointer; font-size:0.95rem; padding:1px 5px; border-radius:3px; }
.dialect-item button:hover { color:var(--danger); background:rgba(252,129,129,.1); }
#dialect-stats-line { font-size:0.75rem; color:var(--muted); margin-top:4px; }

/* ── toast ── */
.toast { position: fixed; bottom: 20px; right: 20px; background: #2d3748; color: #fff; padding: 10px 18px; border-radius: 8px; font-size: 0.85rem; opacity: 0; transition: opacity .25s; pointer-events: none; z-index: 100; }
.toast.show { opacity: 1; }

/* ── DB mode ── */
.btn-db { background:#1a3a5c; color:#63b3ed; border:1px solid #2b6cb0; }
.btn-db.active { background:#2b6cb0; color:#fff; border-color:#3182ce; }
#db-controls { display:none; align-items:center; gap:8px; flex-wrap:wrap; }
#db-controls.show { display:flex; }
#dataset-select { flex:1; max-width:340px; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:5px 10px; font-size:0.9rem; }
#db-unsaved { font-size:0.78rem; padding:3px 10px; border-radius:999px; white-space:nowrap; display:none; }
#db-unsaved.show { display:inline; background:rgba(252,129,129,.15); color:var(--danger); }

/* ── import modal ── */
#import-backdrop { display:none; position:fixed; inset:0; background:rgba(0,0,0,.65); backdrop-filter:blur(3px); z-index:350; align-items:center; justify-content:center; padding:16px; }
#import-backdrop.open { display:flex; }
#import-modal { background:var(--surface); border:1px solid var(--border); border-radius:12px; width:min(820px,100%); max-height:90vh; display:flex; flex-direction:column; box-shadow:0 24px 64px rgba(0,0,0,.6); }
#import-header { display:flex; align-items:center; gap:10px; padding:14px 18px; border-bottom:1px solid var(--border); flex-shrink:0; }
#import-header h2 { font-size:1rem; font-weight:600; color:var(--accent); flex:1; }
#import-close { background:transparent; border:none; color:var(--muted); font-size:1.3rem; cursor:pointer; line-height:1; padding:2px 6px; }
#import-close:hover { color:var(--text); }
#import-body { flex:1; overflow-y:auto; padding:18px; display:flex; flex-direction:column; gap:14px; }
#import-footer { display:flex; align-items:center; gap:10px; padding:12px 18px; border-top:1px solid var(--border); flex-shrink:0; }
#import-status { flex:1; font-size:0.82rem; color:var(--muted); }
#import-progress-bar { width:100%; height:6px; background:var(--border); border-radius:3px; overflow:hidden; display:none; }
#import-progress-bar.show { display:block; }
#import-progress-fill { height:100%; background:var(--accent); border-radius:3px; transition:width .2s; width:0%; }
#drop-zone { border:2px dashed var(--border); border-radius:10px; padding:32px; text-align:center; color:var(--muted); cursor:pointer; transition:border-color .15s, background .15s; }
#drop-zone:hover, #drop-zone.drag-over { border-color:var(--accent); background:rgba(108,99,255,.06); color:var(--text); }
#drop-zone p { font-size:0.88rem; margin-top:6px; }
#import-options { display:flex; align-items:center; gap:16px; flex-wrap:wrap; font-size:0.85rem; color:var(--text); }
#import-options label { display:flex; align-items:center; gap:6px; cursor:pointer; }

/* column mapping table */
#col-map-wrap { overflow-x:auto; }
#col-map-table { width:100%; border-collapse:collapse; font-size:0.83rem; }
#col-map-table th { background:var(--surface); padding:7px 10px; text-align:left; border-bottom:2px solid var(--accent); color:var(--accent); white-space:nowrap; font-weight:600; }
#col-map-table td { padding:6px 10px; border-bottom:1px solid var(--border); vertical-align:top; }
#col-map-table td.sample-cell { color:var(--muted); font-size:0.78rem; max-width:160px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.col-map-select { background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:5px; padding:4px 8px; font-size:0.82rem; width:100%; }
.col-map-select:focus { outline:none; border-color:var(--accent); }

/* ── header-config modal ── */
#hdr-backdrop { display:none; position:fixed; inset:0; background:rgba(0,0,0,.65); backdrop-filter:blur(3px); z-index:360; align-items:center; justify-content:center; padding:16px; }
#hdr-backdrop.open { display:flex; }
#hdr-modal { background:var(--surface); border:1px solid var(--border); border-radius:12px; width:min(480px,100%); display:flex; flex-direction:column; box-shadow:0 24px 64px rgba(0,0,0,.6); max-height:90vh; }
#hdr-modal-header { display:flex; align-items:center; padding:14px 18px; border-bottom:1px solid var(--border); }
#hdr-modal-header h2 { font-size:1rem; font-weight:600; color:var(--accent); flex:1; }
#hdr-modal-body { padding:18px; overflow-y:auto; }
#hdr-list { display:flex; flex-direction:column; gap:6px; margin-bottom:12px; }
.hdr-row { display:flex; align-items:center; gap:6px; }
.hdr-row input { flex:1; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:6px 10px; font-size:0.88rem; }
.hdr-row input:focus { outline:none; border-color:var(--accent); }
.hdr-row button { background:transparent; border:none; color:var(--muted); cursor:pointer; font-size:1rem; padding:2px 5px; border-radius:3px; }
.hdr-row button:hover { color:var(--danger); background:rgba(252,129,129,.1); }
#hdr-add-btn { background:transparent; border:1px dashed var(--border); color:var(--muted); border-radius:6px; padding:6px 12px; cursor:pointer; font-size:0.82rem; width:100%; }
#hdr-add-btn:hover { border-color:var(--accent); color:var(--accent); }
#hdr-modal-footer { display:flex; gap:8px; padding:12px 18px; border-top:1px solid var(--border); justify-content:flex-end; }

/* ── mobile overlay backdrop (shown when sidebar is open) ── */
#sidebar-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.45); z-index: 140; }
#sidebar-overlay.visible { display: block; }

/* ── responsive ── */
@media (max-width: 768px) {
  /* header — file select takes full row, controls wrap below */
  header { padding: 8px 12px; gap: 7px; }
  header h1 { font-size: 0.92rem; }
  #file-select { flex-basis: 100%; max-width: 100%; order: 2; }
  #search-box  { flex: 1; max-width: none; order: 3; min-width: 0; }
  #row-count, #add-row-btn, #spell-toggle-btn, #save-indicator, #settings-btn { order: 4; }
  #save-indicator { font-size: 0.72rem; padding: 2px 7px; }

  /* pager — hide verbose label, keep buttons tight */
  #pager { padding: 5px 10px; gap: 6px; flex-wrap: wrap; }
  #pager > span[style] { display: none; }

  /* bulk toolbar — horizontal scroll, no wrap */
  #bulk-toolbar { overflow-x: auto; flex-wrap: nowrap; padding: 5px 10px; scrollbar-width: none; }
  #bulk-toolbar::-webkit-scrollbar { display: none; }

  /* find panel */
  #find-panel { flex-wrap: wrap; gap: 6px; }
  #find-panel input { min-width: 0; flex: 1 1 140px; }

  /* table — let it scroll horizontally, tighten cells */
  #table-wrap { overflow-x: auto; }
  table { table-layout: auto; min-width: 480px; }
  td, thead th { padding: 4px 7px; font-size: 0.8rem; }
  thead th.col-row-num { width: 40px; }
  thead th.col-cb { width: 28px; }
  thead th.col-actions { width: 40px; }

  /* sidebar — slide-over overlay instead of side-by-side */
  #spell-sidebar {
    position: fixed;
    top: 0; right: 0; bottom: 0;
    z-index: 150;
    width: min(88vw, 320px) !important;
    border-left: 1px solid var(--border);
    box-shadow: -6px 0 32px rgba(0,0,0,.5);
    transform: translateX(110%);
    transition: transform .25s ease;
  }
  #spell-sidebar.hidden { transform: translateX(110%); width: min(88vw, 320px) !important; border-left: 1px solid var(--border); }
  #spell-sidebar:not(.hidden) { transform: translateX(0); }
  #spell-sidebar-header { min-width: 0; }
  #spell-status, #spell-sidebar-body { min-width: 0; }

  /* modals — bottom sheet style */
  #modal-backdrop, #settings-backdrop, #clean-backdrop {
    padding-top: 0;
    align-items: flex-end;
  }
  #modal, #settings-modal {
    width: 100%;
    max-height: 92dvh;
    border-radius: 14px 14px 0 0;
  }
  #settings-modal { margin: 0; }

  /* settings dialect grid — stack */
  #dialect-add-grid { grid-template-columns: 1fr 1fr; }
  #dialect-add-grid button { grid-column: span 2; }

  /* dict add row */
  #dict-add-row { flex-wrap: wrap; }
  #dict-add-input { flex: 1 1 140px; }
}
</style>
</head>
<body>

<header>
  <h1>TSV Editor</h1>
  <!-- file mode controls -->
  <select id="file-select"><option value="">— select a file —</option></select>
  <!-- db mode controls -->
  <span id="db-controls">
    <select id="dataset-select"><option value="">— select dataset —</option></select>
    <button class="btn" id="new-dataset-btn" style="background:var(--border);color:var(--muted)" title="New dataset">+ Dataset</button>
    <button class="btn" id="import-btn" style="background:var(--border);color:var(--muted)" title="Import TSV">📥 Import</button>
    <button class="btn" id="export-btn" style="background:var(--border);color:var(--muted)" title="Export TSV" disabled>📤 Export</button>
    <span id="db-unsaved" title="Unsaved changes — Ctrl+S to sync">● Unsaved</span>
  </span>
  <button class="btn btn-db" id="db-mode-btn" title="Switch to cloud database mode">☁ DB</button>
  <input id="search-box" type="search" placeholder="Search rows…">
  <span id="row-count" class="badge">0 rows</span>
  <button class="btn btn-primary" id="add-row-btn" disabled>+ Row</button>
  <button class="btn btn-spell" id="spell-toggle-btn">Spell Check</button>
  <span id="save-indicator" class="saved">● Saved</span>
  <button class="btn" id="settings-btn" style="background:var(--border);color:var(--muted)" title="Settings / Dictionary">⚙ Settings</button>
</header>

<div id="find-panel">
  <span style="font-size:0.78rem;color:var(--muted);white-space:nowrap">Find</span>
  <input id="find-input" type="text" placeholder="Search…" autocomplete="off">
  <button class="find-btn" id="find-regex-btn" title="Toggle regular expressions">.*</button>
  <button class="find-btn" id="find-case-btn" title="Toggle case sensitive">Aa</button>
  <span id="find-match-count">No results</span>
  <button class="find-btn" id="find-prev-btn" title="Previous match">‹</button>
  <button class="find-btn" id="find-next-btn" title="Next match">›</button>
  <div class="find-sep"></div>
  <input id="replace-input" type="text" placeholder="Replace…" autocomplete="off" style="display:none">
  <button class="find-btn" id="replace-one-btn" style="display:none">Replace</button>
  <button class="find-btn" id="replace-all-btn" style="display:none">Replace All</button>
  <button class="find-btn" id="find-mode-toggle" title="Toggle replace mode">⇄</button>
  <button id="find-close-btn" title="Close (Esc)">✕</button>
</div>

<div id="pager">
  <button id="prev-btn" disabled>‹ Prev</button>
  <span id="page-info">Page 1 / 1</span>
  <button id="next-btn" disabled>Next ›</button>
  <span style="margin-left:8px;color:var(--muted)">Rows per page:</span>
  <select id="page-size">
    <option value="50">50</option>
    <option value="100" selected>100</option>
    <option value="200">200</option>
    <option value="500">500</option>
  </select>
</div>

<div id="bulk-toolbar">
  <span id="bulk-count">0 selected</span>
  <div class="sep"></div>
  <button class="btn-bulk" id="bulk-move-up-btn" title="Move rows up">↑ Move Up</button>
  <button class="btn-bulk" id="bulk-move-down-btn" title="Move rows down">↓ Move Down</button>
  <div class="sep"></div>
  <button class="btn-bulk" id="bulk-insert-above-btn">+ Insert Above</button>
  <button class="btn-bulk" id="bulk-insert-below-btn">+ Insert Below</button>
  <div class="sep"></div>
  <button class="btn-bulk" id="bulk-copy-btn" title="Copy as TSV">⧉ Copy</button>
  <div class="sep"></div>
  <button class="btn-bulk btn-bulk-danger" id="bulk-delete-btn">✕ Delete</button>
  <button class="btn-bulk" id="bulk-clear-btn" style="margin-left:auto">Clear Selection</button>
</div>

<div id="main-content">
  <div id="table-wrap">
    <div id="empty-msg">Select a TSV file to begin.</div>
  </div>

  <div id="spell-sidebar" class="hidden">
    <div id="spell-sidebar-header">
      <h2>HIL Checker</h2>
      <button id="run-check-btn">Run Check</button>
      <button id="spell-close-btn" title="Close sidebar">✕</button>
    </div>
    <div id="spell-status">Load a file and run check.</div>
    <div id="spell-sidebar-body">
      <div class="sidebar-section-title">Dialect (word-level)</div>
      <div id="spell-issue-list">
        <div id="spell-empty">Run a check to see suggestions.</div>
      </div>
      <div class="sidebar-section-title">Grammar (sentence-level)</div>
      <div id="gram-correction-list">
        <div id="gram-empty" style="padding:12px 16px;color:var(--muted);font-size:.82rem">No grammar fixes needed.</div>
      </div>
      <div class="sidebar-section-title" style="color:#63b3ed">EN Grammar</div>
      <div id="en-gram-list">
        <div id="en-gram-empty" style="padding:12px 16px;color:var(--muted);font-size:.82rem">Run check to see EN issues.</div>
      </div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>
<div id="sidebar-overlay"></div>

<!-- ── import modal ── -->
<div id="import-backdrop">
  <div id="import-modal">
    <div id="import-header">
      <h2>📥 Import TSV</h2>
      <button id="import-close">✕</button>
    </div>
    <div id="import-body">
      <div id="drop-zone" tabindex="0">
        <div style="font-size:2rem">📂</div>
        <strong>Drop a TSV file here</strong>
        <p>or click to browse</p>
        <input type="file" id="import-file-input" accept=".tsv,.txt,.csv" style="display:none">
      </div>
      <div id="import-options" style="display:none">
        <label><input type="checkbox" id="import-has-header" checked> First row is a header</label>
        <span id="import-file-name" style="color:var(--muted);font-size:0.8rem"></span>
      </div>
      <div id="col-map-wrap" style="display:none">
        <p style="font-size:0.82rem;color:var(--muted);margin-bottom:8px">Map each file column to a dataset column, or skip it.</p>
        <table id="col-map-table">
          <thead><tr>
            <th>File column</th>
            <th>Sample values (up to 5)</th>
            <th>Maps to dataset column</th>
          </tr></thead>
          <tbody id="col-map-body"></tbody>
        </table>
      </div>
      <div id="import-progress-bar"><div id="import-progress-fill"></div></div>
    </div>
    <div id="import-footer">
      <span id="import-status">Select a file to begin.</span>
      <button class="btn" id="import-do-btn" style="background:var(--accent);color:#fff" disabled>Import Rows</button>
    </div>
  </div>
</div>

<!-- ── header-config modal ── -->
<div id="hdr-backdrop">
  <div id="hdr-modal">
    <div id="hdr-modal-header">
      <h2 id="hdr-modal-title">Configure Columns</h2>
    </div>
    <div id="hdr-modal-body">
      <p style="font-size:.82rem;color:var(--muted);margin-bottom:14px">Define the column names for this dataset. Order matters — it determines the TSV column order.</p>
      <div id="hdr-list"></div>
      <button id="hdr-add-btn">+ Add column</button>
    </div>
    <div id="hdr-modal-footer">
      <button class="btn" id="hdr-cancel-btn" style="background:var(--border)">Cancel</button>
      <button class="btn btn-primary" id="hdr-save-btn">Save Columns</button>
    </div>
  </div>
</div>

<div id="settings-backdrop">
  <div id="settings-modal">
    <div id="settings-header">
      <h2>⚙ Settings</h2>
      <button id="settings-close" title="Close">✕</button>
    </div>
    <div id="settings-tabs">
      <button class="settings-tab active" data-tab="dict">Custom Dictionary</button>
      <button class="settings-tab" data-tab="dialect">Dialect Map</button>
    </div>
    <div id="settings-body">

      <!-- Custom Dictionary panel -->
      <div class="settings-panel active" id="panel-dict">
        <p style="font-size:.82rem;color:var(--muted)">Words added here are treated as valid and won't be flagged by the HIL spell checker. Useful for slang, proper nouns, or domain-specific terms.</p>
        <div id="dict-add-row">
          <input id="dict-add-input" type="text" placeholder="Add word to dictionary…" autocomplete="off">
          <button id="dict-add-btn">Add</button>
        </div>
        <div id="dict-list"><div id="dict-empty">No custom words yet.</div></div>
        <div id="dict-stats"></div>
      </div>

      <!-- Dialect Map panel -->
      <div class="settings-panel" id="panel-dialect">
        <p style="font-size:.82rem;color:var(--muted)">Manage word-level HIL→Ilonggo replacements. Entries are stored in <code style="font-size:.8rem;background:var(--bg);padding:1px 5px;border-radius:3px">spell-checker/words.csv</code>. Changes take effect immediately.</p>
        <div id="dialect-add-grid">
          <input id="dialect-base-input"   type="text" placeholder="Non-standard word…" autocomplete="off">
          <input id="dialect-target-input" type="text" placeholder="Ilonggo form…"      autocomplete="off">
          <button id="dialect-add-btn">Add</button>
        </div>
        <input id="dialect-filter" type="search" placeholder="Filter entries…" autocomplete="off">
        <div id="dialect-list"><div style="padding:12px;text-align:center;color:var(--muted);font-size:.82rem">Loading…</div></div>
        <div id="dialect-stats-line"></div>
      </div>

    </div>
  </div>
</div>

<div id="modal-backdrop">
  <div id="modal">
    <div id="modal-header">
      <span id="modal-title">Row</span>
      <div id="modal-nav">
        <button id="modal-prev">‹</button>
        <span id="modal-pos"></span>
        <button id="modal-next">›</button>
      </div>
      <button id="modal-close" title="Close (Esc)">✕</button>
    </div>
    <div id="modal-body"></div>
    <div id="modal-footer">
      <span id="modal-status"></span>
      <button class="btn btn-primary" id="modal-save-btn">Save all</button>
      <button class="btn" id="modal-done-btn" style="background:var(--border)">Done</button>
    </div>
  </div>
</div>

<script>
const $ = id => document.getElementById(id);
let state = { file: '', headers: [], rows: [], filtered: [], page: 0, pageSize: 100, query: '' };
let selectedRows = new Set();   // absolute row indices into state.rows
let lastCheckedIdx = null;      // for shift-click range select (filtered index)

// ── spell + grammar state ─────────────────────────────────────────────────────
let spellIssues       = [];   // [{row, col, issues:[{word,start,end,suggestion,type}]}]
let gramCorrections   = [];   // [{row, col, original, corrected, diff}]
let hilCol            = null;
let enIssues          = [];   // [{row, col, issues:[{start,end,message,replacements,rule_id}]}]
let enCol             = null;

// ── file list ────────────────────────────────────────────────────────────────
async function loadFiles() {
  const res   = await fetch('/api/files');
  const files = await res.json();
  const sel   = $('file-select');
  const groups = {};
  files.forEach(f => { (groups[f.dir] = groups[f.dir] || []).push(f); });
  Object.entries(groups).forEach(([dir, list]) => {
    const og = document.createElement('optgroup');
    og.label = dir === '.' ? '(root)' : dir;
    list.forEach(f => {
      const o = document.createElement('option');
      o.value = f.path; o.textContent = f.name;
      og.appendChild(o);
    });
    sel.appendChild(og);
  });
}

// ── load TSV ─────────────────────────────────────────────────────────────────
async function loadFile(path) {
  $('table-wrap').innerHTML = '<div id="empty-msg">Loading…</div>';
  const res = await fetch('/api/tsv?path=' + encodeURIComponent(path));
  if (!res.ok) { showToast('Failed to load file', true); return; }
  const data = await res.json();
  state.file    = path;
  state.headers = data.headers;
  state.rows    = data.rows;
  state.page    = parseInt(localStorage.getItem('tsv-page:' + path) || '0', 10);
  // Reset selection + spell + grammar state on new file
  selectedRows    = new Set();
  lastCheckedIdx  = null;
  renderBulkToolbar();
  spellIssues     = [];
  gramCorrections = [];
  hilCol          = null;
  enIssues        = [];
  enCol           = null;
  $('spell-status').textContent    = 'Checking…';
  $('spell-issue-list').innerHTML  = '';
  $('gram-correction-list').innerHTML = '';
  applyFilter();
  $('add-row-btn').disabled = false;
  // Auto-open sidebar and run check
  $('spell-sidebar').classList.remove('hidden');
  $('spell-toggle-btn').classList.add('active');
  runSpellCheck();
}

// ── filter & paginate ────────────────────────────────────────────────────────
function applyFilter() {
  const q = state.query.toLowerCase();
  state.filtered = q
    ? state.rows.map((r, i) => ({ r, i })).filter(({r}) => r.some(c => c.toLowerCase().includes(q)))
    : state.rows.map((r, i) => ({ r, i }));
  state.page = Math.min(state.page, Math.max(0, Math.ceil(state.filtered.length / state.pageSize) - 1));
  render();
}

function pageSlice() {
  const s = state.page * state.pageSize;
  return state.filtered.slice(s, s + state.pageSize);
}

// ── render ───────────────────────────────────────────────────────────────────
function render() {
  const wrap  = $('table-wrap');
  const total = state.filtered.length;
  const pages = Math.max(1, Math.ceil(total / state.pageSize));
  $('row-count').textContent  = total + ' rows';
  $('page-info').textContent  = `Page ${state.page + 1} / ${pages}`;
  $('prev-btn').disabled      = state.page === 0;
  $('next-btn').disabled      = state.page >= pages - 1;

  if (!state.headers.length) { wrap.innerHTML = '<div id="empty-msg">No data.</div>'; return; }

  const issueRows = new Set(spellIssues.map(r => r.row));
  const colW = Math.floor(86 / state.headers.length);
  const allPageSelected = pageSlice().length > 0 && pageSlice().every(({i}) => selectedRows.has(i));
  let html = `<table><thead><tr>`;
  html += `<th class="col-cb"><input type="checkbox" class="row-cb" id="cb-all" ${allPageSelected ? 'checked' : ''} title="Select all on page"></th>`;
  html += `<th class="col-row-num">#</th>`;
  state.headers.forEach(h => { html += `<th style="width:${colW}%">${esc(h)}</th>`; });
  html += `<th class="col-actions"></th></tr></thead><tbody>`;

  pageSlice().forEach(({ r, i }, filtPageIdx) => {
    const selCls = selectedRows.has(i) ? ' row-selected' : '';
    html += `<tr data-row="${i}" data-filt-idx="${filtPageIdx}">`;
    html += `<td class="col-cb"><input type="checkbox" class="row-cb" data-row="${i}" data-filt-idx="${filtPageIdx}" ${selectedRows.has(i) ? 'checked' : ''}></td>`;
    html += `<td class="row-num" data-row="${i}" title="Open row editor">${i + 1}</td>`;
    r.forEach((cell, j) => {
      const spellCls = (hilCol !== null && j === hilCol && issueRows.has(i))
        ? ' hil-cell-issue' : '';
      html += `<td class="cell-editable${spellCls}" contenteditable="true" data-row="${i}" data-col="${j}" spellcheck="true">${esc(cell)}</td>`;
    });
    html += `<td class="col-actions"><button class="del-btn" title="Delete row" data-row="${i}">✕</button></td>`;
    html += `</tr>`;
  });

  html += `</tbody></table>`;
  wrap.innerHTML = html;

  wrap.querySelectorAll('.cell-editable').forEach(td => {
    td.addEventListener('focus', onCellFocus);
    td.addEventListener('blur', onCellBlur);
    td.addEventListener('keydown', onCellKeydown);
  });
  wrap.querySelectorAll('.del-btn').forEach(btn => {
    btn.addEventListener('click', onDeleteRow);
  });
  wrap.querySelectorAll('td.row-num').forEach(td => {
    td.addEventListener('click', () => openModal(parseInt(td.dataset.row)));
  });

  // Select-all checkbox
  const cbAll = document.getElementById('cb-all');
  if (cbAll) cbAll.addEventListener('change', () => {
    if (cbAll.checked) pageSlice().forEach(({i}) => selectedRows.add(i));
    else pageSlice().forEach(({i}) => selectedRows.delete(i));
    lastCheckedIdx = null;
    renderBulkToolbar();
    // re-check individual checkboxes without full re-render
    wrap.querySelectorAll('input.row-cb[data-row]').forEach(cb => {
      cb.checked = selectedRows.has(parseInt(cb.dataset.row));
      cb.closest('tr').classList.toggle('row-selected', cb.checked);
    });
  });

  // Individual row checkboxes
  wrap.querySelectorAll('input.row-cb[data-row]').forEach(cb => {
    cb.addEventListener('click', e => {
      const rowIdx  = parseInt(cb.dataset.row);
      const filtIdx = parseInt(cb.dataset.filtIdx);
      if (e.shiftKey && lastCheckedIdx !== null) {
        const lo = Math.min(lastCheckedIdx, filtIdx);
        const hi = Math.max(lastCheckedIdx, filtIdx);
        const slice = pageSlice();
        for (let k = lo; k <= hi; k++) {
          if (k < slice.length) selectedRows.add(slice[k].i);
        }
      } else {
        if (cb.checked) selectedRows.add(rowIdx);
        else selectedRows.delete(rowIdx);
        lastCheckedIdx = filtIdx;
      }
      renderBulkToolbar();
      // sync checkboxes + row classes without full re-render
      wrap.querySelectorAll('input.row-cb[data-row]').forEach(c => {
        c.checked = selectedRows.has(parseInt(c.dataset.row));
        c.closest('tr').classList.toggle('row-selected', c.checked);
      });
      const allSel = pageSlice().every(({i}) => selectedRows.has(i));
      if (cbAll) cbAll.checked = allSel;
    });
  });

  // Re-apply inline underlines for the freshly rendered page
  applySpellHighlights();
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── cell editing ─────────────────────────────────────────────────────────────
function onCellFocus(e) {
  const td = e.target;
  // Strip highlight spans so editing is clean plain text
  if (td.querySelector('span.spell-err, span.gram-err')) {
    const row = parseInt(td.dataset.row);
    const col = parseInt(td.dataset.col);
    td.textContent = state.rows[row][col];
    // Restore cursor to end
    const range = document.createRange();
    range.selectNodeContents(td);
    range.collapse(false);
    const sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
  }
}

function onCellKeydown(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); e.target.blur(); }
  if (e.key === 'Escape') { e.target.blur(); }
}

async function onCellBlur(e) {
  const td   = e.target;
  const row  = parseInt(td.dataset.row);
  const col  = parseInt(td.dataset.col);
  const val  = td.textContent;
  const prev = state.rows[row][col];
  if (val === prev) return;
  state.rows[row][col] = val;
  td.classList.add('saving');
  const res = await fetch('/api/tsv/cell', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, row, col, value: val })
  });
  td.classList.remove('saving');
  if (res.ok) {
    td.closest('tr').classList.add('changed');
    showToast('Saved');
  } else {
    state.rows[row][col] = prev;
    td.textContent = prev;
    showToast('Save failed', true);
  }
}

// ── row actions ──────────────────────────────────────────────────────────────
async function onDeleteRow(e) {
  const row = parseInt(e.target.dataset.row);
  if (!confirm(`Delete row ${row + 1}?`)) return;
  const res = await fetch('/api/tsv/row/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, row })
  });
  if (res.ok) {
    state.rows.splice(row, 1);
    applyFilter();
    showToast('Row deleted');
  }
}

$('add-row-btn').addEventListener('click', async () => {
  const res = await fetch('/api/tsv/row/add', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file })
  });
  if (res.ok) {
    state.rows.push(Array(state.headers.length).fill(''));
    state.page = Math.ceil(state.rows.length / state.pageSize) - 1;
    applyFilter();
    setTimeout(() => {
      const cells = document.querySelectorAll('.cell-editable');
      if (cells.length) cells[cells.length - state.headers.length].focus();
    }, 50);
    showToast('Row added');
  }
});

// ── row modal ────────────────────────────────────────────────────────────────
let modalRow = -1;

function openModal(rowIdx) {
  modalRow = rowIdx;
  renderModal();
  $('modal-backdrop').classList.add('open');
  setTimeout(() => { const ta = $('modal-body').querySelector('textarea'); if (ta) ta.focus(); }, 50);
}

function closeModal() {
  $('modal-backdrop').classList.remove('open');
  modalRow = -1;
}

function renderModal() {
  const r = state.rows[modalRow];
  $('modal-title').textContent  = `Row ${modalRow + 1}`;
  $('modal-pos').textContent    = `${modalRow + 1} / ${state.rows.length}`;
  $('modal-prev').disabled      = modalRow <= 0;
  $('modal-next').disabled      = modalRow >= state.rows.length - 1;
  $('modal-status').textContent = '';

  const body = $('modal-body');
  body.innerHTML = '';
  state.headers.forEach((h, j) => {
    const div = document.createElement('div');
    div.className = 'modal-field';
    div.innerHTML = `<label>${esc(h)}</label><textarea data-col="${j}" spellcheck="true">${esc(r[j])}</textarea>`;
    body.appendChild(div);
  });
}

async function saveModal() {
  const textareas = $('modal-body').querySelectorAll('textarea');
  const pending   = [];
  textareas.forEach(ta => {
    const col = parseInt(ta.dataset.col);
    const val = ta.value;
    if (val !== state.rows[modalRow][col]) pending.push({ col, val });
  });
  if (!pending.length) { $('modal-status').textContent = 'No changes.'; return; }

  $('modal-save-btn').disabled  = true;
  $('modal-status').textContent = 'Saving…';
  let failed = 0;
  for (const { col, val } of pending) {
    const res = await fetch('/api/tsv/cell', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: state.file, row: modalRow, col, value: val })
    });
    if (res.ok) { state.rows[modalRow][col] = val; } else { failed++; }
  }
  $('modal-save-btn').disabled = false;
  if (failed) {
    $('modal-status').textContent = `${failed} field(s) failed to save.`;
  } else {
    $('modal-status').textContent = `Saved ${pending.length} field(s).`;
    const tr = document.querySelector(`tr[data-row="${modalRow}"]`);
    if (tr) {
      tr.classList.add('changed');
      tr.querySelectorAll('.cell-editable').forEach(td => {
        td.textContent = state.rows[modalRow][parseInt(td.dataset.col)];
      });
    }
  }
}

$('modal-prev').addEventListener('click', () => { if (modalRow > 0) { modalRow--; renderModal(); } });
$('modal-next').addEventListener('click', () => { if (modalRow < state.rows.length - 1) { modalRow++; renderModal(); } });
$('modal-close').addEventListener('click', closeModal);
$('modal-done-btn').addEventListener('click', closeModal);
$('modal-save-btn').addEventListener('click', saveModal);
$('modal-backdrop').addEventListener('click', e => { if (e.target === $('modal-backdrop')) closeModal(); });
document.addEventListener('keydown', e => {
  if (!$('modal-backdrop').classList.contains('open')) return;
  if (e.key === 'Escape') closeModal();
  if (e.key === 'ArrowLeft'  && e.altKey) { $('modal-prev').click(); e.preventDefault(); }
  if (e.key === 'ArrowRight' && e.altKey) { $('modal-next').click(); e.preventDefault(); }
});

// ── bulk row operations ───────────────────────────────────────────────────────

function renderBulkToolbar() {
  const n = selectedRows.size;
  $('bulk-toolbar').classList.toggle('visible', n > 0);
  $('bulk-count').textContent = `${n} row${n !== 1 ? 's' : ''} selected`;
}

async function bulkDelete() {
  const rows = [...selectedRows];
  if (!rows.length) return;
  if (!confirm(`Delete ${rows.length} row(s)?`)) return;
  const res = await fetch('/api/tsv/rows/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, rows })
  });
  if (!res.ok) { showToast('Bulk delete failed', true); return; }
  const sorted = [...rows].sort((a, b) => b - a);
  sorted.forEach(i => state.rows.splice(i, 1));
  selectedRows.clear();
  lastCheckedIdx = null;
  renderBulkToolbar();
  applyFilter();
  showToast(`Deleted ${rows.length} row(s)`);
}

async function bulkMove(direction) {
  const rows = [...selectedRows];
  if (!rows.length) return;
  const res = await fetch('/api/tsv/rows/move', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, rows, direction })
  });
  if (!res.ok) { showToast('Move failed', true); return; }
  // Reflect move locally
  const rowsSet = new Set(rows);
  const sorted  = [...rows].sort((a, b) => a - b);
  if (direction === 'up') {
    for (const i of sorted) {
      if (i > 0 && !rowsSet.has(i - 1)) {
        [state.rows[i], state.rows[i - 1]] = [state.rows[i - 1], state.rows[i]];
      }
    }
    selectedRows = new Set(rows.map(i => (i > 0 && !rowsSet.has(i - 1)) ? i - 1 : i));
  } else {
    for (const i of sorted.reverse()) {
      if (i < state.rows.length - 1 && !rowsSet.has(i + 1)) {
        [state.rows[i], state.rows[i + 1]] = [state.rows[i + 1], state.rows[i]];
      }
    }
    selectedRows = new Set(rows.map(i => (i < state.rows.length - 1 && !rowsSet.has(i + 1)) ? i + 1 : i));
  }
  lastCheckedIdx = null;
  applyFilter();
}

async function bulkInsert(position) {
  // position: 'above' inserts before min selected row, 'below' inserts after max
  const rows = [...selectedRows].sort((a, b) => a - b);
  if (!rows.length) return;
  const after = position === 'above' ? rows[0] - 1 : rows[rows.length - 1];
  const res = await fetch('/api/tsv/row/insert', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, after })
  });
  if (!res.ok) { showToast('Insert failed', true); return; }
  const d = await res.json();
  state.rows.splice(d.row, 0, Array(state.headers.length).fill(''));
  // Shift selected indices if rows were inserted above them
  if (position === 'above') {
    selectedRows = new Set([...selectedRows].map(i => i >= d.row ? i + 1 : i));
  }
  applyFilter();
  showToast('Row inserted');
}

function bulkCopy() {
  const rows = [...selectedRows].sort((a, b) => a - b);
  if (!rows.length) return;
  const lines = rows.map(i => state.rows[i].join('\t'));
  navigator.clipboard.writeText(lines.join('\n'))
    .then(() => showToast(`Copied ${rows.length} row(s) as TSV`))
    .catch(() => showToast('Clipboard write failed', true));
}

$('bulk-delete-btn').addEventListener('click', bulkDelete);
$('bulk-move-up-btn').addEventListener('click', () => bulkMove('up'));
$('bulk-move-down-btn').addEventListener('click', () => bulkMove('down'));
$('bulk-insert-above-btn').addEventListener('click', () => bulkInsert('above'));
$('bulk-insert-below-btn').addEventListener('click', () => bulkInsert('below'));
$('bulk-copy-btn').addEventListener('click', bulkCopy);
$('bulk-clear-btn').addEventListener('click', () => {
  selectedRows.clear();
  lastCheckedIdx = null;
  renderBulkToolbar();
  document.querySelectorAll('input.row-cb').forEach(cb => cb.checked = false);
  document.querySelectorAll('tr.row-selected').forEach(tr => tr.classList.remove('row-selected'));
});

// ── spell check ───────────────────────────────────────────────────────────────

function toggleSpellSidebar() {
  const sidebar  = $('spell-sidebar');
  const isHidden = sidebar.classList.toggle('hidden');
  $('spell-toggle-btn').classList.toggle('active', !isHidden);
  const isMobile = window.matchMedia('(max-width: 768px)').matches;
  $('sidebar-overlay').classList.toggle('visible', !isHidden && isMobile);
}

$('sidebar-overlay').addEventListener('click', () => {
  $('spell-sidebar').classList.add('hidden');
  $('spell-toggle-btn').classList.remove('active');
  $('sidebar-overlay').classList.remove('visible');
});

// Build cell HTML combining spell (red wavy) and grammar (amber wavy) highlights.
// spellIssues: [{start, end, word, suggestion}]
// gramOrigDiff: [{token, changed}]  — tokens from the original sentence
function buildCellHtml(text, spellIssues, gramOrigDiff) {
  const ranges = [];

  // Spell ranges come with exact char positions
  for (const iss of (spellIssues || [])) {
    ranges.push({ start: iss.start, end: iss.end, cls: 'spell-err', title: `Use: ${iss.suggestion}` });
  }

  // Grammar ranges: find each changed token's position in the original text
  if (gramOrigDiff) {
    let searchFrom = 0;
    for (const tok of gramOrigDiff) {
      const idx = text.indexOf(tok.token, searchFrom);
      if (idx === -1) continue;
      const end = idx + tok.token.length;
      if (tok.changed) {
        // Only add if not already covered by a spell range
        const covered = ranges.some(r => r.start <= idx && r.end >= end);
        if (!covered) {
          ranges.push({ start: idx, end, cls: 'gram-err', title: 'Grammar: Ilonggo form needed' });
        }
      }
      searchFrom = end;
    }
  }

  ranges.sort((a, b) => a.start - b.start);
  let result = '', pos = 0;
  for (const r of ranges) {
    if (r.start < pos) continue;
    result += esc(text.slice(pos, r.start));
    result += `<span class="${r.cls}" title="${esc(r.title)}">${esc(text.slice(r.start, r.end))}</span>`;
    pos = r.end;
  }
  return result + esc(text.slice(pos));
}

// Apply inline highlights (spell + grammar) to all visible HIL cells
function applyAllHighlights() {
  if (hilCol === null) return;
  const spellMap = new Map(spellIssues.map(ri => [ri.row, ri.issues]));
  const gramMap  = new Map(gramCorrections.map(c  => [c.row,  c.orig_diff]));

  document.querySelectorAll(`.cell-editable[data-col="${hilCol}"]`).forEach(td => {
    if (document.activeElement === td) return;
    const row       = parseInt(td.dataset.row);
    const sIssues   = spellMap.get(row);
    const gOrigDiff = gramMap.get(row);
    if (!sIssues?.length && !gOrigDiff) return;
    td.innerHTML = buildCellHtml(state.rows[row][hilCol], sIssues, gOrigDiff);
  });
}

// Keep old name as alias so existing callers don't break
const applySpellHighlights = applyAllHighlights;

async function runSpellCheck() {
  if (!state.file) { showToast('Load a file first', true); return; }

  hilCol = state.headers.findIndex(h => h.toLowerCase().includes('hil'));
  enCol  = state.headers.findIndex(h => /\ben\b/i.test(h) || h.toLowerCase() === 'english');

  $('run-check-btn').disabled      = true;
  $('spell-status').textContent    = 'Checking…';
  $('spell-issue-list').innerHTML  = '';
  $('gram-correction-list').innerHTML = '';
  $('en-gram-list').innerHTML      = '<div style="padding:12px 16px;color:var(--muted);font-size:.82rem">Checking EN…</div>';

  const fetches = [];

  if (hilCol !== -1) {
    const rows = pageSlice().map(({ r, i }) => ({ row: i, text: r[hilCol] || '' }));
    const body = JSON.stringify({ rows });
    const opts = { method: 'POST', headers: { 'Content-Type': 'application/json' }, body };
    fetches.push(fetch('/api/spellcheck/batch', opts));
    fetches.push(fetch('/api/grammar/batch',    opts));
  } else {
    fetches.push(Promise.resolve(null));
    fetches.push(Promise.resolve(null));
    $('spell-issue-list').innerHTML = '<div style="padding:12px 8px;color:var(--muted);font-size:.82rem">No HIL column.</div>';
    $('gram-correction-list').innerHTML = '<div style="padding:12px 8px;color:var(--muted);font-size:.82rem">—</div>';
  }

  if (enCol !== -1) {
    const enRows = pageSlice().map(({ r, i }) => ({ row: i, text: r[enCol] || '' }));
    fetches.push(fetch('/api/en-grammar/batch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rows: enRows }),
    }));
  } else {
    fetches.push(Promise.resolve(null));
    $('en-gram-list').innerHTML = '<div style="padding:12px 16px;color:var(--muted);font-size:.82rem">No EN column found.</div>';
  }

  const [spellRes, gramRes, enRes] = await Promise.all(fetches);
  $('run-check-btn').disabled = false;

  // HIL spell
  if (spellRes && spellRes.ok) {
    const d = await spellRes.json();
    spellIssues = d.issues.map(ri => ({ ...ri, col: hilCol }));
  }
  // HIL grammar
  if (gramRes && gramRes.ok) {
    const d = await gramRes.json();
    gramCorrections = d.corrections.map(c => ({ ...c, col: hilCol }));
  }
  // EN grammar
  if (enRes) {
    if (enRes.ok) {
      const d = await enRes.json();
      enIssues = d.issues.map(ri => ({ ...ri, col: enCol }));
    } else {
      const err = await enRes.json().catch(() => ({}));
      $('en-gram-list').innerHTML = `<div style="padding:12px 16px;color:var(--danger);font-size:.82rem">${esc(err.error || 'EN check failed')}</div>`;
    }
  }

  renderSidebarStatus();
  if (hilCol !== -1) { renderSpellIssues(); renderGrammarCorrections(); applySpellHighlights(); }
  if (enCol  !== -1 && enRes?.ok) { renderEnIssues(); applyEnHighlights(); }
}

function renderSidebarStatus() {
  const sc  = spellIssues.reduce((s, r) => s + r.issues.length, 0);
  const gc  = gramCorrections.length;
  const enc = enIssues.reduce((s, r) => s + r.issues.length, 0);
  if (sc === 0 && gc === 0 && enc === 0) {
    $('spell-status').textContent = 'No issues found!';
  } else {
    const parts = [];
    if (sc)  parts.push(`${sc} HIL spell`);
    if (gc)  parts.push(`${gc} HIL grammar`);
    if (enc) parts.push(`${enc} EN issue${enc !== 1 ? 's' : ''}`);
    $('spell-status').textContent = parts.join(' · ');
  }
}

function renderSpellIssues() {
  const list  = $('spell-issue-list');
  const total = spellIssues.reduce((s, r) => s + r.issues.length, 0);

  if (total === 0) {
    list.innerHTML = '<div style="padding:12px 16px;text-align:center;color:var(--spell-fix);font-size:.82rem">All clear!</div>';
    return;
  }
  list.innerHTML = '';

  spellIssues.forEach((rowIssue, ri) => {
    rowIssue.issues.forEach((issue, ii) => {
      const card = document.createElement('div');
      card.className = 'spell-card';
      card.dataset.ri = ri;
      card.dataset.ii = ii;
      card.innerHTML = `
        <div class="row-ref">Row ${rowIssue.row + 1} &mdash; HIL column</div>
        <div class="issue-body" data-ri="${ri}" data-ii="${ii}" title="Click to jump to word">
          <span class="word-error">${esc(issue.word)}</span>
          <span class="arrow">&#8594;</span>
          <span class="word-fix">${esc(issue.suggestion)}</span>
        </div>
        <div class="card-actions">
          <button class="accept-btn" data-ri="${ri}" data-ii="${ii}">Accept</button>
          <button class="reject-btn" data-ri="${ri}" data-ii="${ii}">Ignore</button>
          <button class="btn-dict" data-word="${esc(issue.word)}" data-ri="${ri}" data-ii="${ii}" title="Mark as valid — won't be flagged again">+ Dict</button>
        </div>`;
      list.appendChild(card);
    });
  });

  list.querySelectorAll('.issue-body').forEach(el => el.addEventListener('click', e => {
    scrollToIssue(parseInt(e.currentTarget.dataset.ri), parseInt(e.currentTarget.dataset.ii));
  }));
  list.querySelectorAll('.accept-btn').forEach(btn => btn.addEventListener('click', onAcceptIssue));
  list.querySelectorAll('.reject-btn').forEach(btn => btn.addEventListener('click', onRejectIssue));
  list.querySelectorAll('.btn-dict').forEach(btn => btn.addEventListener('click', onAddToDict));
}

// ── grammar rendering ─────────────────────────────────────────────────────────

function buildGrammarDiffHtml(original, corrected, diff) {
  // Corrected sentence with changed tokens highlighted green
  const corrHtml = diff.map(tok =>
    tok.changed
      ? `<span class="gram-ins">${esc(tok.token)}</span>`
      : esc(tok.token)
  ).join(' ');

  // Original sentence with changed positions struck out
  const origToks = original.split(/\s+/);
  const origHtml = origToks.map((tok, i) => {
    const corrTok = diff[i];
    const changed = corrTok && corrTok.changed && corrTok.token.toLowerCase() !== tok.toLowerCase();
    return changed ? `<span class="gram-del">${esc(tok)}</span>` : esc(tok);
  }).join(' ');

  return { origHtml, corrHtml };
}

function renderGrammarCorrections() {
  const list = $('gram-correction-list');

  if (gramCorrections.length === 0) {
    list.innerHTML = '<div style="padding:12px 16px;color:var(--spell-fix);font-size:.82rem;text-align:center">All clear!</div>';
    return;
  }

  list.innerHTML = '';
  gramCorrections.forEach((corr, gi) => {
    const { origHtml, corrHtml } = buildGrammarDiffHtml(corr.original, corr.corrected, corr.diff);
    const card = document.createElement('div');
    card.className = 'gram-card';
    card.dataset.gi = gi;
    card.innerHTML = `
      <div class="row-ref gram-card-nav" data-gi="${gi}" title="Click to jump to row" style="cursor:pointer">
        Row ${corr.row + 1} &mdash; HIL column
      </div>
      <div class="gram-orig gram-card-nav" data-gi="${gi}" title="Click to jump to row" style="cursor:pointer">${origHtml}</div>
      <div class="gram-fix">${corrHtml}</div>
      <div class="card-actions">
        <button class="gram-accept-btn" data-gi="${gi}">Accept Fix</button>
        <button class="gram-reject-btn" data-gi="${gi}">Ignore</button>
      </div>`;
    list.appendChild(card);
  });

  list.querySelectorAll('.gram-card-nav').forEach(el =>
    el.addEventListener('click', e => scrollToGrammar(parseInt(e.currentTarget.dataset.gi)))
  );
  list.querySelectorAll('.gram-accept-btn').forEach(btn => btn.addEventListener('click', onAcceptGrammar));
  list.querySelectorAll('.gram-reject-btn').forEach(btn => btn.addEventListener('click', onRejectGrammar));
}

function glowCell(td) {
  if (!td) return;
  td.classList.remove('cell-glow');
  void td.offsetWidth; // force reflow to restart animation
  td.classList.add('cell-glow');
  setTimeout(() => td.classList.remove('cell-glow'), 950);
}

function scrollToGrammar(gi) {
  const corr = gramCorrections[gi];
  if (!corr) return;

  // Mark card active
  $('gram-correction-list').querySelectorAll('.gram-card').forEach(c => c.classList.remove('active'));
  const activeCard = $('gram-correction-list').querySelector(`.gram-card[data-gi="${gi}"]`);
  if (activeCard) { activeCard.classList.add('active'); activeCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); }

  // Navigate to the right page if needed
  const inPage = pageSlice().some(({ i }) => i === corr.row);
  if (!inPage) {
    const filtIdx = state.filtered.findIndex(f => f.i === corr.row);
    if (filtIdx !== -1) state.page = Math.floor(filtIdx / state.pageSize);
    render();
  }

  // Scroll row into view and glow the cell
  const tr = document.querySelector(`tr[data-row="${corr.row}"]`);
  if (tr) tr.scrollIntoView({ behavior: 'smooth', block: 'center' });

  const td = document.querySelector(
    `td.cell-editable[data-row="${corr.row}"][data-col="${corr.col}"]`
  );
  glowCell(td);
  if (td) {
    td.querySelectorAll('span.gram-err').forEach(span => {
      span.classList.remove('flash');
      void span.offsetWidth;
      span.classList.add('flash');
      setTimeout(() => span.classList.remove('flash'), 750);
    });
  }
}

async function onAcceptGrammar(e) {
  const gi   = parseInt(e.target.dataset.gi);
  const corr = gramCorrections[gi];
  if (!corr) return;

  const res = await fetch('/api/tsv/cell', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, row: corr.row, col: corr.col, value: corr.corrected })
  });
  if (!res.ok) { showToast('Save failed', true); return; }

  // Update in-memory state
  state.rows[corr.row][corr.col] = corr.corrected;

  // Update the visible cell (clear highlights, show corrected plain text)
  const td = document.querySelector(`td.cell-editable[data-row="${corr.row}"][data-col="${corr.col}"]`);
  if (td && document.activeElement !== td) {
    td.textContent = corr.corrected;
    td.classList.remove('hil-cell-issue');
  }
  // Mark row as changed
  const tr = document.querySelector(`tr[data-row="${corr.row}"]`);
  if (tr) tr.classList.add('changed');

  // Drop resolved spell issues for this row
  spellIssues     = spellIssues.filter(ri => ri.row !== corr.row);
  gramCorrections.splice(gi, 1);

  showToast('Grammar fix applied');
  renderSidebarStatus();
  renderSpellIssues();
  renderGrammarCorrections();
  applyAllHighlights();
}

function onRejectGrammar(e) {
  const gi   = parseInt(e.target.dataset.gi);
  const corr = gramCorrections[gi];
  // Clear the amber underlines for this row
  const td = document.querySelector(`td.cell-editable[data-row="${corr.row}"][data-col="${corr.col}"]`);
  gramCorrections.splice(gi, 1);
  // Re-apply (may still have spell highlights)
  if (td && document.activeElement !== td) {
    const sIssues = spellIssues.find(ri => ri.row === corr.row)?.issues;
    td.innerHTML = buildCellHtml(state.rows[corr.row][corr.col], sIssues, null);
    if (!sIssues?.length) td.classList.remove('hil-cell-issue');
  }
  renderSidebarStatus();
  renderGrammarCorrections();
}

// ── EN grammar rendering ──────────────────────────────────────────────────────

function renderEnIssues() {
  const list = $('en-gram-list');
  const total = enIssues.reduce((s, r) => s + r.issues.length, 0);
  if (total === 0) {
    list.innerHTML = '<div style="padding:12px 16px;text-align:center;color:var(--spell-fix);font-size:.82rem">EN looks good!</div>';
    return;
  }
  list.innerHTML = '';
  enIssues.forEach((rowIssue, ri) => {
    rowIssue.issues.forEach((issue, ii) => {
      const orig = esc(issue.start < issue.end
        ? (state.rows[rowIssue.row][rowIssue.col] || '').slice(issue.start, issue.end)
        : '…');
      const firstFix = issue.replacements[0] ? esc(issue.replacements[0]) : '';
      const card = document.createElement('div');
      card.className = 'en-card';
      card.dataset.ri = ri; card.dataset.ii = ii;
      card.innerHTML = `
        <div class="row-ref">Row ${rowIssue.row + 1} &mdash; EN column</div>
        <div class="en-msg" data-ri="${ri}" data-ii="${ii}" title="Click to jump">${esc(issue.message)}</div>
        ${firstFix ? `<div style="display:flex;align-items:center;gap:4px;font-size:.82rem;margin-bottom:6px">
          <span class="en-orig">${orig}</span>
          <span style="color:var(--muted)">→</span>
          <span class="en-fix">${firstFix}</span>
        </div>` : ''}
        <div class="card-actions">
          ${firstFix ? `<button class="accept-btn" data-ri="${ri}" data-ii="${ii}">Accept</button>` : ''}
          <button class="reject-btn" data-ri="${ri}" data-ii="${ii}">Ignore</button>
        </div>`;
      list.appendChild(card);
    });
  });

  list.querySelectorAll('.en-msg').forEach(el =>
    el.addEventListener('click', e =>
      scrollToEnIssue(parseInt(e.currentTarget.dataset.ri), parseInt(e.currentTarget.dataset.ii))
    )
  );
  list.querySelectorAll('.accept-btn').forEach(btn => btn.addEventListener('click', onAcceptEnIssue));
  list.querySelectorAll('.reject-btn').forEach(btn => btn.addEventListener('click', onRejectEnIssue));
}

function applyEnHighlights() {
  if (enCol === null) return;
  const enMap = new Map(enIssues.map(ri => [ri.row, ri.issues]));
  document.querySelectorAll(`.cell-editable[data-col="${enCol}"]`).forEach(td => {
    if (document.activeElement === td) return;
    const row    = parseInt(td.dataset.row);
    const issues = enMap.get(row);
    if (!issues?.length) return;
    const text = state.rows[row][enCol] || '';
    const ranges = issues.map(iss => ({
      start: iss.start, end: iss.end,
      cls: 'en-err', title: esc(iss.message)
    })).sort((a, b) => a.start - b.start);
    let html = '', pos = 0;
    for (const r of ranges) {
      if (r.start < pos) continue;
      html += esc(text.slice(pos, r.start));
      html += `<span class="${r.cls}" title="${r.title}">${esc(text.slice(r.start, r.end))}</span>`;
      pos = r.end;
    }
    html += esc(text.slice(pos));
    td.innerHTML = html;
    td.classList.add('en-cell-issue');
  });
}

function scrollToEnIssue(ri, ii) {
  const rowIssue = enIssues[ri];
  if (!rowIssue) return;

  $('en-gram-list').querySelectorAll('.en-card').forEach(c => c.classList.remove('active'));
  const card = $('en-gram-list').querySelector(`.en-card[data-ri="${ri}"][data-ii="${ii}"]`);
  if (card) { card.classList.add('active'); card.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); }

  const inPage = pageSlice().some(({ i }) => i === rowIssue.row);
  if (!inPage) {
    const filtIdx = state.filtered.findIndex(f => f.i === rowIssue.row);
    if (filtIdx !== -1) state.page = Math.floor(filtIdx / state.pageSize);
    render();
    applyEnHighlights();
  }
  const tr = document.querySelector(`tr[data-row="${rowIssue.row}"]`);
  if (tr) tr.scrollIntoView({ behavior: 'smooth', block: 'center' });

  const td = document.querySelector(`td.cell-editable[data-row="${rowIssue.row}"][data-col="${rowIssue.col}"]`);
  glowCell(td);
  if (td) {
    td.querySelectorAll('span.en-err').forEach(span => {
      span.classList.remove('flash'); void span.offsetWidth; span.classList.add('flash');
      setTimeout(() => span.classList.remove('flash'), 750);
    });
  }
}

async function onAcceptEnIssue(e) {
  const ri       = parseInt(e.target.dataset.ri);
  const ii       = parseInt(e.target.dataset.ii);
  const rowIssue = enIssues[ri];
  const issue    = rowIssue?.issues[ii];
  if (!issue || !issue.replacements[0]) return;

  const text   = state.rows[rowIssue.row][rowIssue.col] || '';
  const newVal = text.slice(0, issue.start) + issue.replacements[0] + text.slice(issue.end);

  const res = await fetch('/api/tsv/cell', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, row: rowIssue.row, col: rowIssue.col, value: newVal })
  });
  if (!res.ok) { showToast('Save failed', true); return; }

  state.rows[rowIssue.row][rowIssue.col] = newVal;
  const tr = document.querySelector(`tr[data-row="${rowIssue.row}"]`);
  if (tr) tr.classList.add('changed');

  // Remove this specific issue
  rowIssue.issues.splice(ii, 1);
  if (rowIssue.issues.length === 0) {
    enIssues.splice(ri, 1);
    const td = document.querySelector(`td.cell-editable[data-row="${rowIssue.row}"][data-col="${rowIssue.col}"]`);
    if (td) { td.textContent = newVal; td.classList.remove('en-cell-issue'); }
  } else {
    applyEnHighlights();
  }
  showToast('EN fix applied');
  renderSidebarStatus();
  renderEnIssues();
}

function onRejectEnIssue(e) {
  const ri       = parseInt(e.target.dataset.ri);
  const ii       = parseInt(e.target.dataset.ii);
  const rowIssue = enIssues[ri];
  if (!rowIssue) return;
  rowIssue.issues.splice(ii, 1);
  if (rowIssue.issues.length === 0) {
    enIssues.splice(ri, 1);
    const td = document.querySelector(`td.cell-editable[data-row="${rowIssue.row}"][data-col="${rowIssue.col}"]`);
    if (td) { td.textContent = state.rows[rowIssue.row][rowIssue.col]; td.classList.remove('en-cell-issue'); }
  }
  renderSidebarStatus();
  renderEnIssues();
}

function scrollToIssue(ri, ii) {
  const rowIssue = spellIssues[ri];
  if (!rowIssue) return;
  const issue = rowIssue.issues[ii];

  // Mark the card as active and scroll it into view in the sidebar
  $('spell-issue-list').querySelectorAll('.spell-card').forEach(c => c.classList.remove('active'));
  const activeCard = $('spell-issue-list').querySelector(`.spell-card[data-ri="${ri}"][data-ii="${ii}"]`);
  if (activeCard) { activeCard.classList.add('active'); activeCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); }

  // If the row is on a different page, navigate there first
  const pageStart = state.page * state.pageSize;
  const pageEnd   = pageStart + state.pageSize;
  if (rowIssue.row < pageStart || rowIssue.row >= pageEnd) {
    state.page = Math.floor(
      state.filtered.findIndex(f => f.i === rowIssue.row) / state.pageSize
    );
    render();
  }

  // Scroll the row into view and glow the cell
  const tr = document.querySelector(`tr[data-row="${rowIssue.row}"]`);
  if (tr) tr.scrollIntoView({ behavior: 'smooth', block: 'center' });

  const td = document.querySelector(
    `td.cell-editable[data-row="${rowIssue.row}"][data-col="${rowIssue.col}"]`
  );
  glowCell(td);
  if (td) {
    td.querySelectorAll('span.spell-err').forEach(span => {
      if (span.textContent === issue.word) {
        span.classList.remove('flash');
        void span.offsetWidth;
        span.classList.add('flash');
        setTimeout(() => span.classList.remove('flash'), 750);
      }
    });
  }
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function onAcceptIssue(e) {
  const ri       = parseInt(e.target.dataset.ri);
  const ii       = parseInt(e.target.dataset.ii);
  const rowIssue = spellIssues[ri];
  const issue    = rowIssue.issues[ii];

  const currentVal = state.rows[rowIssue.row][rowIssue.col];
  // Replace all occurrences with the proper Ilonggo form (word-boundary aware)
  const newVal = currentVal.replace(
    new RegExp('\\b' + escapeRegex(issue.word) + '\\b', 'g'),
    issue.suggestion
  );

  if (newVal === currentVal) {
    // Word already changed; just dismiss
    removeIssue(ri, ii);
    return;
  }

  const res = await fetch('/api/tsv/cell', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, row: rowIssue.row, col: rowIssue.col, value: newVal })
  });

  if (res.ok) {
    state.rows[rowIssue.row][rowIssue.col] = newVal;
    // Update visible cell if on current page
    const td = document.querySelector(
      `td.cell-editable[data-row="${rowIssue.row}"][data-col="${rowIssue.col}"]`
    );
    if (td) td.textContent = newVal;
    removeIssue(ri, ii);
    showToast('Applied');
  } else {
    showToast('Save failed', true);
  }
}

function onRejectIssue(e) {
  removeIssue(parseInt(e.target.dataset.ri), parseInt(e.target.dataset.ii));
}

function removeIssue(ri, ii) {
  const rowIssue = spellIssues[ri];
  rowIssue.issues.splice(ii, 1);
  const td = document.querySelector(
    `td.cell-editable[data-row="${rowIssue.row}"][data-col="${rowIssue.col}"]`
  );
  if (rowIssue.issues.length === 0) {
    spellIssues.splice(ri, 1);
    // Clear all highlights on this cell
    if (td && document.activeElement !== td) {
      td.textContent = state.rows[rowIssue.row][rowIssue.col];
      td.classList.remove('hil-cell-issue');
    }
  } else {
    // Re-apply remaining underlines
    if (td && document.activeElement !== td) {
      const gd = gramCorrections.find(c => c.row === rowIssue.row)?.orig_diff;
      td.innerHTML = buildCellHtml(state.rows[rowIssue.row][rowIssue.col], rowIssue.issues, gd);
    }
  }
  renderSidebarStatus();
  renderSpellIssues();
}

$('spell-toggle-btn').addEventListener('click', toggleSpellSidebar);
$('run-check-btn').addEventListener('click', runSpellCheck);
$('spell-close-btn').addEventListener('click', () => {
  $('spell-sidebar').classList.add('hidden');
  $('spell-toggle-btn').classList.remove('active');
  $('sidebar-overlay').classList.remove('visible');
});

// ── save indicator ───────────────────────────────────────────────────────────
let pendingSaves = 0;

function setSaveState(s) {
  const el = $('save-indicator');
  el.className = 'saved'; // reset
  if (s === 'saving')  { el.className = 'saving';  el.textContent = '● Saving…'; }
  else if (s === 'unsaved') { el.className = 'unsaved'; el.textContent = '● Unsaved'; }
  else                 { el.className = 'saved';   el.textContent = '● Saved'; }
}

// Patch onCellBlur to track save state
const _origOnCellBlur = onCellBlur;
async function onCellBlurPatched(e) {
  const td   = e.target;
  const row  = parseInt(td.dataset.row);
  const col  = parseInt(td.dataset.col);
  const val  = td.textContent;
  const prev = state.rows[row][col];
  if (val === prev) return;
  pendingSaves++;
  setSaveState('saving');
  state.rows[row][col] = val;
  td.classList.add('saving');
  const res = await fetch('/api/tsv/cell', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file, row, col, value: val })
  });
  td.classList.remove('saving');
  pendingSaves--;
  if (res.ok) {
    td.closest('tr').classList.add('changed');
    showToast('Saved');
  } else {
    state.rows[row][col] = prev;
    td.textContent = prev;
    showToast('Save failed', true);
  }
  if (pendingSaves === 0) setSaveState('saved');
}

// Replace cell-blur listener in render by overriding the function reference
// (we swap onCellBlur reference used in addEventListener)
function onCellBlur(e) { onCellBlurPatched(e); }

// Ctrl+S — no-op (all saves are per-cell); just show confirmation
document.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 's' && !$('modal-backdrop').classList.contains('open')) {
    e.preventDefault();
    if (!state.file) return;
    if (pendingSaves === 0) showToast('All changes saved');
    else showToast('Saving…');
  }
});

// ── find / replace ───────────────────────────────────────────────────────────
let findMatches  = [];   // [{row, col, start, end}] — across all state.rows
let findCurrent  = -1;
let replaceMode  = false;
let findRegex    = false;
let findCase     = false;

function buildFindRegex(pattern) {
  if (!pattern) return null;
  try {
    const flags = findCase ? 'g' : 'gi';
    return findRegex ? new RegExp(pattern, flags) : new RegExp(escapeRegex(pattern), flags);
  } catch (_) { return null; }
}

function runFind() {
  const pattern = $('find-input').value;
  $('find-input').classList.remove('has-error');
  findMatches = [];
  findCurrent = -1;
  clearFindHighlights();
  if (!pattern || !state.rows.length) { $('find-match-count').textContent = 'No results'; return; }

  const re = buildFindRegex(pattern);
  if (!re) { $('find-input').classList.add('has-error'); $('find-match-count').textContent = 'Bad regex'; return; }

  state.rows.forEach((row, ri) => {
    row.forEach((cell, ci) => {
      re.lastIndex = 0;
      let m;
      while ((m = re.exec(cell)) !== null) {
        findMatches.push({ row: ri, col: ci, start: m.index, end: m.index + m[0].length });
        if (m[0].length === 0) break; // guard zero-width matches
      }
    });
  });

  $('find-match-count').textContent = findMatches.length ? `1 of ${findMatches.length}` : 'No results';
  if (findMatches.length) { findCurrent = 0; jumpToMatch(0); }
}

function clearFindHighlights() {
  document.querySelectorAll('td.find-match, td.find-match-active').forEach(td => {
    td.classList.remove('find-match', 'find-match-active');
  });
}

function jumpToMatch(idx) {
  if (!findMatches.length) return;
  findCurrent = ((idx % findMatches.length) + findMatches.length) % findMatches.length;
  $('find-match-count').textContent = `${findCurrent + 1} of ${findMatches.length}`;
  clearFindHighlights();

  // Highlight all matches on the current page
  const pageRowSet = new Set(pageSlice().map(({i}) => i));
  findMatches.forEach((m, k) => {
    if (!pageRowSet.has(m.row)) return;
    const td = document.querySelector(`td.cell-editable[data-row="${m.row}"][data-col="${m.col}"]`);
    if (td) td.classList.add(k === findCurrent ? 'find-match-active' : 'find-match');
  });

  const cur = findMatches[findCurrent];
  // Navigate to the page containing the active match
  const filtIdx = state.filtered.findIndex(f => f.i === cur.row);
  if (filtIdx !== -1) {
    const targetPage = Math.floor(filtIdx / state.pageSize);
    if (targetPage !== state.page) {
      state.page = targetPage;
      render();
      // Re-highlight after render
      clearFindHighlights();
      const pageRowSet2 = new Set(pageSlice().map(({i}) => i));
      findMatches.forEach((m, k) => {
        if (!pageRowSet2.has(m.row)) return;
        const td = document.querySelector(`td.cell-editable[data-row="${m.row}"][data-col="${m.col}"]`);
        if (td) td.classList.add(k === findCurrent ? 'find-match-active' : 'find-match');
      });
    }
  }

  const activeTd = document.querySelector(`td.find-match-active`);
  if (activeTd) activeTd.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

async function doReplace(allMatches) {
  const pattern = $('find-input').value;
  const repl    = $('replace-input').value;
  if (!pattern || !state.file) return;
  const re = buildFindRegex(pattern);
  if (!re) return;

  const targets = allMatches ? findMatches : (findCurrent >= 0 ? [findMatches[findCurrent]] : []);
  const changed = new Map(); // row → new value

  for (const m of targets) {
    const original = changed.has(m.row) ? changed.get(m.row)[m.col] : state.rows[m.row][m.col];
    // Build per-row replacement
    if (!changed.has(m.row)) changed.set(m.row, [...state.rows[m.row]]);
  }
  // Apply replacements per row
  for (const [ri, rowData] of changed.entries()) {
    state.rows[ri].forEach((cell, ci) => {
      re.lastIndex = 0;
      const newVal = cell.replace(re, repl);
      if (newVal !== cell) rowData[ci] = newVal;
    });
  }

  setSaveState('saving');
  let count = 0;
  for (const [ri, rowData] of changed.entries()) {
    for (let ci = 0; ci < rowData.length; ci++) {
      if (rowData[ci] !== state.rows[ri][ci]) {
        pendingSaves++;
        const res = await fetch('/api/tsv/cell', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: state.file, row: ri, col: ci, value: rowData[ci] })
        });
        pendingSaves--;
        if (res.ok) { state.rows[ri][ci] = rowData[ci]; count++; }
      }
    }
  }
  if (pendingSaves === 0) setSaveState('saved');
  showToast(`Replaced ${count} occurrence(s)`);
  runFind();
}

function openFindPanel(withReplace = false) {
  $('find-panel').classList.add('open');
  replaceMode = withReplace;
  $('replace-input').style.display  = withReplace ? '' : 'none';
  $('replace-one-btn').style.display = withReplace ? '' : 'none';
  $('replace-all-btn').style.display = withReplace ? '' : 'none';
  setTimeout(() => $('find-input').focus(), 30);
}

function closeFindPanel() {
  $('find-panel').classList.remove('open');
  clearFindHighlights();
  findMatches  = [];
  findCurrent  = -1;
}

$('find-input').addEventListener('input', runFind);
$('find-regex-btn').addEventListener('click', () => {
  findRegex = !findRegex;
  $('find-regex-btn').classList.toggle('active', findRegex);
  runFind();
});
$('find-case-btn').addEventListener('click', () => {
  findCase = !findCase;
  $('find-case-btn').classList.toggle('active', findCase);
  runFind();
});
$('find-prev-btn').addEventListener('click', () => jumpToMatch(findCurrent - 1));
$('find-next-btn').addEventListener('click', () => jumpToMatch(findCurrent + 1));
$('find-mode-toggle').addEventListener('click', () => openFindPanel(!replaceMode));
$('find-close-btn').addEventListener('click', closeFindPanel);
$('replace-one-btn').addEventListener('click', () => doReplace(false));
$('replace-all-btn').addEventListener('click', () => doReplace(true));
$('find-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') { e.shiftKey ? jumpToMatch(findCurrent - 1) : jumpToMatch(findCurrent + 1); }
  if (e.key === 'Escape') closeFindPanel();
});

// Global keyboard shortcuts
document.addEventListener('keydown', e => {
  if ($('modal-backdrop').classList.contains('open')) return;
  if ((e.ctrlKey || e.metaKey) && e.key === 'f') { e.preventDefault(); openFindPanel(false); }
  if ((e.ctrlKey || e.metaKey) && e.key === 'h') { e.preventDefault(); openFindPanel(true); }
  if (e.key === 'Escape' && $('find-panel').classList.contains('open')) closeFindPanel();
});

// ── settings / dictionary ─────────────────────────────────────────────────────
let dictWords    = [];   // sorted custom-dict words
let dialectAll   = [];   // all dialect entries [{base, target, source}]
let dialectQuery = '';

// ── tab switching ─────────────────────────────────────────────────────────────
document.querySelectorAll('.settings-tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.settings-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.settings-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    $('panel-' + tab.dataset.tab).classList.add('active');
    if (tab.dataset.tab === 'dialect' && !dialectAll.length) loadDialect();
  });
});

async function openSettings() {
  $('settings-backdrop').classList.add('open');
  // Always start on the dict tab
  document.querySelectorAll('.settings-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === 'dict'));
  document.querySelectorAll('.settings-panel').forEach(p => p.classList.toggle('active', p.id === 'panel-dict'));
  dialectAll   = [];
  dialectQuery = '';
  await loadDictionary();
  setTimeout(() => $('dict-add-input').focus(), 60);
}

function closeSettings() { $('settings-backdrop').classList.remove('open'); }

// ── custom dictionary ─────────────────────────────────────────────────────────
async function loadDictionary() {
  const res = await fetch('/api/dictionary');
  if (!res.ok) return;
  dictWords = (await res.json()).words;
  renderDictionary();
}

function renderDictionary() {
  const list = $('dict-list');
  $('dict-stats').textContent = `${dictWords.length} word${dictWords.length !== 1 ? 's' : ''} in custom dictionary`;
  if (!dictWords.length) { list.innerHTML = '<div id="dict-empty">No custom words yet.</div>'; return; }
  list.innerHTML = '';
  dictWords.forEach(word => {
    const item = document.createElement('div');
    item.className = 'dict-item';
    item.innerHTML = `<span>${esc(word)}</span><button title="Remove">✕</button>`;
    item.querySelector('button').addEventListener('click', () => removeDictWord(word));
    list.appendChild(item);
  });
}

async function addDictWord(word) {
  word = word.trim().toLowerCase();
  if (!word) return;
  const res = await fetch('/api/dictionary/add', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ word })
  });
  if (!res.ok) { showToast('Failed to add word', true); return; }
  const data = await res.json();
  if (!dictWords.includes(data.word)) dictWords = [...dictWords, data.word].sort();
  renderDictionary();
  showToast(`"${data.word}" added to dictionary`);
}

async function removeDictWord(word) {
  const res = await fetch('/api/dictionary/remove', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ word })
  });
  if (!res.ok) { showToast('Failed to remove word', true); return; }
  dictWords = dictWords.filter(w => w !== word);
  renderDictionary();
  showToast(`"${word}" removed`);
}

async function onAddToDict(e) {
  const word = e.target.dataset.word;
  const ri   = parseInt(e.target.dataset.ri);
  const ii   = parseInt(e.target.dataset.ii);
  await addDictWord(word);
  removeIssue(ri, ii);
  applyAllHighlights();
}

$('dict-add-btn').addEventListener('click', () => {
  const val = $('dict-add-input').value;
  if (!val.trim()) return;
  addDictWord(val);
  $('dict-add-input').value = '';
});
$('dict-add-input').addEventListener('keydown', e => { if (e.key === 'Enter') $('dict-add-btn').click(); });

// ── dialect map management ────────────────────────────────────────────────────
async function loadDialect() {
  $('dialect-list').innerHTML = '<div style="padding:12px;text-align:center;color:var(--muted);font-size:.82rem">Loading…</div>';
  const res = await fetch('/api/dialect');
  if (!res.ok) { $('dialect-list').innerHTML = '<div style="padding:12px;color:var(--danger);font-size:.82rem">Load failed</div>'; return; }
  const data = await res.json();
  dialectAll = data.entries;
  renderDialect();
}

function renderDialect() {
  const q    = dialectQuery.toLowerCase();
  const list = $('dialect-list');
  const vis  = q ? dialectAll.filter(e => e.base.toLowerCase().includes(q) || e.target.toLowerCase().includes(q)) : dialectAll;

  $('dialect-stats-line').textContent =
    `${vis.length} of ${dialectAll.length} entries shown`;

  if (!vis.length) {
    list.innerHTML = `<div style="padding:14px;text-align:center;color:var(--muted);font-size:.82rem">${q ? 'No matches.' : 'No entries yet.'}</div>`;
    return;
  }
  list.innerHTML = '';
  vis.forEach(entry => {
    const item = document.createElement('div');
    item.className = 'dialect-item';
    item.innerHTML = `
      <span class="di-base">${esc(entry.base)}</span>
      <span class="di-arr">→</span>
      <span class="di-tgt">${esc(entry.target)}</span>
      <span class="di-src">${esc(entry.source)}</span>
      <button data-base="${esc(entry.base)}" title="Remove mapping">✕</button>`;
    item.querySelector('button').addEventListener('click', () => removeDialectEntry(entry.base));
    list.appendChild(item);
  });
}

async function addDialectEntry() {
  const base   = $('dialect-base-input').value.trim();
  const target = $('dialect-target-input').value.trim();
  if (!base || !target) { showToast('Fill in both fields', true); return; }
  const res = await fetch('/api/dialect/add', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ base, target })
  });
  if (!res.ok) { showToast('Failed to add entry', true); return; }
  $('dialect-base-input').value = '';
  $('dialect-target-input').value = '';
  $('dialect-base-input').focus();
  // Reload full list to reflect the server rebuild
  await loadDialect();
  showToast(`"${base}" → "${target}" added`);
}

async function removeDialectEntry(base) {
  if (!confirm(`Remove mapping for "${base}"?`)) return;
  const res = await fetch('/api/dialect/remove', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ base })
  });
  if (!res.ok) { showToast('Remove failed', true); return; }
  dialectAll = dialectAll.filter(e => e.base.toLowerCase() !== base.toLowerCase());
  renderDialect();
  showToast(`"${base}" removed`);
}

$('dialect-add-btn').addEventListener('click', addDialectEntry);
[$('dialect-base-input'), $('dialect-target-input')].forEach(inp =>
  inp.addEventListener('keydown', e => { if (e.key === 'Enter') addDialectEntry(); })
);
$('dialect-filter').addEventListener('input', e => {
  dialectQuery = e.target.value;
  renderDialect();
});

// ── settings open/close ───────────────────────────────────────────────────────
$('settings-btn').addEventListener('click', openSettings);
$('settings-close').addEventListener('click', closeSettings);
$('settings-backdrop').addEventListener('click', e => { if (e.target === $('settings-backdrop')) closeSettings(); });
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && $('settings-backdrop').classList.contains('open')) closeSettings();
});

// ── controls ─────────────────────────────────────────────────────────────────
$('file-select').addEventListener('change', e => { if (e.target.value) loadFile(e.target.value); });
$('search-box').addEventListener('input', e => { state.query = e.target.value; applyFilter(); });
$('prev-btn').addEventListener('click', () => { state.page--; if (state.file) localStorage.setItem('tsv-page:' + state.file, state.page); render(); $('table-wrap').scrollTop = 0; if (state.file) runSpellCheck(); });
$('next-btn').addEventListener('click', () => { state.page++; if (state.file) localStorage.setItem('tsv-page:' + state.file, state.page); render(); $('table-wrap').scrollTop = 0; if (state.file) runSpellCheck(); });
$('page-size').addEventListener('change', e => { state.pageSize = parseInt(e.target.value); applyFilter(); });

// ── toast ─────────────────────────────────────────────────────────────────────
let toastTimer;
function showToast(msg, err = false) {
  const t = $('toast');
  t.textContent      = msg;
  t.style.background = err ? '#742a2a' : '#2d3748';
  t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 1800);
}

// ── DB mode ──────────────────────────────────────────────────────────────────

// Client-side DB state
const db = {
  mode:      false,
  datasets:  [],
  current:   null,       // {id, name, headers: [], row_count}
  rowIds:    [],         // UUID per displayed row (parallel to state.rows)
  rowPos:    [],         // position per displayed row
  pageCache: new Map(),  // `${dsId}:${page}` → {rowIds, rowPos, rows}
  dirty:     new Map(),  // rowId → {position, data}
  deletes:   new Set(),  // rowIds pending delete
};

// ── mode toggle ───────────────────────────────────────────────────────────────
async function enterDbMode() {
  const status = await fetch('/api/db/status').then(r => r.json());
  if (!status.ok) {
    showToast('DB not configured — set SUPABASE_URL and SUPABASE_KEY', true);
    return;
  }
  db.mode = true;
  $('db-mode-btn').classList.add('active');
  $('db-controls').classList.add('show');
  $('file-select').style.display = 'none';
  $('add-row-btn').disabled = true;
  await loadDatasets();
}

function exitDbMode() {
  db.mode = false;
  $('db-mode-btn').classList.remove('active');
  $('db-controls').classList.remove('show');
  $('file-select').style.display = '';
  // Clear DB state from table
  state.file = ''; state.headers = []; state.rows = []; state.filtered = [];
  db.rowIds = []; db.rowPos = [];
  render();
}

$('db-mode-btn').addEventListener('click', () => {
  if (db.mode) exitDbMode(); else enterDbMode();
});

// ── dataset list ──────────────────────────────────────────────────────────────
async function loadDatasets() {
  const res = await fetch('/api/db/datasets');
  if (!res.ok) { showToast('Failed to load datasets', true); return; }
  db.datasets = (await res.json()).datasets || [];
  renderDatasetSelect();
}

function renderDatasetSelect() {
  const sel = $('dataset-select');
  sel.innerHTML = '<option value="">— select dataset —</option>';
  db.datasets.forEach(ds => {
    const o = document.createElement('option');
    o.value = ds.id;
    o.textContent = `${ds.name} (${ds.row_count.toLocaleString()} rows)`;
    sel.appendChild(o);
  });
}

$('dataset-select').addEventListener('change', async e => {
  const id = e.target.value;
  if (!id) return;
  const ds = db.datasets.find(d => d.id === id);
  if (!ds) return;
  if (!ds.headers || !ds.headers.length) {
    openHdrModal(ds, true);
  } else {
    await loadDbDataset(ds);
  }
});

async function loadDbDataset(ds) {
  db.current = ds;
  db.pageCache.clear();
  db.dirty.clear();
  db.deletes.clear();
  $('db-unsaved').classList.remove('show');
  $('export-btn').disabled = false;
  $('add-row-btn').disabled = false;
  state.headers = [...ds.headers];
  state.page    = 0;
  state.query   = '';
  $('search-box').value = '';
  await fetchDbPage(0);
}

async function fetchDbPage(page) {
  const dsId = db.current?.id;
  if (!dsId) return;
  const cacheKey = `${dsId}:${page}`;

  if (db.pageCache.has(cacheKey)) {
    applyDbPage(db.pageCache.get(cacheKey), page);
    return;
  }

  const res = await fetch(`/api/db/datasets/${dsId}/rows?page=${page}&size=${state.pageSize}`);
  if (!res.ok) { showToast('Failed to fetch rows', true); return; }
  const data = await res.json();

  const rowIds = data.rows.map(r => r.id);
  const rowPos = data.rows.map(r => r.position);
  const rows   = data.rows.map(r => (db.current?.headers || []).map(h => r.data[h] ?? ''));

  db.pageCache.set(cacheKey, { rowIds, rowPos, rows });
  applyDbPage({ rowIds, rowPos, rows }, page);
}

function applyDbPage({ rowIds, rowPos, rows }, page) {
  db.rowIds = rowIds;
  db.rowPos  = rowPos;
  // Merge any pending dirty edits into display
  state.rows = rows.map((row, i) => {
    const id = rowIds[i];
    if (db.dirty.has(id)) {
      return (db.current?.headers || []).map(h => db.dirty.get(id).data[h] ?? '');
    }
    return row;
  });
  state.filtered = state.rows.map((r, i) => ({ r, i }));
  state.page     = page;
  $('row-count').textContent = `${db.current?.row_count?.toLocaleString() ?? 0} rows`;
  const totalPages = Math.max(1, Math.ceil((db.current?.row_count || 0) / state.pageSize));
  $('page-info').textContent  = `Page ${page + 1} / ${totalPages}`;
  $('prev-btn').disabled      = page === 0;
  $('next-btn').disabled      = page >= totalPages - 1;
  render();
}

// Override page navigation for DB mode
const _origPrev = $('prev-btn').onclick;
const _origNext = $('next-btn').onclick;
$('prev-btn').addEventListener('click', () => { if (db.mode) { fetchDbPage(state.page - 1); $('table-wrap').scrollTop = 0; } });
$('next-btn').addEventListener('click', () => { if (db.mode) { fetchDbPage(state.page + 1); $('table-wrap').scrollTop = 0; } });

// ── DB cell editing (dirty tracking only — no server call) ────────────────────
function onCellBlurDb(e) {
  const td  = e.target;
  const row = parseInt(td.dataset.row);
  const col = parseInt(td.dataset.col);
  const val = td.textContent;
  if (val === state.rows[row]?.[col]) return;
  state.rows[row][col] = val;
  td.closest('tr')?.classList.add('changed');

  const id = db.rowIds[row];
  if (!id) return;
  const current = db.dirty.get(id) || { position: db.rowPos[row], data: {} };
  current.data = Object.fromEntries(state.headers.map((h, i) => [h, state.rows[row][i]]));
  db.dirty.set(id, current);

  // Invalidate this page's cache so re-visit shows latest
  db.pageCache.delete(`${db.current?.id}:${state.page}`);

  $('db-unsaved').classList.add('show');
  setSaveState('unsaved');
}

// ── DB row add ────────────────────────────────────────────────────────────────
async function addDbRow() {
  if (!db.current) return;
  const res = await fetch(`/api/db/datasets/${db.current.id}/import`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ rows: [Object.fromEntries(state.headers.map(h => [h, '']))] })
  });
  if (!res.ok) { showToast('Add row failed', true); return; }
  db.current.row_count++;
  db.pageCache.clear();
  const lastPage = Math.ceil(db.current.row_count / state.pageSize) - 1;
  await fetchDbPage(lastPage);
  showToast('Row added');
}

// ── DB row delete ─────────────────────────────────────────────────────────────
async function deleteDbRow(rowIdx) {
  const id = db.rowIds[rowIdx];
  if (!id) return;
  if (!confirm(`Delete row ${rowIdx + 1}?`)) return;
  db.deletes.add(id);
  db.dirty.delete(id);
  await flushDbDeletes();
}

async function flushDbDeletes() {
  if (!db.deletes.size || !db.current) return;
  const ids = [...db.deletes];
  const res = await fetch(`/api/db/datasets/${db.current.id}/rows/delete`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ids })
  });
  if (!res.ok) { showToast('Delete failed', true); return; }
  db.deletes.clear();
  db.current.row_count = Math.max(0, db.current.row_count - ids.length);
  db.pageCache.clear();
  await fetchDbPage(state.page);
  showToast('Deleted');
}

// ── Save / sync dirty ─────────────────────────────────────────────────────────
async function saveDbDirty() {
  if (!db.current) return;
  if (!db.dirty.size && !db.deletes.size) { setSaveState('saved'); return; }
  setSaveState('saving');

  if (db.dirty.size) {
    const payload = [...db.dirty.entries()].map(([id, { position, data }]) => ({ id, position, data }));
    const res = await fetch(`/api/db/datasets/${db.current.id}/rows/save`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rows: payload })
    });
    if (res.ok) {
      db.dirty.clear();
      $('db-unsaved').classList.remove('show');
      showToast(`Synced ${payload.length} row(s)`);
    } else {
      showToast('Sync failed', true);
    }
  }
  if (db.deletes.size) await flushDbDeletes();
  setSaveState('saved');
}

// Hook Ctrl+S into DB save when in DB mode
document.addEventListener('keydown', e => {
  if (db.mode && (e.ctrlKey || e.metaKey) && e.key === 's') {
    e.preventDefault();
    saveDbDirty();
  }
});

// ── Override cell blur + delete in DB mode ────────────────────────────────────
// Monkey-patch the existing handlers to check db.mode
const _origOnCellBlur = onCellBlur;
function onCellBlur(e) {
  if (db.mode) { onCellBlurDb(e); return; }
  _origOnCellBlur(e);
}

const _origOnDeleteRow = onDeleteRow;
async function onDeleteRow(e) {
  if (db.mode) { await deleteDbRow(parseInt(e.target.dataset.row)); return; }
  await _origOnDeleteRow(e);
}

// ── New dataset ───────────────────────────────────────────────────────────────
$('new-dataset-btn').addEventListener('click', async () => {
  const name = prompt('Dataset name:');
  if (!name?.trim()) return;
  const res = await fetch('/api/db/datasets', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: name.trim(), headers: [] })
  });
  if (!res.ok) { showToast('Create failed', true); return; }
  const { dataset } = await res.json();
  db.datasets.unshift(dataset);
  renderDatasetSelect();
  $('dataset-select').value = dataset.id;
  openHdrModal(dataset, true);
});

// ── Export ────────────────────────────────────────────────────────────────────
$('export-btn').addEventListener('click', () => {
  if (!db.current) return;
  window.location.href = `/api/db/datasets/${db.current.id}/export`;
});

// ── Header config modal ───────────────────────────────────────────────────────
let _hdrCallback = null;

function openHdrModal(ds, required = false) {
  $('hdr-modal-title').textContent = required
    ? 'Configure Columns — required before viewing data'
    : `Edit Columns — ${ds.name}`;
  $('hdr-cancel-btn').style.display = required ? 'none' : '';
  $('hdr-list').innerHTML = '';
  const hdrs = ds.headers?.length ? [...ds.headers] : [''];
  hdrs.forEach(h => addHdrRow(h));
  $('hdr-backdrop').classList.add('open');
  _hdrCallback = async (newHeaders) => {
    const res = await fetch(`/api/db/datasets/${ds.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ headers: newHeaders })
    });
    if (!res.ok) { showToast('Save failed', true); return; }
    ds.headers = newHeaders;
    const updated = db.datasets.find(d => d.id === ds.id);
    if (updated) updated.headers = newHeaders;
    showToast('Columns saved');
    if (required) await loadDbDataset(ds);
  };
}

function addHdrRow(val = '') {
  const row = document.createElement('div');
  row.className = 'hdr-row';
  row.innerHTML = `<input type="text" placeholder="Column name" value="${esc(val)}"><button title="Remove">✕</button>`;
  row.querySelector('button').addEventListener('click', () => row.remove());
  $('hdr-list').appendChild(row);
  row.querySelector('input').focus();
}

$('hdr-add-btn').addEventListener('click', () => addHdrRow());
$('hdr-cancel-btn').addEventListener('click', () => $('hdr-backdrop').classList.remove('open'));
$('hdr-save-btn').addEventListener('click', async () => {
  const inputs = [...$('hdr-list').querySelectorAll('input')];
  const headers = inputs.map(i => i.value.trim()).filter(Boolean);
  if (!headers.length) { showToast('Add at least one column', true); return; }
  $('hdr-backdrop').classList.remove('open');
  if (_hdrCallback) await _hdrCallback(headers);
});

// ── Import modal ──────────────────────────────────────────────────────────────
let _importParsed = { headers: [], rows: [] };

function openImportModal() {
  if (!db.current) { showToast('Select a dataset first', true); return; }
  $('drop-zone').innerHTML = '<div style="font-size:2rem">📂</div><strong>Drop a TSV file here</strong><p>or click to browse</p><input type="file" id="import-file-input" accept=".tsv,.txt,.csv" style="display:none">';
  $('import-options').style.display = 'none';
  $('col-map-wrap').style.display = 'none';
  $('import-progress-bar').classList.remove('show');
  $('import-progress-fill').style.width = '0%';
  $('import-do-btn').disabled = true;
  $('import-status').textContent = 'Select a file to begin.';
  $('import-backdrop').classList.add('open');
  rewireDropZone();
}

function rewireDropZone() {
  const dz = $('drop-zone');
  dz.addEventListener('click', () => dz.querySelector('input')?.click());
  dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('drag-over'); });
  dz.addEventListener('dragleave', () => dz.classList.remove('drag-over'));
  dz.addEventListener('drop', e => {
    e.preventDefault(); dz.classList.remove('drag-over');
    const f = e.dataTransfer.files[0];
    if (f) handleImportFile(f);
  });
  const fi = dz.querySelector('input');
  if (fi) fi.addEventListener('change', e => { if (e.target.files[0]) handleImportFile(e.target.files[0]); });
}

async function handleImportFile(file) {
  $('import-file-name').textContent = file.name;
  const text  = await file.text();
  const lines = text.split('\n').map(l => l.trimEnd()).filter(l => l);
  if (!lines.length) { $('import-status').textContent = 'File is empty.'; return; }

  const allRows = lines.map(l => l.split('\t'));
  _importParsed = { allRows };

  $('import-options').style.display = 'flex';
  renderColMapping(allRows);
}

function renderColMapping(allRows) {
  const hasHeader = $('import-has-header').checked;
  const fileHeaders = hasHeader ? allRows[0] : allRows[0].map((_, i) => `Column ${i + 1}`);
  const sampleRows  = allRows.slice(hasHeader ? 1 : 0, hasHeader ? 6 : 5);
  const dsHeaders   = db.current?.headers || [];

  const tbody = $('col-map-body');
  tbody.innerHTML = '';
  fileHeaders.forEach((fh, ci) => {
    const samples = sampleRows.map(r => r[ci] ?? '').filter(Boolean).slice(0, 5);
    // Auto-match: if file header name == dataset header name
    const autoMatch = dsHeaders.findIndex(h => h.toLowerCase() === fh.toLowerCase());

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><strong>${esc(fh)}</strong></td>
      <td class="sample-cell" title="${esc(samples.join(' | '))}">${esc(samples.join(', '))}</td>
      <td>
        <select class="col-map-select" data-file-col="${ci}">
          <option value="">— Skip —</option>
          ${dsHeaders.map((h, i) => `<option value="${esc(h)}" ${i === autoMatch ? 'selected' : ''}>${esc(h)}</option>`).join('')}
        </select>
      </td>`;
    tbody.appendChild(tr);
  });

  $('col-map-wrap').style.display = '';
  $('import-do-btn').disabled = false;
  $('import-status').textContent = `${allRows.length - (hasHeader ? 1 : 0)} rows ready to import.`;
}

$('import-has-header').addEventListener('change', () => {
  if (_importParsed.allRows) renderColMapping(_importParsed.allRows);
});

$('import-do-btn').addEventListener('click', async () => {
  const { allRows } = _importParsed;
  if (!allRows || !db.current) return;

  const hasHeader = $('import-has-header').checked;
  const dataRows  = allRows.slice(hasHeader ? 1 : 0);
  const mapping   = [...$('col-map-body').querySelectorAll('.col-map-select')]
    .map(s => ({ fileCol: parseInt(s.dataset.fileCol), dsCol: s.value }))
    .filter(m => m.dsCol);

  if (!mapping.length) { showToast('Map at least one column', true); return; }

  const mapped = dataRows.map(row => Object.fromEntries(mapping.map(m => [m.dsCol, row[m.fileCol] ?? ''])));

  $('import-do-btn').disabled = true;
  $('import-progress-bar').classList.add('show');

  const batchSize = 500;
  let inserted = 0;
  for (let i = 0; i < mapped.length; i += batchSize) {
    const chunk = mapped.slice(i, i + batchSize);
    const res = await fetch(`/api/db/datasets/${db.current.id}/import`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rows: chunk })
    });
    if (!res.ok) { showToast('Import failed at batch ' + i, true); break; }
    inserted += chunk.length;
    $('import-progress-fill').style.width = `${Math.round(inserted / mapped.length * 100)}%`;
    $('import-status').textContent = `Importing… ${inserted.toLocaleString()} / ${mapped.length.toLocaleString()}`;
  }

  $('import-do-btn').disabled = false;
  $('import-status').textContent = `✓ Imported ${inserted.toLocaleString()} rows.`;
  db.current.row_count += inserted;
  db.pageCache.clear();
  renderDatasetSelect();
  showToast(`Imported ${inserted.toLocaleString()} rows`);
  await fetchDbPage(0);
});

$('import-btn').addEventListener('click', openImportModal);
$('import-close').addEventListener('click', () => $('import-backdrop').classList.remove('open'));
$('import-backdrop').addEventListener('click', e => { if (e.target === $('import-backdrop')) $('import-backdrop').classList.remove('open'); });

// ── init ─────────────────────────────────────────────────────────────────────
loadFiles();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("TSV Editor running at http://localhost:5000")
    print(f"Loaded {len(DIALECT_MAP)} Hiligaynon→Ilonggo dialect mappings.")
    app.run(debug=True, port=5000)
