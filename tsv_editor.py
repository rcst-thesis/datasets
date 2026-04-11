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


def build_tagalog_map() -> dict[str, str]:
    """
    Load {normalized_tagalog_word: hiligaynon_replacement} from
    spell-checker/words.csv and spell-checker/verbs.csv.
    """
    mapping: dict[str, str] = {}
    for fname in ("words.csv", "verbs.csv"):
        fp = SPELL_DIR / fname
        if not fp.exists():
            continue
        with open(fp, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                base   = strip_diacritics(row["base_word"].strip())
                target = row["target_word"].strip()
                if base and target:
                    mapping[base] = target
    return mapping


# Load once at startup
TAGALOG_MAP: dict[str, str] = build_tagalog_map()


def spellcheck_text(text: str) -> list[dict]:
    """
    Check `text` for Tagalog words that should be Hiligaynon.
    Returns list of {word, start, end, suggestion, type}.
    """
    issues = []
    for m in re.finditer(r"[\w'-]+", text):
        word = m.group()
        # Skip numbers, very short tokens, and proper nouns (capitalized)
        if word.isdigit() or len(word) < 2 or word[0].isupper():
            continue
        norm = strip_diacritics(word)
        if norm in TAGALOG_MAP:
            issues.append({
                "word":       word,
                "start":      m.start(),
                "end":        m.end(),
                "suggestion": TAGALOG_MAP[norm],
                "type":       "tagalog",
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


@app.post("/api/spellcheck/file")
def api_spellcheck_file():
    """
    Run spellcheck on the HIL column of a TSV file.
    Returns {hil_col, issues: [{row, col, cell, issues: [...]}, ...]}.
    """
    body = request.json or {}
    rel  = body.get("path", "")
    path = safe_path(rel)
    if not path or not path.exists():
        return jsonify({"error": "File not found"}), 404

    headers, data = read_tsv(path)

    # Find HIL column (case-insensitive match on "hil")
    hil_col = next(
        (i for i, h in enumerate(headers) if "hil" in h.lower()),
        None
    )
    if hil_col is None:
        return jsonify({"hil_col": None, "issues": []})

    all_issues = []
    for row_idx, row in enumerate(data):
        cell = row[hil_col] if hil_col < len(row) else ""
        cell_issues = spellcheck_text(cell)
        if cell_issues:
            all_issues.append({
                "row":    row_idx,
                "col":    hil_col,
                "cell":   cell,
                "issues": cell_issues,
            })

    return jsonify({"hil_col": hil_col, "issues": all_issues})


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
body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }

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
thead th.col-actions { width: 52px; }
tbody tr:hover { background: var(--row-hover); }
tbody tr.changed { border-left: 3px solid var(--accent2); }
td { padding: 5px 10px; border-bottom: 1px solid var(--border); vertical-align: top; word-break: break-word; }
td.row-num { color: var(--muted); font-size: 0.78rem; text-align: right; user-select: none; cursor: pointer; border-radius: 4px; transition: color .15s, background .15s; }
td.row-num:hover { color: var(--accent); background: rgba(108,99,255,.12); }
td.cell-editable { cursor: text; white-space: pre-wrap; min-height: 28px; }
td.cell-editable:focus { outline: 2px solid var(--accent); background: var(--edit-bg); border-radius: 3px; }
td.cell-editable.saving { opacity: .5; }
td.col-actions { text-align: center; }
.del-btn { background: transparent; border: none; cursor: pointer; color: var(--muted); font-size: 1rem; line-height: 1; padding: 2px 5px; border-radius: 4px; }
.del-btn:hover { color: var(--danger); background: rgba(252,129,129,.1); }
#empty-msg { padding: 60px; text-align: center; color: var(--muted); }

/* ── spell issue cell highlight ── */
td.hil-cell-issue { box-shadow: inset 3px 0 0 var(--spell-warn); background: rgba(246,173,85,.05); }
td.hil-cell-issue:hover { background: rgba(246,173,85,.1); }

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
#spell-issue-list { flex: 1; overflow-y: auto; padding: 8px; min-width: 300px; }
.spell-card { background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; transition: border-color .15s; }
.spell-card:hover { border-color: var(--spell-warn); }
.spell-card .row-ref { font-size: 0.7rem; color: var(--muted); margin-bottom: 5px; }
.spell-card .issue-body { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; font-size: 0.85rem; }
.spell-card .word-error { color: var(--danger); text-decoration: underline wavy var(--danger); text-underline-offset: 3px; }
.spell-card .arrow { color: var(--muted); }
.spell-card .word-fix { color: var(--spell-fix); font-weight: 500; }
.spell-card .card-actions { display: flex; gap: 6px; margin-top: 8px; }
.accept-btn { flex: 1; padding: 4px 8px; border-radius: 5px; border: none; cursor: pointer; font-size: 0.75rem; font-weight: 500; background: rgba(104,211,145,.18); color: var(--spell-fix); transition: background .15s; }
.accept-btn:hover { background: rgba(104,211,145,.35); }
.reject-btn { flex: 1; padding: 4px 8px; border-radius: 5px; border: none; cursor: pointer; font-size: 0.75rem; background: var(--border); color: var(--muted); transition: color .15s; }
.reject-btn:hover { color: var(--text); }
#spell-empty { padding: 24px 16px; text-align: center; color: var(--muted); font-size: 0.82rem; }

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

/* ── toast ── */
.toast { position: fixed; bottom: 20px; right: 20px; background: #2d3748; color: #fff; padding: 10px 18px; border-radius: 8px; font-size: 0.85rem; opacity: 0; transition: opacity .25s; pointer-events: none; z-index: 100; }
.toast.show { opacity: 1; }
</style>
</head>
<body>

<header>
  <h1>TSV Editor</h1>
  <select id="file-select"><option value="">— select a file —</option></select>
  <input id="search-box" type="search" placeholder="Search rows…">
  <span id="row-count" class="badge">0 rows</span>
  <button class="btn btn-primary" id="add-row-btn" disabled>+ Row</button>
  <button class="btn btn-spell" id="spell-toggle-btn">Spell Check</button>
</header>

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

<div id="main-content">
  <div id="table-wrap">
    <div id="empty-msg">Select a TSV file to begin.</div>
  </div>

  <div id="spell-sidebar" class="hidden">
    <div id="spell-sidebar-header">
      <h2>Spell Check — HIL</h2>
      <button id="run-check-btn">Run Check</button>
      <button id="spell-close-btn" title="Close sidebar">✕</button>
    </div>
    <div id="spell-status">Load a file and run check.</div>
    <div id="spell-issue-list">
      <div id="spell-empty">Run a check to see suggestions.</div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

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

// ── spell check state ─────────────────────────────────────────────────────────
let spellIssues = [];   // [{row, col, cell, issues:[{word,start,end,suggestion,type}]}]
let hilCol = null;

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
  state.page    = 0;
  // Reset spell state on new file
  spellIssues = [];
  hilCol = null;
  $('spell-status').textContent = 'Load a file and run check.';
  $('spell-issue-list').innerHTML = '<div id="spell-empty">Run a check to see suggestions.</div>';
  applyFilter();
  $('add-row-btn').disabled = false;
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
  const colW = Math.floor(88 / state.headers.length);
  let html = `<table><thead><tr><th class="col-row-num">#</th>`;
  state.headers.forEach(h => { html += `<th style="width:${colW}%">${esc(h)}</th>`; });
  html += `<th class="col-actions"></th></tr></thead><tbody>`;

  pageSlice().forEach(({ r, i }) => {
    html += `<tr data-row="${i}">`;
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
    td.addEventListener('blur', onCellBlur);
    td.addEventListener('keydown', onCellKeydown);
  });
  wrap.querySelectorAll('.del-btn').forEach(btn => {
    btn.addEventListener('click', onDeleteRow);
  });
  wrap.querySelectorAll('td.row-num').forEach(td => {
    td.addEventListener('click', () => openModal(parseInt(td.dataset.row)));
  });
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── cell editing ─────────────────────────────────────────────────────────────
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

// ── spell check ───────────────────────────────────────────────────────────────

function toggleSpellSidebar() {
  const sidebar = $('spell-sidebar');
  const isHidden = sidebar.classList.toggle('hidden');
  $('spell-toggle-btn').classList.toggle('active', !isHidden);
}

async function runSpellCheck() {
  if (!state.file) { showToast('Load a file first', true); return; }
  $('run-check-btn').disabled   = true;
  $('spell-status').textContent = 'Checking…';
  $('spell-issue-list').innerHTML = '';

  const res = await fetch('/api/spellcheck/file', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: state.file })
  });
  $('run-check-btn').disabled = false;

  if (!res.ok) { showToast('Spell check failed', true); $('spell-status').textContent = 'Check failed.'; return; }

  const data = await res.json();
  hilCol      = data.hil_col;
  spellIssues = data.issues;

  if (hilCol === null) {
    $('spell-status').textContent = 'No HIL column found in this file.';
    $('spell-issue-list').innerHTML = '<div id="spell-empty">No HIL column detected.</div>';
    return;
  }

  renderSpellIssues();
  render(); // Re-render table to apply cell highlights
}

function renderSpellIssues() {
  const list  = $('spell-issue-list');
  const total = spellIssues.reduce((s, r) => s + r.issues.length, 0);

  if (total === 0) {
    $('spell-status').textContent = 'No issues found — HIL column looks good!';
    list.innerHTML = '<div id="spell-empty" style="padding:24px 16px;text-align:center;color:var(--spell-fix);font-size:.85rem">All clear!</div>';
    return;
  }

  $('spell-status').textContent = `${total} issue${total !== 1 ? 's' : ''} in ${spellIssues.length} row${spellIssues.length !== 1 ? 's' : ''}`;
  list.innerHTML = '';

  spellIssues.forEach((rowIssue, ri) => {
    rowIssue.issues.forEach((issue, ii) => {
      const card = document.createElement('div');
      card.className = 'spell-card';
      card.innerHTML = `
        <div class="row-ref">Row ${rowIssue.row + 1} &mdash; HIL column</div>
        <div class="issue-body">
          <span class="word-error">${esc(issue.word)}</span>
          <span class="arrow">&#8594;</span>
          <span class="word-fix">${esc(issue.suggestion)}</span>
        </div>
        <div class="card-actions">
          <button class="accept-btn" data-ri="${ri}" data-ii="${ii}">Accept</button>
          <button class="reject-btn" data-ri="${ri}" data-ii="${ii}">Ignore</button>
        </div>`;
      list.appendChild(card);
    });
  });

  list.querySelectorAll('.accept-btn').forEach(btn => btn.addEventListener('click', onAcceptIssue));
  list.querySelectorAll('.reject-btn').forEach(btn => btn.addEventListener('click', onRejectIssue));
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
  // Replace all occurrences of the Tagalog word (word-boundary aware)
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
  if (rowIssue.issues.length === 0) {
    // Remove cell highlight
    const td = document.querySelector(
      `td.cell-editable[data-row="${rowIssue.row}"][data-col="${rowIssue.col}"]`
    );
    if (td) td.classList.remove('hil-cell-issue');
    spellIssues.splice(ri, 1);
  }
  renderSpellIssues();
}

$('spell-toggle-btn').addEventListener('click', toggleSpellSidebar);
$('run-check-btn').addEventListener('click', runSpellCheck);
$('spell-close-btn').addEventListener('click', () => {
  $('spell-sidebar').classList.add('hidden');
  $('spell-toggle-btn').classList.remove('active');
});

// ── controls ─────────────────────────────────────────────────────────────────
$('file-select').addEventListener('change', e => { if (e.target.value) loadFile(e.target.value); });
$('search-box').addEventListener('input', e => { state.query = e.target.value; applyFilter(); });
$('prev-btn').addEventListener('click', () => { state.page--; render(); $('table-wrap').scrollTop = 0; });
$('next-btn').addEventListener('click', () => { state.page++; render(); $('table-wrap').scrollTop = 0; });
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

// ── init ─────────────────────────────────────────────────────────────────────
loadFiles();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("TSV Editor running at http://localhost:5000")
    print(f"Loaded {len(TAGALOG_MAP)} Tagalog→Hiligaynon mappings for spell check.")
    app.run(debug=True, port=5000)
