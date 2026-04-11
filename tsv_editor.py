"""
TSV Editor — Flask web app for viewing and editing parallel-corpus TSV files.
Run:  python tsv_editor.py
Then open http://localhost:5000
"""

import csv
import os
from pathlib import Path
from flask import Flask, jsonify, render_template_string, request

BASE_DIR = Path(__file__).parent
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
        resolved.relative_to(BASE_DIR.resolve())  # raises ValueError if outside
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
    # Normalise row widths
    ncols = len(headers)
    data = [r + [""] * (ncols - len(r)) if len(r) < ncols else r[:ncols] for r in data]
    return headers, data


def write_tsv(path: Path, headers: list[str], data: list[list[str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(headers)
        writer.writerows(data)


# ── routes ───────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.get("/api/files")
def api_files():
    return jsonify(find_tsv_files())


@app.get("/api/tsv")
def api_get_tsv():
    rel = request.args.get("path", "")
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
  font-size: 14px;
}
body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }

/* ── top bar ── */
header { display: flex; align-items: center; gap: 12px; padding: 10px 16px; background: var(--surface); border-bottom: 1px solid var(--border); flex-shrink: 0; }
header h1 { font-size: 1.05rem; font-weight: 600; color: var(--accent); white-space: nowrap; }
#file-select { flex: 1; max-width: 420px; background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 6px; padding: 5px 10px; font-size: 0.9rem; }
#search-box { flex: 1; max-width: 260px; background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 6px; padding: 5px 10px; font-size: 0.9rem; }
#search-box::placeholder { color: var(--muted); }
.badge { background: var(--border); color: var(--muted); border-radius: 999px; padding: 2px 10px; font-size: 0.78rem; white-space: nowrap; }
.btn { padding: 5px 14px; border-radius: 6px; border: none; cursor: pointer; font-size: 0.85rem; font-weight: 500; transition: opacity .15s; }
.btn:hover { opacity: .8; }
.btn-primary { background: var(--accent); color: #fff; }
.btn-danger  { background: var(--danger); color: #fff; }

/* ── pagination ── */
#pager { display: flex; align-items: center; gap: 8px; padding: 6px 16px; background: var(--surface); border-bottom: 1px solid var(--border); flex-shrink: 0; }
#pager span { color: var(--muted); font-size: 0.82rem; }
#pager button { background: var(--border); color: var(--text); border: none; border-radius: 5px; padding: 3px 10px; cursor: pointer; font-size: 0.82rem; }
#pager button:disabled { opacity: .35; cursor: default; }
#page-size { background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 5px; padding: 2px 6px; font-size: 0.82rem; }

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
td.cell-editable { cursor: text; white-space: pre-wrap; min-height: 28px; }
td.cell-editable:focus { outline: 2px solid var(--accent); background: var(--edit-bg); border-radius: 3px; }
td.cell-editable.saving { opacity: .5; }
td.col-actions { text-align: center; }
.del-btn { background: transparent; border: none; cursor: pointer; color: var(--muted); font-size: 1rem; line-height: 1; padding: 2px 5px; border-radius: 4px; }
.del-btn:hover { color: var(--danger); background: rgba(252,129,129,.1); }
#empty-msg { padding: 60px; text-align: center; color: var(--muted); }
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

<div id="table-wrap">
  <div id="empty-msg">Select a TSV file to begin.</div>
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
let saveTimer = null;

// ── file list ────────────────────────────────────────────────────────────────
async function loadFiles() {
  const res = await fetch('/api/files');
  const files = await res.json();
  const sel = $('file-select');
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
  state.file = path;
  state.headers = data.headers;
  state.rows = data.rows;
  state.page = 0;
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
  const wrap = $('table-wrap');
  const total = state.filtered.length;
  const pages = Math.max(1, Math.ceil(total / state.pageSize));
  $('row-count').textContent = total + ' rows';
  $('page-info').textContent = `Page ${state.page + 1} / ${pages}`;
  $('prev-btn').disabled = state.page === 0;
  $('next-btn').disabled = state.page >= pages - 1;

  if (!state.headers.length) { wrap.innerHTML = '<div id="empty-msg">No data.</div>'; return; }

  const colW = Math.floor(88 / state.headers.length);
  let html = `<table><thead><tr><th class="col-row-num">#</th>`;
  state.headers.forEach(h => { html += `<th style="width:${colW}%">${esc(h)}</th>`; });
  html += `<th class="col-actions"></th></tr></thead><tbody>`;

  pageSlice().forEach(({ r, i }) => {
    html += `<tr data-row="${i}">`;
    html += `<td class="row-num" data-row="${i}" title="Open row editor">${i + 1}</td>`;
    r.forEach((cell, j) => {
      html += `<td class="cell-editable" contenteditable="true" data-row="${i}" data-col="${j}" spellcheck="true">${esc(cell)}</td>`;
    });
    html += `<td class="col-actions"><button class="del-btn" title="Delete row" data-row="${i}">✕</button></td>`;
    html += `</tr>`;
  });

  html += `</tbody></table>`;
  wrap.innerHTML = html;

  // attach events
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
    const data = await res.json();
    state.rows.push(Array(state.headers.length).fill(''));
    state.page = Math.ceil(state.rows.length / state.pageSize) - 1;
    applyFilter();
    // focus last row first cell
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
  $('modal-title').textContent = `Row ${modalRow + 1}`;
  $('modal-pos').textContent = `${modalRow + 1} / ${state.rows.length}`;
  $('modal-prev').disabled = modalRow <= 0;
  $('modal-next').disabled = modalRow >= state.rows.length - 1;
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
  const pending = [];
  textareas.forEach(ta => {
    const col = parseInt(ta.dataset.col);
    const val = ta.value;
    if (val !== state.rows[modalRow][col]) pending.push({ col, val });
  });
  if (!pending.length) { $('modal-status').textContent = 'No changes.'; return; }

  $('modal-save-btn').disabled = true;
  $('modal-status').textContent = 'Saving…';
  let failed = 0;
  for (const { col, val } of pending) {
    const res = await fetch('/api/tsv/cell', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: state.file, row: modalRow, col, value: val })
    });
    if (res.ok) {
      state.rows[modalRow][col] = val;
    } else { failed++; }
  }
  $('modal-save-btn').disabled = false;
  if (failed) {
    $('modal-status').textContent = `${failed} field(s) failed to save.`;
  } else {
    $('modal-status').textContent = `Saved ${pending.length} field(s).`;
    // reflect changes in the table without full re-render
    const tr = document.querySelector(`tr[data-row="${modalRow}"]`);
    if (tr) {
      tr.classList.add('changed');
      tr.querySelectorAll('.cell-editable').forEach(td => {
        const col = parseInt(td.dataset.col);
        td.textContent = state.rows[modalRow][col];
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
  if (e.key === 'ArrowLeft' && e.altKey) { $('modal-prev').click(); e.preventDefault(); }
  if (e.key === 'ArrowRight' && e.altKey) { $('modal-next').click(); e.preventDefault(); }
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
  t.textContent = msg;
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
    app.run(debug=True, port=5000)
