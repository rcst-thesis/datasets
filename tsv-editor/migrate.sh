#!/usr/bin/env bash
set -e

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ ! -f .env ]; then
  echo "[error] .env file not found"
  exit 1
fi
export $(grep -v '^#' .env | xargs)

if [ -z "$SUPABASE_DB_URL" ]; then
  echo "[error] SUPABASE_DB_URL not set in .env"
  exit 1
fi

# ── Run all migrations in order ───────────────────────────────────────────────
MIGRATIONS_DIR="$(dirname "$0")/migrations"

if [ ! -d "$MIGRATIONS_DIR" ]; then
  echo "[error] migrations/ directory not found"
  exit 1
fi

FILES=$(ls "$MIGRATIONS_DIR"/*.sql 2>/dev/null | sort)

if [ -z "$FILES" ]; then
  echo "[info] No migration files found in migrations/"
  exit 0
fi

python3 - <<EOF
import psycopg2, os, glob, sys

url   = os.environ["SUPABASE_DB_URL"]
mdir  = os.path.join(os.path.dirname(os.path.abspath("$0")), "migrations")
files = sorted(glob.glob(os.path.join(mdir, "*.sql")))

if not files:
    print("[info] No migration files found.")
    sys.exit(0)

try:
    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur  = conn.cursor()

    # Track which migrations have already run
    cur.execute("""
        create table if not exists _migrations (
            filename   text primary key,
            applied_at timestamptz default now()
        )
    """)

    for fpath in files:
        fname = os.path.basename(fpath)
        cur.execute("select 1 from _migrations where filename = %s", (fname,))
        if cur.fetchone():
            print(f"[skip] {fname} (already applied)")
            continue
        print(f"[run]  {fname} ...", end=" ", flush=True)
        with open(fpath, encoding="utf-8") as f:
            sql = f.read()
        cur.execute(sql)
        cur.execute("insert into _migrations (filename) values (%s)", (fname,))
        print("done")

    conn.close()
    print("[info] All migrations complete.")

except Exception as e:
    print(f"[error] {e}")
    sys.exit(1)
EOF