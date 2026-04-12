-- ── TSV Editor — initial schema ──────────────────────────────────────────────
-- Run once against your Supabase project:
--   psql "$SUPABASE_DB_URL" -f migrations/001_initial.sql

-- One "dataset" = one parallel corpus project with configurable columns
CREATE TABLE IF NOT EXISTS datasets (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT        NOT NULL,
  headers     JSONB       NOT NULL DEFAULT '[]',   -- ["en","hil"] ordered array
  row_count   INTEGER     NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Each row is a keyed JSON object: {"en": "Hello", "hil": "Kamusta"}
CREATE TABLE IF NOT EXISTS corpus_rows (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id  UUID        NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
  position    INTEGER     NOT NULL,
  data        JSONB       NOT NULL DEFAULT '{}',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast paginated reads
CREATE UNIQUE INDEX IF NOT EXISTS idx_corpus_rows_pos
  ON corpus_rows (dataset_id, position);

CREATE INDEX IF NOT EXISTS idx_corpus_rows_dataset
  ON corpus_rows (dataset_id);

-- Keep datasets.row_count consistent (statement-level for bulk efficiency)
CREATE OR REPLACE FUNCTION _sync_row_count()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  UPDATE datasets
  SET row_count = (
    SELECT COUNT(*) FROM corpus_rows WHERE dataset_id =
      CASE WHEN TG_OP = 'DELETE' THEN OLD.dataset_id ELSE NEW.dataset_id END
  ),
  updated_at = NOW()
  WHERE id =
    CASE WHEN TG_OP = 'DELETE' THEN OLD.dataset_id ELSE NEW.dataset_id END;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_row_count ON corpus_rows;
CREATE TRIGGER trg_sync_row_count
AFTER INSERT OR DELETE ON corpus_rows
FOR EACH ROW EXECUTE FUNCTION _sync_row_count();

-- Row-level security (enable when you add auth)
ALTER TABLE datasets    ENABLE ROW LEVEL SECURITY;
ALTER TABLE corpus_rows ENABLE ROW LEVEL SECURITY;

-- For now: allow all (replace with proper policies when you add auth)
DROP POLICY IF EXISTS "allow_all_datasets"    ON datasets;
DROP POLICY IF EXISTS "allow_all_corpus_rows" ON corpus_rows;
CREATE POLICY "allow_all_datasets"    ON datasets    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "allow_all_corpus_rows" ON corpus_rows FOR ALL USING (true) WITH CHECK (true);
