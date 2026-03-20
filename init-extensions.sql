-- ── HF Group CDC — Extension Setup ──────────────────────────────────────────
-- This script runs automatically on first container start (00_extensions.sql).
-- It must complete before your schema DDL runs (01_schema.sql).

-- pg_partman: automated partition lifecycle management
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- pg_trgm: trigram indexes for fast fuzzy/ILIKE name search on customers
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- pg_cron: schedule partition maintenance jobs inside Postgres
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Verify all three loaded correctly
DO $$
DECLARE
    missing TEXT := '';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_partman') THEN
        missing := missing || ' pg_partman';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        missing := missing || ' pg_trgm';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        missing := missing || ' pg_cron';
    END IF;

    IF missing <> '' THEN
        RAISE EXCEPTION 'Missing extensions: %', missing;
    END IF;

    RAISE NOTICE 'All extensions loaded: pg_partman, pg_trgm, pg_cron';
END $$;
