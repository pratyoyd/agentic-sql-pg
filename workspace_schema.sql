-- =============================================================================
-- workspace_schema.sql
-- Session-local cooperative catalog for agent-database optimization PoC.
--
-- This stub implements the M1 mechanism (named result handles + catalog)
-- described in the agent-led, database-cooperative optimization project.
-- It is deliberately minimal: no GC, no cost-based decisions, no canonical
-- matching. The goal is to measure whether a prompt-engineered agent will
-- spontaneously use a save/catalog/drop API when it's available, not to
-- demonstrate the final mechanism.
--
-- Session semantics:
--   - All stored intermediates are TEMP TABLES and vanish at session end.
--   - The metadata table `workspace.registry` is also a TEMP TABLE, created
--     lazily on first use per session.
--   - No cross-session state. No persistent writes.
--
-- Usage from the agent:
--   SELECT * FROM workspace.catalog();
--   SELECT workspace.save('franchise_base', 'desc', $$SELECT ...$$);
--   SELECT workspace.drop_entry('franchise_base');
--
-- Note: the drop function is `workspace.drop_entry` not `workspace.drop`
-- because DROP is a reserved keyword and makes PL/pgSQL fussy. The agent
-- prompt refers to `workspace.drop()` — the instruction will be updated to
-- use `workspace.drop_entry()` for consistency.
--
-- Logging:
--   Every call to save / catalog / drop writes a row to workspace.activity_log
--   with timestamp, call type, and payload. This is the data we analyze
--   post-session to compute M1 metrics.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Schema and initialization helper
-- -----------------------------------------------------------------------------
-- The schema itself is persistent (it holds the function definitions).
-- The temp tables inside it are recreated per session via workspace.init().

CREATE SCHEMA IF NOT EXISTS workspace;


-- Called lazily on first use; idempotent within a session.
CREATE OR REPLACE FUNCTION workspace.init() RETURNS void
LANGUAGE plpgsql AS $func$
BEGIN
  -- Registry: one row per saved intermediate in this session.
  IF to_regclass('pg_temp.workspace_registry') IS NULL THEN
    CREATE TEMP TABLE workspace_registry (
      name           text PRIMARY KEY,
      description    text,
      source_sql     text,
      temp_table     text NOT NULL,  -- the actual pg_temp table name
      columns        text[],
      row_count      bigint,
      bytes          bigint,
      created_at     timestamptz DEFAULT now(),
      last_accessed  timestamptz DEFAULT now(),
      access_count   int DEFAULT 0
    ) ON COMMIT PRESERVE ROWS;
  END IF;

  -- Activity log: one row per call to any workspace function.
  IF to_regclass('pg_temp.workspace_activity_log') IS NULL THEN
    CREATE TEMP TABLE workspace_activity_log (
      ts          timestamptz DEFAULT clock_timestamp(),
      call_type   text,            -- 'save' | 'catalog' | 'drop' | 'reference'
      entry_name  text,
      payload     jsonb
    ) ON COMMIT PRESERVE ROWS;
  END IF;
END;
$func$;


-- -----------------------------------------------------------------------------
-- 2. workspace.save(name, description, source_sql)
-- -----------------------------------------------------------------------------
-- Executes source_sql, materializes the result into a session-temp table,
-- records metadata and a size estimate.
--
-- Returns a short jsonb summary that the agent can read.
--
-- Eligibility guardrails (enforced here so the agent cannot save runaway
-- results even if it ignores prompt guidance):
--   - Result row count must be <= 500,000 (hard limit).
--   - Result byte size must be <= 256 MB (hard limit).
--   - Hard limits are larger than the prompt's soft limits on purpose;
--     the soft limits steer the agent, the hard limits protect the system.

CREATE OR REPLACE FUNCTION workspace.save(
  p_name        text,
  p_description text,
  p_source_sql  text
) RETURNS jsonb
LANGUAGE plpgsql AS $func$
DECLARE
  v_temp_table  text;
  v_row_count   bigint;
  v_bytes       bigint;
  v_columns     text[];
  v_result      jsonb;
  v_existing    text;
BEGIN
  PERFORM workspace.init();

  -- Name validation: snake_case identifier, 3-50 chars, no SQL keywords via injection.
  IF p_name IS NULL OR p_name !~ '^[a-z_][a-z0-9_]{2,49}$' THEN
    RAISE EXCEPTION 'workspace.save: name must match ^[a-z_][a-z0-9_]{2,49}$ (got %)', p_name;
  END IF;

  -- Idempotence: if the same name was saved before, drop the old one first.
  SELECT temp_table INTO v_existing FROM workspace_registry WHERE name = p_name;
  IF v_existing IS NOT NULL THEN
    EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', v_existing);
    DELETE FROM workspace_registry WHERE name = p_name;
  END IF;

  -- Derive a unique temp table name (name_<8hex>) so multiple saves with
  -- replacement don't fight each other.
  v_temp_table := format(
    '%s_%s',
    left(p_name, 40),
    substr(md5(p_name || clock_timestamp()::text), 1, 8)
  );

  -- Materialize. CREATE TEMP TABLE ... AS wraps the user's query; ON COMMIT
  -- PRESERVE ROWS keeps it alive across statements within the session.
  EXECUTE format(
    'CREATE TEMP TABLE %I ON COMMIT PRESERVE ROWS AS %s',
    v_temp_table, p_source_sql
  );

  -- Size + shape measurement.
  EXECUTE format('SELECT count(*) FROM pg_temp.%I', v_temp_table)
    INTO v_row_count;

  SELECT pg_total_relation_size(format('pg_temp.%I', v_temp_table)::regclass)
    INTO v_bytes;

  -- Hard limits: drop and error out if the agent stored something too large.
  IF v_row_count > 500000 THEN
    EXECUTE format('DROP TABLE pg_temp.%I', v_temp_table);
    RAISE EXCEPTION 'workspace.save: result has % rows, exceeds 500k hard limit (%)',
      v_row_count, p_name;
  END IF;

  IF v_bytes > 256 * 1024 * 1024 THEN
    EXECUTE format('DROP TABLE pg_temp.%I', v_temp_table);
    RAISE EXCEPTION 'workspace.save: result is % bytes, exceeds 256MB hard limit (%)',
      v_bytes, p_name;
  END IF;

  -- Extract column names from the materialized table.
  SELECT array_agg(attname::text ORDER BY attnum)
    INTO v_columns
  FROM pg_attribute
  WHERE attrelid = format('pg_temp.%I', v_temp_table)::regclass
    AND attnum > 0
    AND NOT attisdropped;

  -- Register.
  INSERT INTO workspace_registry
    (name, description, source_sql, temp_table, columns, row_count, bytes)
  VALUES
    (p_name, p_description, p_source_sql, v_temp_table, v_columns, v_row_count, v_bytes);

  -- Build the response the agent sees.
  v_result := jsonb_build_object(
    'status',       'saved',
    'name',         p_name,
    'row_count',    v_row_count,
    'bytes',        v_bytes,
    'columns',      v_columns,
    'usage_hint',   format('Reference as: SELECT ... FROM %I', v_temp_table)
  );

  -- Log.
  INSERT INTO workspace_activity_log(call_type, entry_name, payload)
  VALUES ('save', p_name, v_result);

  RETURN v_result;
END;
$func$;


-- -----------------------------------------------------------------------------
-- 3. workspace.catalog()
-- -----------------------------------------------------------------------------
-- Returns a table the agent can SELECT from to see what intermediates exist.
-- Also records a catalog access in the activity log so we can measure how
-- often the agent actually consults the catalog.

CREATE OR REPLACE FUNCTION workspace.catalog()
RETURNS TABLE (
  name          text,
  description   text,
  temp_table    text,
  columns       text[],
  row_count     bigint,
  bytes         bigint,
  created_at    timestamptz,
  access_count  int
)
LANGUAGE plpgsql AS $func$
BEGIN
  PERFORM workspace.init();

  INSERT INTO workspace_activity_log(call_type, entry_name, payload)
  VALUES (
    'catalog',
    NULL,
    jsonb_build_object(
      'entry_count',
      (SELECT count(*) FROM workspace_registry)
    )
  );

  RETURN QUERY
    SELECT r.name, r.description, r.temp_table, r.columns,
           r.row_count, r.bytes, r.created_at, r.access_count
    FROM workspace_registry r
    ORDER BY r.created_at;
END;
$func$;


-- -----------------------------------------------------------------------------
-- 4. workspace.drop_entry(name)
-- -----------------------------------------------------------------------------
-- Explicit cleanup. Named `drop_entry` rather than `drop` to avoid keyword
-- collision.

CREATE OR REPLACE FUNCTION workspace.drop_entry(p_name text) RETURNS jsonb
LANGUAGE plpgsql AS $func$
DECLARE
  v_temp_table text;
  v_result     jsonb;
BEGIN
  PERFORM workspace.init();

  SELECT temp_table INTO v_temp_table
  FROM workspace_registry WHERE name = p_name;

  IF v_temp_table IS NULL THEN
    v_result := jsonb_build_object('status', 'not_found', 'name', p_name);
  ELSE
    EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', v_temp_table);
    DELETE FROM workspace_registry WHERE name = p_name;
    v_result := jsonb_build_object('status', 'dropped', 'name', p_name);
  END IF;

  INSERT INTO workspace_activity_log(call_type, entry_name, payload)
  VALUES ('drop', p_name, v_result);

  RETURN v_result;
END;
$func$;


-- -----------------------------------------------------------------------------
-- 5. workspace.record_reference(name)
-- -----------------------------------------------------------------------------
-- Called by the agent (optionally) to flag that a query referenced a saved
-- intermediate. Without this, we rely on post-session regex of the query
-- stream against known temp table names to count references.
--
-- Having an explicit API is cleaner if the agent will actually use it;
-- the prompt will recommend it.

CREATE OR REPLACE FUNCTION workspace.record_reference(p_name text) RETURNS jsonb
LANGUAGE plpgsql AS $func$
DECLARE
  v_result jsonb;
  v_exists boolean;
BEGIN
  PERFORM workspace.init();

  SELECT EXISTS(SELECT 1 FROM workspace_registry WHERE name = p_name)
    INTO v_exists;

  IF v_exists THEN
    UPDATE workspace_registry
      SET access_count  = access_count + 1,
          last_accessed = now()
    WHERE name = p_name;
    v_result := jsonb_build_object('status', 'recorded', 'name', p_name);
  ELSE
    v_result := jsonb_build_object('status', 'not_found', 'name', p_name);
  END IF;

  INSERT INTO workspace_activity_log(call_type, entry_name, payload)
  VALUES ('reference', p_name, v_result);

  RETURN v_result;
END;
$func$;


-- -----------------------------------------------------------------------------
-- 6. workspace.dump_activity()
-- -----------------------------------------------------------------------------
-- Post-session export for analysis. Emits the full activity log + registry
-- snapshot as a single JSON blob the harness can persist to disk.

CREATE OR REPLACE FUNCTION workspace.dump_activity() RETURNS jsonb
LANGUAGE plpgsql AS $func$
DECLARE
  v_log      jsonb;
  v_reg      jsonb;
BEGIN
  PERFORM workspace.init();

  SELECT coalesce(jsonb_agg(to_jsonb(l) ORDER BY l.ts), '[]'::jsonb)
    INTO v_log
  FROM workspace_activity_log l;

  SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY r.created_at), '[]'::jsonb)
    INTO v_reg
  FROM workspace_registry r;

  RETURN jsonb_build_object(
    'registry', v_reg,
    'activity', v_log,
    'dumped_at', now()
  );
END;
$func$;
