-- workspace.sql — session-scoped materialized intermediate results
-- Safe to \i multiple times in the same session (idempotent).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS workspace;

-- Internal: lazily create the per-session catalog in pg_temp.
CREATE OR REPLACE FUNCTION workspace._ensure_catalog()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    -- If the table already exists in pg_temp, this is a no-op.
    CREATE TEMP TABLE IF NOT EXISTS _ws_catalog (
        handle_name   text PRIMARY KEY,
        description   text,
        columns       jsonb,
        row_count     bigint,
        byte_size     bigint,
        creation_ms   numeric,
        created_at    timestamptz DEFAULT now(),
        last_accessed_at timestamptz DEFAULT now(),
        access_count  integer DEFAULT 0,
        query_sha256  text
    ) ON COMMIT PRESERVE ROWS;
END;
$$;

-- save(): materialize a query into a temp table, return JSON summary.
CREATE OR REPLACE FUNCTION workspace.save(
    p_handle_name text,
    p_description text,
    p_query       text
) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_sha256   text;
    v_existing text;
    v_t0       timestamptz;
    v_ms       numeric;
    v_cols     jsonb;
    v_count    bigint;
    v_size     bigint;
BEGIN
    -- Validate handle name.
    IF p_handle_name !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
        RAISE EXCEPTION 'invalid handle name: "%" — must match ^[a-z_][a-z0-9_]{0,62}$', p_handle_name;
    END IF;

    PERFORM workspace._ensure_catalog();

    -- Compute SHA-256 of the query text (trimmed).
    v_sha256 := encode(digest(btrim(p_query), 'sha256'), 'hex');

    -- Dedup: if a handle with the same query hash already exists, return it.
    SELECT handle_name INTO v_existing
      FROM _ws_catalog
     WHERE query_sha256 = v_sha256
     LIMIT 1;

    IF v_existing IS NOT NULL THEN
        RETURN jsonb_build_object(
            'status',      'existing',
            'handle_name', v_existing,
            'query_sha256', v_sha256
        );
    END IF;

    -- Drop any pre-existing handle with the same name (re-save semantics).
    PERFORM workspace.drop(p_handle_name);

    -- Materialize.
    v_t0 := clock_timestamp();
    EXECUTE format('CREATE TEMP TABLE %I ON COMMIT PRESERVE ROWS AS %s', p_handle_name, p_query);
    v_ms := round(extract(epoch FROM clock_timestamp() - v_t0) * 1000, 2);

    -- Row count.
    EXECUTE format('SELECT count(*) FROM %I', p_handle_name) INTO v_count;

    -- Column metadata.
    SELECT jsonb_agg(jsonb_build_object('name', attname, 'type', format_type(atttypid, atttypmod))
                     ORDER BY attnum)
      INTO v_cols
      FROM pg_attribute
     WHERE attrelid = to_regclass('pg_temp.' || quote_ident(p_handle_name))
       AND attnum > 0 AND NOT attisdropped;

    -- Byte size.
    SELECT pg_relation_size(to_regclass('pg_temp.' || quote_ident(p_handle_name)))
      INTO v_size;

    -- Record in catalog.
    INSERT INTO _ws_catalog (handle_name, description, columns, row_count,
                             byte_size, creation_ms, query_sha256)
    VALUES (p_handle_name, p_description, v_cols, v_count,
            v_size, v_ms, v_sha256);

    RETURN jsonb_build_object(
        'status',      'created',
        'handle_name', p_handle_name,
        'row_count',   v_count,
        'creation_ms', v_ms,
        'byte_size',   v_size,
        'columns',     v_cols,
        'query_sha256', v_sha256
    );
END;
$$;

-- catalog(): list all live handles.
CREATE OR REPLACE FUNCTION workspace.catalog()
RETURNS TABLE(
    handle_name   text,
    description   text,
    columns       jsonb,
    row_count     bigint,
    byte_size     bigint,
    creation_ms   numeric,
    age_seconds   numeric,
    access_count  integer
)
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM workspace._ensure_catalog();
    RETURN QUERY
        SELECT c.handle_name, c.description, c.columns, c.row_count,
               c.byte_size, c.creation_ms,
               round(extract(epoch FROM now() - c.created_at)::numeric, 1),
               c.access_count
          FROM _ws_catalog c
         ORDER BY c.creation_ms DESC;
END;
$$;

-- touch(): bump access metadata.
CREATE OR REPLACE FUNCTION workspace.touch(p_handle_name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM workspace._ensure_catalog();
    UPDATE _ws_catalog
       SET last_accessed_at = now(),
           access_count = access_count + 1
     WHERE handle_name = p_handle_name;
    -- Silent no-op if handle doesn't exist.
END;
$$;

-- drop(): remove a handle and its catalog entry.
CREATE OR REPLACE FUNCTION workspace.drop(p_handle_name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    v_exists boolean;
BEGIN
    PERFORM workspace._ensure_catalog();

    DELETE FROM _ws_catalog WHERE handle_name = p_handle_name;
    v_exists := FOUND;

    -- Drop the temp table if it exists.
    IF to_regclass('pg_temp.' || quote_ident(p_handle_name)) IS NOT NULL THEN
        EXECUTE format('DROP TABLE pg_temp.%I', p_handle_name);
        v_exists := true;
    END IF;

    RETURN v_exists;
END;
$$;
