#!/usr/bin/env python3
"""Verify ws_all_pairs handle v2: with indexes on handle."""

import json
import time
import psycopg

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"

rows = [json.loads(l) for l in open("imdb/traces/task2_rep_b.jsonl")]

MATERIALIZE_SQL = """CREATE TEMP TABLE ws_all_pairs AS
SELECT ci_d.person_id AS director_id,
       ci_a.person_id AS actor_id,
       t.id AS movie_id,
       t.title, t.production_year,
       mii.info::numeric AS rating
FROM cast_info ci_d
JOIN cast_info ci_a
  ON ci_a.movie_id = ci_d.movie_id AND ci_a.person_id <> ci_d.person_id
JOIN title t ON t.id = ci_d.movie_id AND t.kind_id = 1
  AND t.production_year BETWEEN 1990 AND 2010
JOIN movie_info_idx mii ON mii.movie_id = t.id
  AND mii.info_type_id = (SELECT id FROM info_type WHERE info = 'rating')
WHERE ci_d.role_id = (SELECT id FROM role_type WHERE role = 'director')
  AND ci_a.role_id IN (SELECT id FROM role_type WHERE role IN ('actor','actress'))"""

INDEX_SQL = [
    "CREATE INDEX ON ws_all_pairs (director_id, actor_id)",
    "CREATE INDEX ON ws_all_pairs (movie_id)",
    "CREATE INDEX ON ws_all_pairs (rating)",
    "ANALYZE ws_all_pairs",
]

# Rewrites that avoid self-joins where possible.
# Q7 original self-joins pair_counts back to all_pairs for COUNT(DISTINCT movie_id).
# Rewrite uses window function to avoid the 1.8M × 1.8M self-join.
REWRITES = {
    0: """WITH collab AS (
  SELECT director_id, actor_id, COUNT(DISTINCT movie_id) AS film_count
  FROM ws_all_pairs
  GROUP BY director_id, actor_id
  HAVING COUNT(DISTINCT movie_id) >= 3
)
SELECT nd.name AS director_name, na.name AS actor_name, c.film_count
FROM collab c
JOIN name nd ON nd.id = c.director_id
JOIN name na ON na.id = c.actor_id
ORDER BY c.film_count DESC LIMIT 30""",

    1: """WITH collab AS (
  SELECT director_id, actor_id, COUNT(DISTINCT movie_id) AS film_count
  FROM ws_all_pairs WHERE director_id <> actor_id
  GROUP BY director_id, actor_id
  HAVING COUNT(DISTINCT movie_id) >= 5
)
SELECT nd.name AS director_name, na.name AS actor_name, c.film_count
FROM collab c
JOIN name nd ON nd.id = c.director_id
JOIN name na ON na.id = c.actor_id
ORDER BY c.film_count DESC LIMIT 40""",

    2: """WITH collab AS (
  SELECT director_id, actor_id, COUNT(DISTINCT movie_id) AS film_count
  FROM ws_all_pairs WHERE director_id <> actor_id AND rating >= 5.0
  GROUP BY director_id, actor_id
  HAVING COUNT(DISTINCT movie_id) >= 3
)
SELECT nd.name AS director_name, na.name AS actor_name, c.film_count
FROM collab c
JOIN name nd ON nd.id = c.director_id
JOIN name na ON na.id = c.actor_id
ORDER BY c.film_count DESC LIMIT 30""",

    4: """WITH top_dirs AS (
  SELECT id, name FROM name WHERE name IN ('To, Johnnie', 'Wong, Jing', 'Miike, Takashi')
),
top_acts AS (
  SELECT id, name FROM name WHERE name IN ('Lam, Suet', 'Yau, Chingmy', 'Endô, Ken''ichi')
),
collab_films AS (
  SELECT td.name AS director_name, ta.name AS actor_name,
         w.movie_id, w.title, w.production_year
  FROM ws_all_pairs w
  JOIN top_dirs td ON td.id = w.director_id
  JOIN top_acts ta ON ta.id = w.actor_id
)
SELECT cf.director_name, cf.actor_name, cf.title, cf.production_year,
       string_agg(DISTINCT mi_genre.info, ', ' ORDER BY mi_genre.info) AS genres,
       MAX(mii.info) AS rating
FROM collab_films cf
LEFT JOIN movie_info mi_genre ON mi_genre.movie_id = cf.movie_id
  AND mi_genre.info_type_id = (SELECT id FROM info_type WHERE info = 'genres')
LEFT JOIN movie_info_idx mii ON mii.movie_id = cf.movie_id
  AND mii.info_type_id = (SELECT id FROM info_type WHERE info = 'rating')
GROUP BY cf.director_name, cf.actor_name, cf.title, cf.production_year, cf.movie_id
ORDER BY cf.director_name, cf.actor_name, cf.production_year""",

    5: """WITH pairs AS (
  SELECT nd.name AS director_name, na.name AS actor_name,
         w.director_id, w.actor_id, w.movie_id
  FROM ws_all_pairs w
  JOIN name nd ON nd.id = w.director_id
  JOIN name na ON na.id = w.actor_id
  WHERE w.director_id <> w.actor_id
    AND nd.name IN ('To, Johnnie', 'Wong, Jing', 'Miike, Takashi', 'Priyadarshan', 'de Oliveira, Manoel')
),
pair_genres AS (
  SELECT p.director_name, p.actor_name, mi.info AS genre, COUNT(*) AS cnt
  FROM pairs p
  JOIN movie_info mi ON mi.movie_id = p.movie_id
    AND mi.info_type_id = (SELECT id FROM info_type WHERE info = 'genres')
  GROUP BY p.director_name, p.actor_name, mi.info
),
pair_totals AS (
  SELECT director_name, actor_name, COUNT(DISTINCT movie_id) AS total_films
  FROM pairs GROUP BY director_name, actor_name
  HAVING COUNT(DISTINCT movie_id) >= 5
)
SELECT pg.director_name, pg.actor_name, pt.total_films,
       pg.genre, pg.cnt,
       ROUND(pg.cnt * 100.0 / pt.total_films, 0) AS pct_of_films
FROM pair_genres pg
JOIN pair_totals pt ON pt.director_name = pg.director_name AND pt.actor_name = pg.actor_name
ORDER BY pt.total_films DESC, pg.cnt DESC""",

    6: """WITH pairs AS (
  SELECT nd.name AS director_name, na.name AS actor_name, w.movie_id
  FROM ws_all_pairs w
  JOIN name nd ON nd.id = w.director_id
  JOIN name na ON na.id = w.actor_id
  WHERE w.director_id <> w.actor_id
    AND nd.name IN ('To, Johnnie', 'Wong, Jing', 'Miike, Takashi', 'de Oliveira, Manoel', 'Priyadarshan')
),
pair_totals AS (
  SELECT director_name, actor_name, COUNT(DISTINCT movie_id) AS total_films
  FROM pairs GROUP BY director_name, actor_name
  HAVING COUNT(DISTINCT movie_id) >= 5
),
pair_companies AS (
  SELECT p.director_name, p.actor_name, cn.name AS company, COUNT(*) AS cnt
  FROM pairs p
  JOIN pair_totals pt ON pt.director_name = p.director_name AND pt.actor_name = p.actor_name
  JOIN movie_companies mc ON mc.movie_id = p.movie_id
  JOIN company_name cn ON cn.id = mc.company_id
  JOIN company_type ct ON ct.id = mc.company_type_id AND ct.kind = 'production companies'
  GROUP BY p.director_name, p.actor_name, cn.name
  HAVING COUNT(*) >= 3
)
SELECT pc.director_name, pc.actor_name, pt.total_films,
       pc.company, pc.cnt,
       ROUND(pc.cnt * 100.0 / pt.total_films, 0) AS pct_of_films
FROM pair_companies pc
JOIN pair_totals pt ON pt.director_name = pc.director_name AND pt.actor_name = pc.actor_name
ORDER BY pt.total_films DESC, pc.cnt DESC LIMIT 40""",

    # Q7: rewrite using window function to avoid self-join
    7: """WITH top_directors AS (
  SELECT id FROM name WHERE name IN ('To, Johnnie', 'Wong, Jing', 'Miike, Takashi', 'de Oliveira, Manoel')
),
tagged AS (
  SELECT director_id, actor_id, movie_id,
         COUNT(DISTINCT movie_id) OVER (PARTITION BY director_id, actor_id) AS film_count
  FROM ws_all_pairs
  WHERE rating >= 6.0
    AND director_id IN (SELECT id FROM top_directors)
)
SELECT
  CASE WHEN film_count >= 3 THEN 'repeat (3+)' ELSE 'one-off (1)' END AS collab_type,
  COUNT(DISTINCT (director_id, actor_id)) AS pair_count,
  ROUND(AVG(DISTINCT film_count), 2) AS avg_films_together,
  COUNT(DISTINCT movie_id) AS total_films
FROM tagged
WHERE film_count = 1 OR film_count >= 3
GROUP BY collab_type
ORDER BY collab_type""",

    8: """WITH top_dir_ids AS (
  SELECT id FROM name WHERE name IN ('To, Johnnie', 'Wong, Jing', 'Miike, Takashi', 'de Oliveira, Manoel')
),
base AS (
  SELECT director_id, actor_id, movie_id
  FROM ws_all_pairs
  WHERE rating >= 6.0 AND director_id IN (SELECT id FROM top_dir_ids)
    AND director_id <> actor_id
),
tagged AS (
  SELECT *, COUNT(DISTINCT movie_id) OVER (PARTITION BY director_id, actor_id) AS film_count
  FROM base
)
SELECT
  CASE WHEN film_count >= 3 THEN 'repeat (3+)' ELSE 'one-off (1)' END AS collab_type,
  COUNT(DISTINCT (director_id, actor_id)) AS pair_count,
  ROUND(AVG(DISTINCT film_count), 2) AS avg_films,
  COUNT(DISTINCT movie_id) AS distinct_films
FROM tagged
WHERE film_count = 1 OR film_count >= 3
GROUP BY collab_type
ORDER BY collab_type""",

    9: """WITH top_dir_ids AS (
  SELECT id FROM name WHERE name IN ('To, Johnnie', 'Wong, Jing', 'Miike, Takashi', 'de Oliveira, Manoel')
),
base AS (
  SELECT director_id, actor_id, movie_id
  FROM ws_all_pairs
  WHERE rating >= 6.0 AND director_id IN (SELECT id FROM top_dir_ids)
    AND director_id <> actor_id
),
pair_counts AS (
  SELECT director_id, actor_id, COUNT(DISTINCT movie_id) AS film_count
  FROM base GROUP BY director_id, actor_id
),
film_class AS (
  SELECT b.movie_id,
         CASE WHEN pc.film_count >= 3 THEN 'repeat' ELSE 'one-off' END AS collab_type
  FROM base b
  JOIN pair_counts pc ON pc.director_id = b.director_id AND pc.actor_id = b.actor_id
  WHERE pc.film_count = 1 OR pc.film_count >= 3
),
film_totals AS (
  SELECT collab_type, COUNT(DISTINCT movie_id) AS total FROM film_class GROUP BY collab_type
)
SELECT fc.collab_type, k.keyword, COUNT(DISTINCT fc.movie_id) AS film_cnt,
       ft.total AS total_films_of_type,
       ROUND(COUNT(DISTINCT fc.movie_id) * 100.0 / ft.total, 1) AS pct
FROM film_class fc
JOIN movie_keyword mk ON mk.movie_id = fc.movie_id
JOIN keyword k ON k.id = mk.keyword_id
JOIN film_totals ft ON ft.collab_type = fc.collab_type
GROUP BY fc.collab_type, k.keyword, ft.total
HAVING COUNT(DISTINCT fc.movie_id) >= 3
ORDER BY fc.collab_type, pct DESC LIMIT 50""",

    10: """WITH top_dir_ids AS (
  SELECT id FROM name WHERE name IN ('To, Johnnie', 'Wong, Jing', 'Miike, Takashi', 'de Oliveira, Manoel')
),
base AS (
  SELECT director_id, actor_id, movie_id
  FROM ws_all_pairs
  WHERE rating >= 6.0 AND director_id IN (SELECT id FROM top_dir_ids)
    AND director_id <> actor_id
),
pair_counts AS (
  SELECT director_id, actor_id, COUNT(DISTINCT movie_id) AS film_count
  FROM base GROUP BY director_id, actor_id
),
film_class AS (
  SELECT b.movie_id,
         CASE WHEN pc.film_count >= 3 THEN 'repeat' ELSE 'one-off' END AS collab_type
  FROM base b
  JOIN pair_counts pc ON pc.director_id = b.director_id AND pc.actor_id = b.actor_id
  WHERE pc.film_count = 1 OR pc.film_count >= 3
),
film_totals AS (
  SELECT collab_type, COUNT(DISTINCT movie_id) AS total FROM film_class GROUP BY collab_type
)
SELECT fc.collab_type, k.keyword, COUNT(DISTINCT fc.movie_id) AS film_cnt,
       ft.total AS total_films_of_type,
       ROUND(COUNT(DISTINCT fc.movie_id) * 100.0 / ft.total, 1) AS pct
FROM film_class fc
JOIN movie_keyword mk ON mk.movie_id = fc.movie_id
JOIN keyword k ON k.id = mk.keyword_id
JOIN film_totals ft ON ft.collab_type = fc.collab_type
GROUP BY fc.collab_type, k.keyword, ft.total
HAVING COUNT(DISTINCT fc.movie_id) >= 5
ORDER BY fc.collab_type DESC, pct DESC LIMIT 60""",

    11: """WITH all_pairs AS (
  SELECT director_id, actor_id, movie_id
  FROM ws_all_pairs WHERE rating >= 6.0
),
pair_films AS (
  SELECT director_id, actor_id, COUNT(DISTINCT movie_id) AS film_count
  FROM all_pairs GROUP BY director_id, actor_id
),
pair_genre_films AS (
  SELECT pf.director_id, pf.actor_id, pf.film_count,
         mi.info AS genre, COUNT(DISTINCT ap.movie_id) AS genre_film_cnt
  FROM pair_films pf
  JOIN all_pairs ap ON ap.director_id = pf.director_id AND ap.actor_id = pf.actor_id
  JOIN movie_info mi ON mi.movie_id = ap.movie_id
    AND mi.info_type_id = (SELECT id FROM info_type WHERE info = 'genres')
  WHERE pf.film_count = 1 OR pf.film_count >= 3
  GROUP BY pf.director_id, pf.actor_id, pf.film_count, mi.info
),
pair_top_genre AS (
  SELECT director_id, actor_id, film_count,
         MAX(genre_film_cnt) AS top_genre_cnt,
         ROUND(MAX(genre_film_cnt) * 100.0 / film_count, 1) AS top_genre_pct
  FROM pair_genre_films GROUP BY director_id, actor_id, film_count
)
SELECT
  CASE WHEN film_count >= 3 THEN 'repeat (3+)' ELSE 'one-off (1)' END AS collab_type,
  COUNT(*) AS pair_count, ROUND(AVG(film_count), 2) AS avg_films,
  ROUND(AVG(top_genre_pct), 1) AS avg_top_genre_pct,
  ROUND(MIN(top_genre_pct), 1) AS min_top_genre_pct,
  ROUND(MAX(top_genre_pct), 1) AS max_top_genre_pct
FROM pair_top_genre GROUP BY collab_type ORDER BY collab_type""",

    12: """WITH all_pairs AS (
  SELECT director_id, actor_id, movie_id
  FROM ws_all_pairs WHERE rating >= 6.0
),
pair_films AS (
  SELECT director_id, actor_id, COUNT(DISTINCT movie_id) AS film_count
  FROM all_pairs GROUP BY director_id, actor_id
),
pair_genre_films AS (
  SELECT pf.director_id, pf.actor_id, pf.film_count,
         mi.info AS genre, COUNT(DISTINCT ap.movie_id) AS genre_film_cnt
  FROM pair_films pf
  JOIN all_pairs ap ON ap.director_id = pf.director_id AND ap.actor_id = pf.actor_id
  JOIN movie_info mi ON mi.movie_id = ap.movie_id
    AND mi.info_type_id = (SELECT id FROM info_type WHERE info = 'genres')
  WHERE pf.film_count BETWEEN 2 AND 2 OR pf.film_count >= 3
  GROUP BY pf.director_id, pf.actor_id, pf.film_count, mi.info
),
pair_top_genre AS (
  SELECT director_id, actor_id, film_count,
         MAX(genre_film_cnt) AS top_genre_cnt,
         ROUND(MAX(genre_film_cnt) * 100.0 / film_count, 1) AS top_genre_pct
  FROM pair_genre_films GROUP BY director_id, actor_id, film_count
),
pair_company AS (
  SELECT pf.director_id, pf.actor_id, pf.film_count,
         MAX(sub.same_company_cnt) AS max_company_films,
         ROUND(MAX(sub.same_company_cnt) * 100.0 / pf.film_count, 1) AS top_company_pct
  FROM pair_films pf
  JOIN (
    SELECT ap.director_id, ap.actor_id, mc.company_id, COUNT(DISTINCT ap.movie_id) AS same_company_cnt
    FROM all_pairs ap
    JOIN movie_companies mc ON mc.movie_id = ap.movie_id
    GROUP BY ap.director_id, ap.actor_id, mc.company_id
  ) sub ON sub.director_id = pf.director_id AND sub.actor_id = pf.actor_id
  WHERE pf.film_count = 2 OR pf.film_count >= 3
  GROUP BY pf.director_id, pf.actor_id, pf.film_count
)
SELECT
  CASE WHEN ptg.film_count = 2 THEN 'two-film (2)' ELSE 'repeat (3+)' END AS collab_type,
  COUNT(*) AS pair_count, ROUND(AVG(ptg.film_count), 2) AS avg_films,
  ROUND(AVG(ptg.top_genre_pct), 1) AS avg_top_genre_pct,
  ROUND(AVG(pc.top_company_pct), 1) AS avg_top_company_pct
FROM pair_top_genre ptg
JOIN pair_company pc ON pc.director_id = ptg.director_id AND pc.actor_id = ptg.actor_id
GROUP BY collab_type ORDER BY collab_type""",
}


def time_query(conn, sql):
    t0 = time.time()
    try:
        cur = conn.execute(sql)
        rr = cur.fetchall()
        return (time.time() - t0) * 1000, len(rr)
    except Exception as e:
        return (time.time() - t0) * 1000, f"ERR: {str(e)[:80]}"


def main():
    # --- Phase 1: Baseline ---
    print("=" * 75)
    print("PHASE 1: Baseline (each query from scratch)")
    print("=" * 75)
    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("SET statement_timeout = '600s'")

    baseline = {}
    for r in rows:
        seq = r["query_seq"]
        if not r["success"]:
            print(f"  Q{seq}: skipped (failed)")
            continue
        ms, nrows = time_query(conn, r["raw_sql"])
        baseline[seq] = ms
        print(f"  Q{seq}: {ms:10.0f} ms  ({nrows} rows)")

    baseline_total = sum(baseline.values())
    print(f"\n  Baseline total: {baseline_total/1000:.1f}s")
    conn.close()

    # --- Phase 2: Materialize + index ---
    print(f"\n{'=' * 75}")
    print("PHASE 2: Materialize ws_all_pairs + create indexes")
    print("=" * 75)
    conn = psycopg.connect(CONNINFO, autocommit=True)
    conn.execute("SET statement_timeout = '600s'")

    t0 = time.time()
    conn.execute(MATERIALIZE_SQL)
    mat_ms = (time.time() - t0) * 1000

    t0 = time.time()
    for sql in INDEX_SQL:
        conn.execute(sql)
    idx_ms = (time.time() - t0) * 1000

    cnt = conn.execute("SELECT COUNT(*) FROM ws_all_pairs").fetchone()[0]
    setup_ms = mat_ms + idx_ms
    print(f"  Materialized: {cnt:,} rows in {mat_ms:.0f} ms")
    print(f"  Indexes + ANALYZE: {idx_ms:.0f} ms")
    print(f"  Total setup: {setup_ms:.0f} ms")

    # --- Phase 3: Rewritten queries ---
    print(f"\n{'=' * 75}")
    print("PHASE 3: Rewritten queries (using ws_all_pairs + indexes)")
    print("=" * 75)

    rewritten = {}
    for r in rows:
        seq = r["query_seq"]
        if not r["success"]:
            continue
        if seq not in REWRITES:
            print(f"  Q{seq}: no rewrite")
            continue
        ms, nrows = time_query(conn, REWRITES[seq])
        rewritten[seq] = ms
        print(f"  Q{seq}: {ms:10.0f} ms  ({nrows} rows)")

    conn.close()

    # --- Summary ---
    print(f"\n{'=' * 75}")
    print(f"{'Q':<5} {'Baseline':>12} {'Rewritten':>12} {'Speedup':>10} {'Note'}")
    print("-" * 75)
    print(f"{'MAT':<5} {'—':>12} {setup_ms:>10.0f}ms {'(setup)':>10}")

    total_b = 0
    total_r = setup_ms
    for r in rows:
        seq = r["query_seq"]
        if not r["success"]:
            continue
        b = baseline.get(seq, 0)
        rw = rewritten.get(seq)
        total_b += b
        if rw is not None:
            total_r += rw
            speedup = b / rw if rw > 0 else float("inf")
            note = ""
            if seq in (0, 1):
                note = "≈ (no rating filter in orig)"
            elif speedup < 1:
                note = "REGRESSION"
            print(f"Q{seq:<4} {b:>10.0f}ms {rw:>10.0f}ms {speedup:>9.1f}x {note}")
        else:
            total_r += b
            print(f"Q{seq:<4} {b:>10.0f}ms {'(as-is)':>12} {'—':>10}")

    print("-" * 75)
    speedup = total_b / total_r if total_r > 0 else 0
    pct = (1 - total_r / total_b) * 100
    print(f"{'TOTAL':<5} {total_b:>10.0f}ms {total_r:>10.0f}ms {speedup:>9.1f}x")
    print(f"\nSession: {total_b/1000:.1f}s → {total_r/1000:.1f}s  ({pct:+.1f}%)")
    print(f"  Setup (materialization + indexes): {setup_ms/1000:.1f}s")
    print(f"  Rewritten queries only: {(total_r - setup_ms)/1000:.1f}s")


if __name__ == "__main__":
    main()
