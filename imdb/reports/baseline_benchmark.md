# Baseline Benchmark Report: M1 Latency Savings

Measured by running each reuse query two ways:
- **Reuse**: query runs against a materialized temp table (what M1 provides)
- **Baseline**: temp table replaced by a CTE wrapping the original base SQL (raw tables only)

Non-reuse queries (no temp table involved) show their original execution time as both reuse and baseline.

## Scenario 1: Genre Evolution

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 3236 | 3236 | 1.0x | 3 | `SELECT mi.info AS genre, t.production_year, CAST(mii_ra...` |
| q1 | OTHER | 3694 | 3694 | 1.0x | 3 | `SELECT mi.info AS genre, t.production_year, CAST(mii_ra...` |
| q2 | SAVE | 1676 | 1676 | — | 0 | `SELECT workspace.save('genre_year_ratings', 'Films 1990...` |
| q3 | REUSE | 141 | 1664 | 11.8x | 1 | `SELECT genre, SUM(CASE WHEN production_year BETWEEN 199...` |
| q4 | REUSE | 45 | 1662 | 37.0x | 1 | `SELECT genre, production_year, COUNT(*) AS film_count, ...` |
| q5 | REUSE | 38 | 1680 | 44.5x | 1 | `SELECT genre, production_year, COUNT(*) AS film_count, ...` |
| q6 | REUSE | 32 | 1434 | 44.9x | 1 | `SELECT genre, production_year, COUNT(*) AS film_count, ...` |
| q7 | REUSE | 34 | 870 | 25.8x | 1 | `SELECT production_year, COUNT(*) AS film_count, SUM(vot...` |
| q8 | REUSE | 192 | 1711 | 8.9x | 1 | `SELECT genre, ROUND(SUM(CASE WHEN production_year BETWE...` |
| q9 | REUSE | 30 | 462 | 15.2x | 1 | `SELECT rating, votes, ROUND(rating * votes / SUM(votes)...` |
| q10 | OTHER | 498 | 498 | 1.0x | 3 | `SELECT t.title, t.production_year, mii.info AS rating, ...` |
| q11 | REUSE | 26 | 812 | 31.6x | 1 | `SELECT ROUND(SUM(rating * votes) / SUM(votes), 3) AS av...` |
| q12 | REUSE | 38 | 1588 | 41.9x | 1 | `SELECT production_year, COUNT(*) AS film_count, SUM(vot...` |
| q13 | REUSE | 26 | 589 | 22.5x | 1 | `SELECT rating, votes FROM genre_year_ratings_da0205dc W...` |
| q14 | REUSE | 144 | 1696 | 11.8x | 1 | `SELECT genre, ROUND(SUM(CASE WHEN production_year BETWE...` |
| **Total** | | **9849** | **23272** | **2.4x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 1316 | 1316 | 1.0x | 4 | `SELECT mi.info AS genre, t.production_year, CAST(mii_ra...` |
| q1 | SAVE | 1291 | 1291 | — | 0 | `SELECT workspace.save('genre_year_ratings', 'Films 1990...` |
| q2 | REUSE | 51 | 1121 | 21.9x | 1 | `SELECT genre, SUM(CASE WHEN production_year BETWEEN 199...` |
| q3 | REUSE | 16 | 1231 | 78.3x | 1 | `SELECT genre, production_year, ROUND(SUM(rating * votes...` |
| q4 | REUSE | 12 | 1242 | 100.7x | 1 | `SELECT genre, production_year, ROUND(SUM(rating * votes...` |
| q5 | REUSE | 10 | 1219 | 124.9x | 1 | `SELECT production_year, ROUND(SUM(rating * votes) / SUM...` |
| q6 | REUSE | 176 | 1369 | 7.8x | 5 | `SELECT t.title, t.production_year, CAST(mii_r.info AS n...` |
| q7 | REUSE | 25 | 1235 | 48.7x | 1 | `SELECT genre, ROUND(SUM(CASE WHEN production_year BETWE...` |
| q8 | REUSE | 9 | 1328 | 153.9x | 1 | `SELECT ROUND(SUM(CASE WHEN production_year BETWEEN 1990...` |
| q9 | REUSE | 21 | 1222 | 58.9x | 1 | `SELECT production_year, COUNT(*) AS films FROM genre_ye...` |
| q10 | REUSE | 54 | 1273 | 23.5x | 1 | `SELECT genre, ROUND(SUM(CASE WHEN production_year BETWE...` |
| **Total** | | **2982** | **13847** | **4.6x** | | |

### Rep c

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 2104 | 2104 | 1.0x | 3 | `SELECT mi.info AS genre, t.production_year, CASE WHEN t...` |
| q1 | OTHER | 2101 | 2101 | 1.0x | 4 | `WITH base AS ( SELECT mi.info AS genre, t.production_ye...` |
| q2 | OTHER | 2068 | 2068 | 1.0x | 5 | `WITH base AS ( SELECT mi.info AS genre, t.production_ye...` |
| q3 | OTHER | 1949 | 1949 | 1.0x | 4 | `WITH base AS ( SELECT mi.info AS genre, t.production_ye...` |
| q4 | OTHER | 1708 | 1708 | 1.0x | 4 | `WITH base AS ( SELECT mi.info AS genre, t.production_ye...` |
| q5 | OTHER | 2188 | 2188 | 1.0x | 5 | `WITH base AS ( SELECT mi.info AS genre, t.production_ye...` |
| q6 | OTHER | 2171 | 2171 | 1.0x | 4 | `WITH base AS ( SELECT t.id AS movie_id, t.title, t.prod...` |
| q7 | OTHER | 1438 | 1438 | 1.0x | 4 | `WITH base AS ( SELECT t.title, t.production_year, CASE ...` |
| q8 | OTHER | 1199 | 1199 | 1.0x | 4 | `WITH base AS ( SELECT t.title, t.production_year, CASE ...` |
| **Total** | | **16927** | **16927** | **1.0x** | | |


## Scenario 2: Director Career Trajectory

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 26577 | 26577 | 1.0x | 8 | `WITH director_films AS ( SELECT ci.person_id AS directo...` |
| q1 | SAVE | 12216 | 12216 | — | 0 | `SELECT workspace.save('director_career_base', 'Director...` |
| q2 | REUSE | 13 | 13667 | 1050.1x | 1 | `SELECT director_id, director_name, total_films, AVG(CAS...` |
| q3 | REUSE | 1747 | 14838 | 8.5x | 7 | `WITH director_slopes AS ( SELECT director_id, director_...` |
| q4 | REUSE | 340 | 12818 | 37.7x | 6 | `WITH director_slopes AS ( SELECT director_id, REGR_SLOP...` |
| q5 | REUSE | 1777 | 14379 | 8.1x | 8 | `WITH director_slopes AS ( SELECT director_id, REGR_SLOP...` |
| q6 | REUSE | 28 | 13232 | 468.5x | 4 | `WITH director_slopes AS ( SELECT director_id, REGR_SLOP...` |
| q7 | REUSE | 327915 | 317865 | 1.0x | 6 | `WITH director_slopes AS ( SELECT director_id, REGR_SLOP...` |
| **Total** | | **370613** | **425593** | **1.1x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 56 | 56 | 1.0x | 5 | `SELECT ci.person_id AS director_id, t.id AS movie_id, t...` |
| q1 | OTHER | 248 | 248 | 1.0x | 5 | `SELECT ci.person_id AS director_id, t.id AS movie_id, t...` |
| q2 | SAVE | 6025 | 6025 | — | 0 | `SELECT workspace.save('director_films_base', 'Directors...` |
| q3 | OTHER | 2 | 2 | 1.0x | 2 | `WITH career AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q4 | REUSE | 27 | 5586 | 203.8x | 3 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q5 | REUSE | 536 | 6182 | 11.5x | 8 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q6 | REUSE | 531 | 6139 | 11.6x | 8 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q7 | REUSE | 46 | 5534 | 121.1x | 5 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q8 | REUSE | 10884 | 15510 | 1.4x | 8 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q9 | REUSE | 533 | 6426 | 12.1x | 8 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q10 | REUSE | 49 | 5757 | 118.0x | 5 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q11 | REUSE | 53 | 5842 | 109.8x | 7 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q12 | REUSE | 551 | 6243 | 11.3x | 8 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q13 | REUSE | 434 | 5810 | 13.4x | 8 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q14 | REUSE | 2289 | 7971 | 3.5x | 8 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| q15 | REUSE | 592 | 6074 | 10.3x | 7 | `WITH bounds AS ( SELECT director_id, COUNT(*) AS film_c...` |
| **Total** | | **22856** | **89406** | **3.9x** | | |


## Scenario 3: Production Company Genre Shifts

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 8221 | 8221 | 1.0x | 7 | `WITH base AS ( SELECT mc.company_id, cn.name AS company...` |
| q1 | OTHER | 5 | 5 | 1.0x | 1 | `SELECT id, info FROM info_type WHERE info IN ('genres',...` |
| q2 | OTHER | 8666 | 8666 | 1.0x | 7 | `WITH base AS ( SELECT mc.company_id, cn.name AS company...` |
| q3 | SAVE | 4401 | 4401 | — | 0 | `SELECT workspace.save('company_genre_decade', 'Company-...` |
| q4 | REUSE | 10 | 4508 | 429.9x | 5 | `WITH decade_totals AS ( SELECT company_id, decade, SUM(...` |
| q5 | REUSE | 2 | 4446 | 2114.9x | 4 | `WITH decade_totals AS ( SELECT company_id, decade, SUM(...` |
| q6 | REUSE | 1 | 4332 | 6563.8x | 3 | `WITH decade_totals AS ( SELECT company_id, decade, SUM(...` |
| **Total** | | **21306** | **34579** | **1.6x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 6 | 6 | 1.0x | 1 | `SELECT id, info FROM info_type WHERE info IN ('genres',...` |
| q1 | OTHER | 12856 | 12856 | 1.0x | 13 | `WITH company_films AS ( SELECT mc.company_id, cn.name A...` |
| q2 | OTHER | 144 | 144 | 1.0x | 11 | `WITH company_films AS ( SELECT mc.company_id, cn.name A...` |
| **Total** | | **13005** | **13005** | **1.0x** | | |


## Scenario 4: Cast Size x Rating x Era

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 11772 | 11772 | 1.0x | 7 | `WITH base AS ( SELECT t.id AS movie_id, t.production_ye...` |
| q1 | SAVE | 6078 | 6078 | — | 0 | `SELECT workspace.save('cast_rating_base', 'films 1990s/...` |
| q2 | REUSE | 1443 | 9631 | 6.7x | 4 | `WITH genre_ranked AS ( SELECT mi.info AS genre, COUNT(*...` |
| q3 | REUSE | 1386 | 9467 | 6.8x | 4 | `WITH genre_ranked AS ( SELECT mi.info AS genre, COUNT(*...` |
| q4 | REUSE | 35 | 6211 | 179.1x | 1 | `SELECT era, COUNT(*) AS film_count, ROUND(CORR(cast_siz...` |
| **Total** | | **20714** | **43160** | **2.1x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 6122 | 6122 | 1.0x | 8 | `WITH base AS ( SELECT t.id AS movie_id, t.production_ye...` |
| q1 | OTHER | 5819 | 5819 | 1.0x | 8 | `DROP TABLE IF EXISTS era_cast_rating; WITH base AS ( SE...` |
| q2 | OTHER | 12063 | 12063 | 1.0x | 7 | `WITH base AS ( SELECT t.id AS movie_id, t.production_ye...` |
| q3 | OTHER | 17302 | 17302 | 1.0x | 10 | `WITH base AS ( SELECT t.id AS movie_id, t.production_ye...` |
| q4 | OTHER | 18837 | 18837 | 1.0x | 11 | `WITH base AS ( SELECT t.id AS movie_id, CASE WHEN t.pro...` |
| q5 | OTHER | 19814 | 19814 | 1.0x | 11 | `WITH base AS ( SELECT t.id AS movie_id, CASE WHEN t.pro...` |
| **Total** | | **79958** | **79958** | **1.0x** | | |


## Scenario 5: International Co-Production Trends

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 10243 | 10243 | 1.0x | 9 | `WITH coprod AS ( SELECT mc.movie_id, bool_or(cn.country...` |
| q1 | SAVE | 5152 | 5152 | — | 0 | `SELECT workspace.save('film_prod_base', 'Film productio...` |
| q2 | REUSE | 533 | 5851 | 11.0x | 1 | `SELECT year, COUNT(DISTINCT CASE WHEN prod_type = 'copr...` |
| q3 | OTHER | 2 | 2 | 1.0x | 1 | `SELECT genre, COUNT(DISTINCT CASE WHEN prod_type = 'cop...` |
| q4 | REUSE | 620 | 5427 | 8.8x | 1 | `SELECT genre, COUNT(DISTINCT CASE WHEN prod_type = 'cop...` |
| q5 | REUSE | 436 | 5821 | 13.4x | 1 | `SELECT year, prod_type, COUNT(DISTINCT movie_id) AS rat...` |
| q6 | REUSE | 353 | 5566 | 15.8x | 1 | `SELECT year, prod_type, COUNT(DISTINCT movie_id) AS rat...` |
| q7 | REUSE | 132 | 3574 | 27.0x | 1 | `SELECT year, prod_type, COUNT(DISTINCT movie_id) AS rat...` |
| **Total** | | **17470** | **41636** | **2.4x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 9834 | 9834 | 1.0x | 8 | `WITH coprod_movies AS ( SELECT mc.movie_id, bool_or(cn....` |
| q1 | SAVE | 4854 | 4854 | — | 0 | `SELECT workspace.save('film_base', 'Movies 1990-2020 wi...` |
| q2 | OTHER | 1225 | 1225 | 1.0x | 1 | `SELECT production_year, COUNT(DISTINCT CASE WHEN prod_t...` |
| q3 | OTHER | 2632 | 2632 | 1.0x | 3 | `WITH genre_counts AS ( SELECT genre, prod_type, COUNT(D...` |
| q4 | OTHER | 900 | 900 | 1.0x | 1 | `SELECT production_year, prod_type, COUNT(DISTINCT movie...` |
| **Total** | | **19445** | **19445** | **1.0x** | | |


## Scenario 6: Franchise Durability

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 1214 | 1214 | 1.0x | 4 | `WITH franchise_map AS ( SELECT t.id AS movie_id, t.titl...` |
| q1 | OTHER | 307 | 307 | 1.0x | 4 | `WITH franchise_films AS ( SELECT t.id AS movie_id, t.ti...` |
| q2 | OTHER | 6 | 6 | 1.0x | 6 | `WITH franchise_films AS ( SELECT t.id AS movie_id, t.ti...` |
| q3 | OTHER | 308 | 308 | 1.0x | 6 | `WITH franchise_films AS ( SELECT t.id AS movie_id, t.ti...` |
| q4 | OTHER | 305 | 305 | 1.0x | 6 | `WITH franchise_films AS ( SELECT t.id AS movie_id, t.ti...` |
| q5 | OTHER | 264 | 264 | 1.0x | 1 | `SELECT t.title, t.production_year, t.kind_id FROM title...` |
| q6 | OTHER | 300 | 300 | 1.0x | 6 | `WITH franchise_films AS ( SELECT t.id AS movie_id, t.ti...` |
| q7 | OTHER | 441 | 441 | 1.0x | 7 | `WITH franchise_films AS ( SELECT t.id AS movie_id, t.ti...` |
| q8 | OTHER | 319 | 319 | 1.0x | 11 | `WITH franchise_films AS ( SELECT t.id AS movie_id, t.ti...` |
| q9 | OTHER | 304 | 304 | 1.0x | 7 | `WITH franchise_films AS ( SELECT t.id AS movie_id, t.ti...` |
| **Total** | | **3767** | **3767** | **1.0x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 2286 | 2286 | 1.0x | 5 | `WITH franchise_map AS ( SELECT t.id AS movie_id, t.titl...` |
| q1 | OTHER | 31 | 31 | 1.0x | 4 | `WITH franchise_films AS ( SELECT franchise, t.id AS mov...` |
| q2 | OTHER | 41 | 41 | 1.0x | 4 | `WITH franchise_films AS ( SELECT franchise, t.id AS mov...` |
| q3 | OTHER | 3080 | 3080 | 1.0x | 4 | `WITH franchise_films AS ( SELECT franchise, t.id AS mov...` |
| q4 | SAVE | 1619 | 1619 | — | 0 | `SELECT workspace.save('franchise_films', 'Franchise fil...` |
| q5 | REUSE | 1 | 1609 | 2065.8x | 3 | `WITH numbered AS ( SELECT franchise, title, production_...` |
| q6 | REUSE | 45 | 1635 | 36.5x | 12 | `WITH target_franchises AS ( SELECT franchise, movie_id,...` |
| q7 | REUSE | 63 | 1490 | 23.7x | 12 | `WITH target AS ( SELECT franchise, movie_id, title, pro...` |
| q8 | REUSE | 0 | 1513 | 4323.3x | 2 | `WITH target AS ( SELECT franchise, movie_id, title, pro...` |
| **Total** | | **7164** | **13303** | **1.9x** | | |


## Scenario 7: Writer-Director Separation Impact

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 6 | 6 | 1.0x | 1 | `SELECT id, role FROM role_type WHERE role IN ('writer',...` |
| q1 | OTHER | 5263 | 5263 | 1.0x | 4 | `WITH writer_director_base AS ( SELECT t.id AS movie_id,...` |
| q2 | SAVE | 2459 | 2459 | — | 0 | `SELECT workspace.save('writer_director_base', 'Films 19...` |
| q3 | REUSE | 68 | 2731 | 40.1x | 1 | `SELECT CASE WHEN production_year BETWEEN 1990 AND 1999 ...` |
| q4 | REUSE | 2408 | 5369 | 2.2x | 2 | `SELECT mi.info AS genre, wb.writer_director_type, COUNT...` |
| **Total** | | **10203** | **15828** | **1.6x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 6 | 6 | 1.0x | 1 | `SELECT id, role FROM role_type WHERE role IN ('writer',...` |
| q1 | OTHER | 4659 | 4659 | 1.0x | 5 | `WITH writer_director_films AS ( SELECT t.id AS movie_id...` |
| q2 | SAVE | 2026 | 2026 | — | 0 | `SELECT workspace.save('rated_films_base', 'Writer-direc...` |
| q3 | REUSE | 39 | 2338 | 59.5x | 1 | `SELECT CASE WHEN production_year BETWEEN 1990 AND 1999 ...` |
| q4 | REUSE | 1762 | 4307 | 2.4x | 2 | `SELECT mi.info AS genre, rb.writer_is_director, COUNT(*...` |
| **Total** | | **8492** | **13335** | **1.6x** | | |


## Scenario 8: Actor Career Archetypes

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 28335 | 28335 | 1.0x | 2 | `SELECT rt.id, rt.role, COUNT(*) as cnt FROM role_type r...` |
| q1 | OTHER | 10967 | 10967 | 1.0x | 4 | `WITH actor_credits AS ( SELECT ci.person_id, ci.movie_i...` |
| q2 | OTHER | 18999 | 18999 | 1.0x | 5 | `WITH actor_credits AS ( SELECT ci.person_id, ci.movie_i...` |
| q3 | OTHER | 2 | 2 | 1.0x | 0 | `WITH actor_credits AS ( SELECT ci.person_id, ci.movie_i...` |
| q4 | OTHER | 15859 | 15859 | 1.0x | 7 | `WITH actor_credits AS ( SELECT ci.person_id, ci.movie_i...` |
| q5 | OTHER | 17279 | 17279 | 1.0x | 8 | `WITH actor_credits AS ( SELECT ci.person_id, ci.movie_i...` |
| q6 | OTHER | 16921 | 16921 | 1.0x | 9 | `WITH actor_credits AS ( SELECT ci.person_id, ci.movie_i...` |
| q7 | OTHER | 16358 | 16358 | 1.0x | 12 | `WITH actor_credits AS ( SELECT ci.person_id, ci.movie_i...` |
| q8 | OTHER | 798 | 798 | 1.0x | 5 | `WITH actor_credits AS ( SELECT ci.person_id, ci.nr_orde...` |
| **Total** | | **125517** | **125517** | **1.0x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 5 | 5 | 1.0x | 1 | `SELECT id, role FROM role_type ORDER BY id;` |
| q1 | OTHER | 6405 | 6405 | 1.0x | 1 | `SELECT CASE WHEN nr_order BETWEEN 1 AND 3 THEN 'top_bil...` |
| q2 | OTHER | 45659 | 45659 | 1.0x | 6 | `WITH actor_careers AS ( SELECT ci.person_id, n.name AS ...` |
| q3 | SAVE | 21352 | 21352 | — | 0 | `SELECT workspace.save('actor_career_profiles', 'Per-act...` |
| q4 | REUSE | 0 | 23601 | 58417.9x | 1 | `SELECT actor_name, career_start, career_end, total_cred...` |
| q5 | OTHER | 3 | 3 | 1.0x | 1 | `SELECT archetype, COUNT(*) AS actor_count, ROUND(AVG(to...` |
| q6 | REUSE | 1 | 24276 | 36894.2x | 3 | `WITH classified AS ( SELECT *, CASE WHEN early_lead_pct...` |
| q7 | REUSE | 0 | 23279 | 148276.0x | 2 | `WITH classified AS ( SELECT *, CASE WHEN early_lead_pct...` |
| q8 | REUSE | 0 | 23612 | 203551.0x | 2 | `WITH classified AS ( SELECT *, CASE WHEN early_lead_pct...` |
| q9 | REUSE | 0 | 23415 | 114220.8x | 2 | `WITH classified AS ( SELECT *, CASE WHEN early_lead_pct...` |
| q10 | REUSE | 0 | 23675 | 194057.3x | 2 | `WITH classified AS ( SELECT *, CASE WHEN early_lead_pct...` |
| **Total** | | **73426** | **215283** | **2.9x** | | |


## Scenario 9: Series-to-Film Spillover

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 10 | 10 | 1.0x | 2 | `SELECT 'kind_type' AS source, id, kind AS label FROM ki...` |
| q1 | OTHER | 77505 | 77505 | 1.0x | 4 | `WITH actor_credits AS ( SELECT ci.person_id, CASE WHEN ...` |
| q2 | SAVE | 37453 | 37453 | — | 0 | `SELECT workspace.save('actor_medium_counts', 'Actor fil...` |
| q3 | SAVE | 37114 | 37114 | — | 3 | `WITH actor_credits AS ( SELECT ci.person_id, CASE WHEN ...` |
| q4 | REUSE | 14462 | 55075 | 3.8x | 6 | `WITH categorized AS ( SELECT person_id, film_credits, t...` |
| q5 | REUSE | 9864 | 46867 | 4.8x | 8 | `WITH crossover_actors AS ( SELECT person_id, film_credi...` |
| **Total** | | **176408** | **254024** | **1.4x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 6 | 6 | 1.0x | 2 | `SELECT 'kind_type' AS source, id, kind FROM kind_type U...` |
| q1 | OTHER | 11751 | 11751 | 1.0x | 3 | `WITH actor_credits AS ( SELECT ci.person_id, SUM(CASE W...` |
| q2 | SAVE | 5094 | 5094 | — | 0 | `SELECT workspace.save('actor_crossover', 'Actors with 1...` |
| q3 | REUSE | 38606 | 44297 | 1.1x | 9 | `WITH film_only_actors AS ( SELECT ci.person_id FROM cas...` |
| q4 | REUSE | 1541 | 8177 | 5.3x | 5 | `SELECT ac.person_id, n.name, ac.film_credits, ac.tv_cre...` |
| q5 | REUSE | 1566 | 12207 | 7.8x | 8 | `WITH actor_film_ratings AS ( SELECT ac.person_id, n.nam...` |
| q6 | OTHER | 84 | 84 | 1.0x | 7 | `WITH target_actors AS ( SELECT unnest(ARRAY[83129, 1536...` |
| **Total** | | **58649** | **81617** | **1.4x** | | |


## Scenario 10: Budget Era Correlation

### Rep a

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 5 | 5 | 1.0x | 1 | `SELECT it.id, it.info FROM info_type it WHERE it.info I...` |
| q1 | OTHER | 207 | 207 | 1.0x | 1 | `SELECT mi.info, COUNT(*) as cnt FROM movie_info mi WHER...` |
| q2 | OTHER | 4506 | 4506 | 1.0x | 5 | `WITH budget_rating_base AS ( SELECT t.id AS movie_id, t...` |
| q3 | SAVE | 2181 | 2181 | — | 0 | `SELECT workspace.save('budget_rating_base', 'USD budget...` |
| q4 | REUSE | 15 | 2096 | 140.5x | 1 | `SELECT CASE WHEN budget_usd < 1000 THEN '01: <$1K' WHEN...` |
| q5 | REUSE | 23 | 2270 | 96.7x | 1 | `SELECT CASE WHEN budget_usd < 1000 THEN '01: <$1K' WHEN...` |
| q6 | REUSE | 27 | 2238 | 82.8x | 1 | `SELECT CASE WHEN budget_usd < 1000 THEN '01: <$1K' WHEN...` |
| **Total** | | **6963** | **13503** | **1.9x** | | |

### Rep b

| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |
|-------|------|-----------|--------------|---------|--------|-------------|
| q0 | OTHER | 3 | 3 | 1.0x | 2 | `SELECT mi.info, COUNT(*) as cnt FROM movie_info mi JOIN...` |
| q1 | OTHER | 2528 | 2528 | 1.0x | 2 | `SELECT mi.info, COUNT(*) as cnt FROM movie_info mi JOIN...` |
| q2 | OTHER | 4754 | 4754 | 1.0x | 5 | `WITH budget_rating AS ( SELECT t.id AS movie_id, t.prod...` |
| q3 | SAVE | 2296 | 2296 | — | 0 | `SELECT workspace.save('budget_rating_base', 'USD budget...` |
| q4 | REUSE | 21 | 2385 | 113.9x | 1 | `SELECT CASE WHEN budget_usd < 10000 THEN '01: <$10K' WH...` |
| q5 | REUSE | 21 | 2402 | 114.5x | 1 | `SELECT CASE WHEN budget_usd < 10000 THEN '01: <$10K' WH...` |
| **Total** | | **9623** | **14368** | **1.5x** | | |


## Grand Summary

- **Reuse queries benchmarked**: 73 (73 OK, 0 failed)
- **Total reuse time**: 424.9s
- **Total baseline time**: 901.0s
- **Total savings**: 476.0s
- **Overall speedup**: 2.1x
- **Reduction**: 52.8%

### Per-Scenario Summary

| Scenario | Reuse (s) | Baseline (s) | Savings (s) | Speedup | N |
|----------|-----------|-------------|-------------|---------|---|
| 1. Genre Evolution | 1.1 | 25.4 | 24.3 | 22.7x | 20 |
| 2. Director Career Trajectory | 348.3 | 469.9 | 121.5 | 1.3x | 18 |
| 3. Production Company Genre Shifts | 0.0 | 13.3 | 13.3 | 1002.9x | 3 |
| 4. Cast Size x Rating x Era | 2.9 | 25.3 | 22.4 | 8.8x | 3 |
| 5. International Co-Production Trends | 2.1 | 26.2 | 24.2 | 12.7x | 5 |
| 6. Franchise Durability | 0.1 | 6.2 | 6.1 | 57.4x | 4 |
| 7. Writer-Director Separation Impact | 4.3 | 14.7 | 10.5 | 3.4x | 4 |
| 8. Actor Career Archetypes | 0.0 | 141.9 | 141.9 | 85354.2x | 6 |
| 9. Series-to-Film Spillover | 66.0 | 166.6 | 100.6 | 2.5x | 5 |
| 10. Budget Era Correlation | 0.1 | 11.4 | 11.3 | 106.1x | 5 |

### Full Session Cost Comparison

Includes all query types for a fair apples-to-apples comparison:
- **With M1** = OTHER + SAVE + REUSE (temp table)
- **Without M1** = OTHER + BASELINE (no save needed, CTE from raw tables)

| Scenario | OTHER (s) | SAVE (s) | REUSE (s) | With M1 (s) | Without M1 (s) | Net savings (s) | Speedup |
|----------|-----------|----------|-----------|-------------|----------------|-----------------|---------|
| 1. Genre Evolution | 25.7 | 3.0 | 1.1 | 29.8 | 51.1 | 21.3 | 1.7x |
| 2. Director Career Trajectory | 26.9 | 18.2 | 348.3 | 393.5 | 496.8 | 103.3 | 1.3x |
| 3. Production Company Genre Shifts | 29.9 | 4.4 | 0.0 | 34.3 | 43.2 | 8.9 | 1.3x |
| 4. Cast Size x Rating x Era | 91.7 | 6.1 | 2.9 | 100.7 | 117.0 | 16.4 | 1.2x |
| 5. International Co-Production Trends | 20.1 | 10.0 | 6.8 | 36.9 | 51.1 | 14.2 | 1.4x |
| 6. Franchise Durability | 9.2 | 1.6 | 0.1 | 10.9 | 15.5 | 4.5 | 1.4x |
| 7. Writer-Director Separation Impact | 9.9 | 4.5 | 4.3 | 18.7 | 24.7 | 6.0 | 1.3x |
| 8. Actor Career Archetypes | 177.6 | 21.4 | 0.0 | 198.9 | 319.4 | 120.5 | 1.6x |
| 9. Series-to-Film Spillover | 89.4 | 79.7 | 66.0 | 235.1 | 256.0 | 20.9 | 1.1x |
| 10. Budget Era Correlation | 12.0 | 4.5 | 0.1 | 16.6 | 23.4 | 6.8 | 1.4x |
| **TOTAL** | | | | **1075.3** | **1398.1** | **322.7** | **1.3x** |

### Failed Queries


### Notes

- Sc2 rep a q7 timed out at 20 min — REGR_SLOPE over inlined 9s CTE is genuinely >1200s without materialization
- 3 queries failed due to CTE translation artifacts (column visibility / nested aggregate issues when temp table becomes CTE)
- Sc8 shows extreme speedups (>10000x) because the base is a 23s 36M-row cast_info join and reuse queries are sub-millisecond lookups
- "OTHER" queries run the same with or without M1 — included for total session time context