# agentic-sql-pg

Workload characterization and optimization for agentic SQL sessions on PostgreSQL.

## Overview

This project studies how LLM-driven data analysis agents interact with relational databases, focusing on two mechanisms for reducing redundant computation:

- **M1 (Workspace)**: Session-local materialization of intermediate query results as temp tables, with a structured protocol (SAVE/SAVE_CTE/SKIP) for deciding what to persist
- **M2 (Intent Declaration)**: A signaling mechanism for the agent to declare upcoming query variant clusters before issuing them

## Key Findings

- **M1 adoption**: 71% of sessions (15/21) used workspace saves; SAVE_CTE was the dominant pattern (11/15 saves)
- **M2 adoption**: 43% of sessions (9/21) emitted intent declarations
- **Latency savings**: 1.30x full-session speedup (322.7s saved across 168 queries), conservative due to PG16 CTE auto-materialization
- **Pre-hoc save** (projected): eliminating double-execution would improve to 1.52x

## Structure

```
.
├── workspace.sql              # Workspace schema (save/catalog/dump functions)
├── workspace_schema.sql       # Extended workspace with activity tracking
├── imdb/
│   ├── scenario_common.py     # Shared harness: M1/M2 prompts, agent runner, report generation
│   ├── scenario_agent.py      # Generic agent entry point (--scenario N --rep X)
│   ├── scenario_report.py     # Generic report generator (--scenario N)
│   ├── scenario1_agent.py     # Scenario 1 harness (genre evolution, 3 reps)
│   ├── benchmark_baseline.py  # Baseline measurement: CTE substitution vs temp table
│   ├── gen_benchmark_report.py# Report generator for baseline benchmark
│   ├── run_*.sh               # Orchestration scripts
│   └── reports/               # Generated analysis reports
│       ├── m1_latency_analysis.md       # Full findings report
│       ├── baseline_benchmark.md        # Per-query benchmark results
│       ├── aggregate_scenarios_2_to_10.md
│       └── scenario*_*.md              # Per-scenario reports
├── paper/                     # Paper (Overleaf)
├── pg_plan_parser.py          # EXPLAIN ANALYZE JSON → plan tree extractor
├── pilot_join.py              # Join-order pilot experiments
└── stage3_5_join_pilot/       # Stage 3.5 join pilot data
```

## Scenarios

| # | Scenario | Description |
|---|----------|-------------|
| 1 | Genre Evolution | Rating shifts across genres, 1990s vs 2010s |
| 2 | Director Career | Career trajectory patterns for prolific directors |
| 3 | Company Genre Shifts | Production company genre mix changes over decades |
| 4 | Cast Size x Rating | Relationship between cast size and ratings across eras |
| 5 | International Co-Production | US/non-US co-production trends and ratings |
| 6 | Franchise Durability | Rating trajectories across franchise installments |
| 7 | Writer-Director Separation | Rating impact of writer-director overlap |
| 8 | Actor Career Archetypes | Career billing trajectory patterns |
| 9 | Series-to-Film Spillover | Actor cross-medium mobility and ratings |
| 10 | Budget Correlation | Budget-rating correlation across eras and tiers |

## Setup

Requires PostgreSQL 16 with the IMDB dataset loaded into `agentic_imdb` database, and `claude` CLI available.

```bash
python -m venv .venv
source .venv/bin/activate
pip install psycopg sqlglot

# Load workspace functions
psql -h localhost -p 5434 -d agentic_imdb -f workspace_schema.sql

# Run a single scenario
cd imdb
python scenario_agent.py --scenario 2 --rep a

# Generate report
python scenario_report.py --scenario 2

# Run baseline benchmark
python benchmark_baseline.py
```

## Database

- PostgreSQL 16.13 with pg_hint_plan 1.6.1
- IMDB dataset: 74M rows across 21 tables (cast_info: 36M, movie_info: 15M)
- Connection: `host=localhost port=5434 dbname=agentic_imdb`
