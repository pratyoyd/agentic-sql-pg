#!/usr/bin/env python3
"""
SQL execution logger for Postgres agentic data analysis.

Mirrors the DuckDB logger's JSONL output format:
  - Raw SQL and normalized template (via sqlglot)
  - Tables, columns, predicates, GROUP BY columns
  - Physical plan tree from EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
  - Inter-query gap timing
"""

import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any

import psycopg
import sqlglot
from sqlglot import exp

from pg_plan_parser import extract_plan_tree


# ---------------------------------------------------------------------------
# sqlglot-based SQL metadata extraction (shared with DuckDB logger)
# ---------------------------------------------------------------------------

def _extract_tables(parsed: exp.Expression) -> list[str]:
    return sorted(set(t.name for t in parsed.find_all(exp.Table) if t.name))


def _extract_columns(parsed: exp.Expression) -> list[str]:
    return sorted(set(c.name for c in parsed.find_all(exp.Column) if c.name))


def _extract_predicates(parsed: exp.Expression) -> list[dict]:
    preds = []
    for node in parsed.find_all(exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE,
                                  exp.NEQ, exp.Like, exp.ILike, exp.Is, exp.In):
        col = None
        op = type(node).__name__
        val = None
        for child in node.args.values():
            if isinstance(child, exp.Column):
                col = child.name
            elif isinstance(child, (exp.Literal, exp.Boolean, exp.Null)):
                val = str(child)
        if col:
            preds.append({"column": col, "operator": op, "value": val or ""})
    return preds


def _extract_group_by(parsed: exp.Expression) -> list[str]:
    group = parsed.find(exp.Group)
    if not group:
        return []
    keys = []
    for expr in group.expressions:
        if isinstance(expr, exp.Column):
            keys.append(expr.name)
        else:
            keys.append(str(expr))
    return keys


def _templatize(sql: str) -> str:
    result = re.sub(r"'[^']*'", "?s", sql)
    result = re.sub(r'\b\d+\.?\d*\b', '?n', result)
    return result.strip()


def _parse_metadata(sql: str) -> dict:
    try:
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        return {
            "template": _templatize(sql),
            "tables": _extract_tables(parsed),
            "columns": _extract_columns(parsed),
            "predicates": _extract_predicates(parsed),
            "group_by_cols": _extract_group_by(parsed),
        }
    except Exception as e:
        return {
            "template": sql,
            "tables": [],
            "columns": [],
            "predicates": [],
            "group_by_cols": [],
            "parse_error": str(e),
        }


# ---------------------------------------------------------------------------
# Logger class
# ---------------------------------------------------------------------------

class PgSessionLogger:
    """Logs all SQL queries for a single Postgres agent session to a JSONL file."""

    def __init__(self, session_id: str, log_dir: str | Path = "stage2/traces",
                 raw_plan_dir: str | Path | None = None):
        self.session_id = session_id
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.log_dir / f"{session_id}.jsonl"
        self.query_seq = 0
        self.prev_query_end_ts: float | None = None

        # Optional: save raw plan JSONs to disk
        self.raw_plan_dir = Path(raw_plan_dir) if raw_plan_dir else None
        if self.raw_plan_dir:
            self.raw_plan_dir.mkdir(parents=True, exist_ok=True)

    def execute_and_log(
        self,
        conn: psycopg.Connection,
        sql: str,
        max_display_rows: int = 50,
    ) -> dict[str, Any]:
        """Execute SQL with EXPLAIN ANALYZE, log metadata + plan tree, return result."""
        meta = _parse_metadata(sql)
        query_start_ts = time.time()

        plan_tree = []
        plan_json = None

        t0 = time.time()
        try:
            # First: get the plan via EXPLAIN ANALYZE
            explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
            plan_row = conn.execute(explain_sql).fetchone()
            plan_json = plan_row[0]
            plan_tree = extract_plan_tree(plan_json)

            # Save raw plan JSON
            if self.raw_plan_dir:
                plan_path = self.raw_plan_dir / f"{self.query_seq}.json"
                with open(plan_path, "w") as f:
                    json.dump(plan_json, f, indent=2)

            # EXPLAIN ANALYZE also executes the query, so we can get result info
            # from the plan. But we need actual result rows for the agent.
            # Re-execute the query to get result data.
            result = conn.execute(sql)
            description = result.description or []
            col_names = [d.name for d in description]
            rows = result.fetchall()
            elapsed_ms = (time.time() - t0) * 1000
            result_rows = len(rows)
            display_data = [list(r) for r in rows[:max_display_rows]]

            # Make data JSON-serializable
            for row in display_data:
                for i, v in enumerate(row):
                    if not isinstance(v, (str, int, float, bool, type(None))):
                        row[i] = str(v)

            success = True
            error_msg = None
            out = {
                "success": True,
                "result_rows": result_rows,
                "columns": col_names,
                "data": display_data,
                "error": None,
            }

        except Exception as e:
            elapsed_ms = (time.time() - t0) * 1000
            success = False
            error_msg = str(e)
            result_rows = 0
            col_names = []
            out = {
                "success": False,
                "result_rows": 0,
                "columns": [],
                "data": [],
                "error": error_msg,
            }

        query_end_ts = time.time()

        entry = {
            "session_id": self.session_id,
            "query_seq": self.query_seq,
            "timestamp": time.time(),
            "query_start_ts": query_start_ts,
            "prev_query_end_ts": self.prev_query_end_ts,
            "raw_sql": sql,
            **meta,
            "result_rows": result_rows,
            "success": success,
            "error_msg": error_msg,
            "execution_ms": round(elapsed_ms, 2),
            "plan_tree": plan_tree,
        }

        with open(self.log_path, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")

        self.prev_query_end_ts = query_end_ts
        self.query_seq += 1
        return out


def format_result(result: dict, max_rows: int = 20) -> str:
    """Format a query result dict into a readable string for the agent."""
    if not result["success"]:
        return f"ERROR: {result['error']}"

    lines = []
    cols = result["columns"]
    data = result["data"][:max_rows]

    if not cols:
        return "(query returned no columns)"

    lines.append(" | ".join(str(c) for c in cols))
    lines.append("-" * len(lines[0]))
    for row in data:
        lines.append(" | ".join(str(v) for v in row))

    if result["result_rows"] > max_rows:
        lines.append(f"... ({result['result_rows']} total rows, showing first {max_rows})")
    else:
        lines.append(f"({result['result_rows']} rows)")

    return "\n".join(lines)
