#!/usr/bin/env python3
"""
Construct pg_hint_plan Rows() hints from cardinality history.

Given a query's plan tree (from EXPLAIN without ANALYZE) and a history of
previously-executed plan trees (with actual cardinalities), construct a
/*+ Rows(...) */ block that injects measured actual cardinalities for
matching operator signatures.

Limitation: pg_hint_plan Rows() requires ≥2 relations. Single-table scan
cardinalities cannot be hinted via this mechanism.
"""

import time
from typing import Any

from pg_plan_parser import (
    extract_plan_tree,
    is_plan_critical,
    _operator_signature,
    PLAN_CRITICAL_OPS,
)


def _is_rows_hintable(node: dict) -> bool:
    """Check if a node can receive a Rows() hint (requires ≥2 relation aliases)."""
    node_type = node["operator_type"]
    if node_type not in PLAN_CRITICAL_OPS:
        return False
    aliases = node.get("relation_aliases", [])
    return len(aliases) >= 2


def build_signature_history(plan_trees: list[list[dict]]) -> dict[str, dict]:
    """
    Build a signature → (actual_card, relation_aliases) lookup from historical
    plan trees. If a signature appears multiple times, use the most recent.

    Args:
        plan_trees: list of flat node lists from previous queries' EXPLAIN ANALYZE.

    Returns:
        {signature: {"actual_card": int, "relation_aliases": [...], "query_idx": int}}
    """
    history = {}
    for query_idx, nodes in enumerate(plan_trees):
        for node in nodes:
            if not _is_rows_hintable(node):
                continue
            sig = node["operator_signature"]
            actual = node.get("actual_card")
            if actual is None:
                continue
            history[sig] = {
                "actual_card": actual,
                "relation_aliases": node["relation_aliases"],
                "query_idx": query_idx,
            }
    return history


def construct_hints(
    vanilla_plan_tree: list[dict],
    signature_history: dict[str, dict],
) -> tuple[str, list[dict]]:
    """
    Construct a pg_hint_plan hint block for a query.

    Args:
        vanilla_plan_tree: flat node list from EXPLAIN (no ANALYZE) of the query.
        signature_history: output of build_signature_history from prior queries.

    Returns:
        (hint_block_string, list_of_applied_hints)
        hint_block_string: e.g. "/*+ Rows(f1 f2 #39) */" or "" if no hints apply.
        list_of_applied_hints: [{signature, relation_aliases, actual_card, source_query_idx}]
    """
    hints = []
    applied = []

    for node in vanilla_plan_tree:
        if not _is_rows_hintable(node):
            continue
        sig = node["operator_signature"]
        if sig not in signature_history:
            continue

        match = signature_history[sig]
        alias_str = " ".join(sorted(node["relation_aliases"]))
        actual_card = match["actual_card"]
        hint_str = f"Rows({alias_str} #{actual_card})"
        hints.append(hint_str)
        applied.append({
            "signature": sig,
            "relation_aliases": node["relation_aliases"],
            "actual_card": actual_card,
            "source_query_idx": match["query_idx"],
            "hint_string": hint_str,
        })

    if not hints:
        return "", applied

    hint_block = "/*+ " + " ".join(hints) + " */"
    return hint_block, applied


def construct_hints_for_query(
    sql: str,
    conn,
    plan_history: list[list[dict]],
) -> tuple[str, str, list[dict], float]:
    """
    Full pipeline: EXPLAIN the SQL, match against history, construct hints.

    Args:
        sql: the raw SQL query.
        conn: psycopg connection (to run EXPLAIN).
        plan_history: list of plan trees from prior queries in this session.

    Returns:
        (hinted_sql, hint_block, applied_hints, overhead_ms)
    """
    t0 = time.time()

    sig_history = build_signature_history(plan_history)

    if not sig_history:
        # No history with hintable nodes — nothing to match against
        overhead_ms = (time.time() - t0) * 1000
        return sql, "", [], overhead_ms

    # Run EXPLAIN (no ANALYZE) to get vanilla plan structure
    explain_row = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
    vanilla_plan_json = explain_row[0]
    vanilla_tree = extract_plan_tree(vanilla_plan_json)

    hint_block, applied = construct_hints(vanilla_tree, sig_history)

    if hint_block:
        hinted_sql = f"{hint_block}\n{sql}"
    else:
        hinted_sql = sql

    overhead_ms = (time.time() - t0) * 1000
    return hinted_sql, hint_block, applied, overhead_ms
