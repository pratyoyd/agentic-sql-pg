#!/usr/bin/env python3
"""
Parse Postgres EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) output into the same
flat node list format used by the DuckDB logger.

Each node: {node_id, operator_type, tables, predicates, groupby_keys,
            estimated_card, actual_card, operator_signature, children_ids,
            alias, relation_aliases}

Operator mapping to plan-critical / bookkeeping categories:

Plan-critical (cardinality drives join ordering, memory sizing, plan shape):
  Hash Join, Merge Join, Nested Loop        — join methods
  HashAggregate, GroupAggregate, Aggregate   — aggregation
  Seq Scan, Index Scan, Index Only Scan,
    Bitmap Heap Scan, Bitmap Index Scan      — scans
  Filter (appears as a sub-field, not a node)
  WindowAgg                                  — window

Bookkeeping (structural, passthrough cardinalities):
  Sort, Materialize, Gather, Gather Merge,
  Result, Append, Merge Append, Subquery Scan,
  Limit, Unique, SetOp, CTE Scan, Memoize, IncrementalSort
"""

import hashlib
import re
from typing import Any


# --- Operator classification ---

PLAN_CRITICAL_OPS = frozenset({
    "Hash Join", "Merge Join", "Nested Loop",
    "HashAggregate", "GroupAggregate", "Aggregate", "MixedAggregate",
    "Seq Scan", "Index Scan", "Index Only Scan",
    "Bitmap Heap Scan", "Bitmap Index Scan",
    "WindowAgg",
})

BOOKKEEPING_OPS = frozenset({
    "Sort", "Incremental Sort", "Materialize", "Memoize",
    "Gather", "Gather Merge",
    "Result", "Append", "Merge Append",
    "Subquery Scan", "CTE Scan",
    "Limit", "Unique", "SetOp",
    "Hash",  # hash build side — passthrough
})


def _templatize_predicate(pred_str: str) -> str:
    """Normalize a predicate string by replacing literals with placeholders."""
    result = re.sub(r"'[^']*'", "?s", pred_str)
    result = re.sub(r'\b\d+\.?\d*\b', '?n', result)
    return result


def _operator_signature(operator_type: str, tables: list[str],
                        predicates: list[str], groupby_keys: list[str]) -> str:
    """SHA1 hash over canonical operator representation (matches DuckDB logger)."""
    normalized_preds = sorted(_templatize_predicate(p) for p in predicates)
    canonical = f"{operator_type}|{sorted(tables)}|{normalized_preds}|{sorted(groupby_keys)}"
    return hashlib.sha1(canonical.encode()).hexdigest()


def _collect_relation_aliases(node: dict) -> set[str]:
    """Recursively collect all base-relation aliases from a plan subtree."""
    aliases = set()
    alias = node.get("Alias")
    node_type = node.get("Node Type", "")
    # Only count base relation scans (not subquery scans, CTE scans, etc.)
    if node_type in ("Seq Scan", "Index Scan", "Index Only Scan",
                     "Bitmap Heap Scan", "Bitmap Index Scan"):
        if alias:
            aliases.add(alias)
        elif node.get("Relation Name"):
            aliases.add(node["Relation Name"])
    for child in node.get("Plans", []):
        aliases.update(_collect_relation_aliases(child))
    return aliases


def extract_plan_tree(plan_json: list[dict]) -> list[dict]:
    """
    Parse Postgres EXPLAIN (FORMAT JSON) output (the list of plan objects)
    into a flat list of node dicts.

    Args:
        plan_json: The JSON list from EXPLAIN (FORMAT JSON), e.g. [{"Plan": {...}, ...}]

    Returns:
        Flat list of node dicts in depth-first order.
    """
    if not plan_json:
        return []
    root = plan_json[0].get("Plan", {})
    if not root:
        return []
    counter = [0]
    return _extract_node(root, counter)


def _extract_node(pg_node: dict, counter: list[int]) -> list[dict]:
    """Recursively extract a Postgres plan node."""
    node_id = counter[0]
    counter[0] += 1

    node_type = pg_node.get("Node Type", "")

    # --- Tables ---
    tables = []
    rel = pg_node.get("Relation Name")
    if rel:
        tables = [rel]

    # --- Alias ---
    alias = pg_node.get("Alias", "")

    # --- Predicates ---
    predicates = []
    for key in ("Filter", "Hash Cond", "Join Filter", "Merge Cond",
                "Index Cond", "Recheck Cond"):
        val = pg_node.get(key)
        if val:
            predicates.append(str(val))

    # --- GROUP BY keys ---
    groupby_keys = []
    gk = pg_node.get("Group Key")
    if gk:
        groupby_keys = [str(k) for k in gk]

    # --- Cardinalities ---
    estimated_card = pg_node.get("Plan Rows")
    actual_card = pg_node.get("Actual Rows")
    # Actual Rows is per-loop in Postgres; multiply by loops
    actual_loops = pg_node.get("Actual Loops", 1)
    if actual_card is not None and actual_loops:
        actual_card = actual_card * actual_loops

    # --- Relation aliases (for join hint construction) ---
    relation_aliases = sorted(_collect_relation_aliases(pg_node))

    # --- Process children ---
    children = pg_node.get("Plans", [])
    children_ids = []
    child_nodes = []
    for child in children:
        child_tree = _extract_node(child, counter)
        if child_tree:
            children_ids.append(child_tree[0]["node_id"])
            child_nodes.extend(child_tree)

    sig = _operator_signature(node_type, tables, predicates, groupby_keys)

    this_node = {
        "node_id": node_id,
        "operator_type": node_type,
        "tables": tables,
        "predicates": predicates,
        "groupby_keys": groupby_keys,
        "estimated_card": estimated_card,
        "actual_card": actual_card,
        "operator_signature": sig,
        "children_ids": children_ids,
        "alias": alias,
        "relation_aliases": relation_aliases,
    }

    return [this_node] + child_nodes


def is_plan_critical(node_type: str) -> bool:
    return node_type in PLAN_CRITICAL_OPS


def is_hintable(node: dict) -> tuple[bool, str | None]:
    """
    Determine if a plan node can receive a pg_hint_plan Rows() hint.

    Returns:
        (hintable, hint_string_or_None)

    pg_hint_plan Rows() accepts:
      - Single table: Rows(alias #n)  — for scan nodes
      - Join of relations: Rows(alias1 alias2 ... #n)  — for join nodes

    It does NOT accept hints for:
      - Aggregation nodes (HashAggregate etc.) — grouping cardinality is estimator's own
      - Filter nodes above a scan that don't push into the scan
      - Sort, Materialize, etc. (bookkeeping)
    """
    node_type = node["operator_type"]
    actual = node.get("actual_card")
    if actual is None:
        return False, None

    # Scan nodes — hintable as Rows(alias #n)
    if node_type in ("Seq Scan", "Index Scan", "Index Only Scan",
                     "Bitmap Heap Scan"):
        alias = node.get("alias") or (node["tables"][0] if node["tables"] else None)
        if alias:
            return True, f"Rows({alias} #{actual})"
        return False, None

    # Join nodes — hintable as Rows(alias1 alias2 ... #n)
    if node_type in ("Hash Join", "Merge Join", "Nested Loop"):
        aliases = node.get("relation_aliases", [])
        if len(aliases) >= 2:
            alias_str = " ".join(sorted(aliases))
            return True, f"Rows({alias_str} #{actual})"
        return False, None

    # Everything else (aggregates, sorts, etc.) — not hintable
    return False, None
