#!/usr/bin/env python3
"""
Post-processing for IMDB experiment traces.
Produces per-rep q-error distributions, CTE analysis, and pathological q-error records.
Run after each task's 3 reps complete.
"""

import json
import re
from pathlib import Path


TRACE_DIR = Path("traces")
QERROR_DIR = Path("qerror_distributions")
CTE_PATH = Path("cte_analysis.jsonl")
PATHOLOGICAL_PATH = Path("pathological_qerrors.jsonl")


def get_join_qerrors_depth3(plan_tree):
    """Extract q-errors from join nodes with 3+ relation aliases (depth 3+)."""
    qerrors = []
    for node in plan_tree:
        if "Join" in node.get("operator_type", ""):
            est = node.get("estimated_card", 0)
            act = node.get("actual_card", 0)
            aliases = node.get("relation_aliases", [])
            if est > 0 and act > 0 and len(aliases) >= 3:
                qe = max(est / act, act / est)
                qerrors.append((qe, node))
    return qerrors


def save_qerror_distribution(task_key, rep, entries):
    """Save sorted q-error list to qerror_distributions/<task>_<rep>.txt."""
    QERROR_DIR.mkdir(parents=True, exist_ok=True)
    all_qerrors = []
    for e in entries:
        for qe, _ in get_join_qerrors_depth3(e.get("plan_tree", [])):
            all_qerrors.append(qe)
    all_qerrors.sort()
    out_path = QERROR_DIR / f"{task_key}_{rep}.txt"
    with open(out_path, "w") as f:
        f.write(f"# q-error distribution for {task_key} rep {rep}\n")
        f.write(f"# {len(all_qerrors)} join nodes at depth 3+\n")
        for qe in all_qerrors:
            f.write(f"{qe:.4f}\n")
    print(f"  Saved {len(all_qerrors)} q-errors to {out_path}")


def count_ctes(sql):
    """Count top-level WITH clauses in SQL."""
    # Match WITH ... AS ( patterns, excluding those inside subqueries
    # Simple approach: count occurrences of WITH at statement start or after ),
    cte_pattern = re.findall(r'\bWITH\b', sql, re.IGNORECASE)
    if not cte_pattern:
        return 0
    # More accurate: parse CTE names after WITH and before SELECT
    cte_names = re.findall(r'(?:WITH|,)\s+(\w+)\s+AS\s*\(', sql, re.IGNORECASE)
    return len(cte_names)


def count_table_accesses(sql):
    """Count base table accesses using sqlglot."""
    try:
        import sqlglot
        from sqlglot import exp
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        tables = [t.name for t in parsed.find_all(exp.Table) if t.name]
        return len(tables), sorted(set(tables))
    except Exception:
        return 0, []


def count_outer_tables(sql):
    """Count tables in the outer SELECT only (not inside CTEs)."""
    # Strip CTE definitions to get just the main query
    # Remove everything between WITH...AS (...) blocks
    stripped = sql
    # Simple approach: find the last top-level SELECT
    # CTEs end before the final SELECT that isn't inside AS (...)
    try:
        import sqlglot
        from sqlglot import exp
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        # Get the main select's FROM tables (not CTE definitions)
        main_tables = []
        # Find all CTEs
        cte_names = set()
        for cte in parsed.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias)
        # Count tables in the main body that aren't CTE references
        for t in parsed.find_all(exp.Table):
            if t.name and t.name not in cte_names:
                # Check if this table is inside a CTE definition
                parent = t.parent
                inside_cte = False
                while parent:
                    if isinstance(parent, exp.CTE):
                        inside_cte = True
                        break
                    parent = parent.parent
                if not inside_cte:
                    main_tables.append(t.name)
        return len(main_tables)
    except Exception:
        return 0


def analyze_cte_reuse(entries):
    """Check if CTEs are reused across queries in a session."""
    # Extract CTE definitions from each query
    cte_defs_by_query = []
    for e in entries:
        sql = e.get("raw_sql", "")
        # Extract CTE bodies: name AS (...)
        cte_bodies = {}
        matches = re.finditer(r'(\w+)\s+AS\s*\(', sql, re.IGNORECASE)
        for m in matches:
            name = m.group(1).lower()
            # Find matching closing paren (simple depth tracking)
            start = m.end()
            depth = 1
            pos = start
            while pos < len(sql) and depth > 0:
                if sql[pos] == '(':
                    depth += 1
                elif sql[pos] == ')':
                    depth -= 1
                pos += 1
            body = sql[start:pos - 1].strip() if depth == 0 else ""
            cte_bodies[name] = body
        cte_defs_by_query.append(cte_bodies)
    return cte_defs_by_query


def normalize_cte_body(body):
    """Normalize a CTE body for comparison: lowercase, collapse whitespace, remove literals."""
    s = body.lower()
    s = re.sub(r'\s+', ' ', s)
    # Replace numeric literals with ?
    s = re.sub(r'\b\d+\b', '?', s)
    # Replace string literals with ?
    s = re.sub(r"'[^']*'", '?', s)
    return s.strip()


def process_cte_analysis(task_key, rep, entries):
    """Produce CTE analysis records for each query in a rep."""
    cte_defs_history = analyze_cte_reuse(entries)
    records = []

    for i, e in enumerate(entries):
        sql = e.get("raw_sql", "")
        n_ctes = count_ctes(sql)
        total_tables, table_list = count_table_accesses(sql)
        outer_tables = count_outer_tables(sql)

        # Check for exact or near-identical CTE reuse
        exact_reuse_count = 0
        near_reuse_count = 0
        current_ctes = cte_defs_history[i]

        for cte_name, cte_body in current_ctes.items():
            norm_current = normalize_cte_body(cte_body)
            for j in range(i):
                for prev_name, prev_body in cte_defs_history[j].items():
                    if cte_body == prev_body:
                        exact_reuse_count += 1
                        break
                    elif normalize_cte_body(prev_body) == norm_current:
                        near_reuse_count += 1
                        break

        record = {
            "task": task_key,
            "rep": rep,
            "query_seq": e.get("query_seq", i),
            "num_ctes": n_ctes,
            "total_table_accesses": total_tables,
            "outer_table_accesses": outer_tables,
            "depth_ratio": round(total_tables / max(outer_tables, 1), 2),
            "exact_cte_reuse": exact_reuse_count,
            "near_identical_cte_reuse": near_reuse_count,
        }
        records.append(record)

    with open(CTE_PATH, "a") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    print(f"  CTE analysis: {len(records)} queries, "
          f"avg {sum(r['num_ctes'] for r in records) / max(len(records), 1):.1f} CTEs/query, "
          f"avg depth ratio {sum(r['depth_ratio'] for r in records) / max(len(records), 1):.1f}")


def process_pathological_qerrors(task_key, rep, entries):
    """Save records for any join node with q-error > 100×."""
    count = 0
    for e in entries:
        for qe, node in get_join_qerrors_depth3(e.get("plan_tree", [])):
            if qe >= 100:
                record = {
                    "task": task_key,
                    "rep": rep,
                    "query_seq": e.get("query_seq"),
                    "join_tables": node.get("relation_aliases", []),
                    "join_predicates": node.get("predicates", []),
                    "estimated_card": node.get("estimated_card"),
                    "actual_card": node.get("actual_card"),
                    "q_error": round(qe, 2),
                    "operator_type": node.get("operator_type"),
                    "node_id": node.get("node_id"),
                    "children_ids": node.get("children_ids", []),
                    "raw_sql": e.get("raw_sql", ""),
                }
                with open(PATHOLOGICAL_PATH, "a") as f:
                    f.write(json.dumps(record, default=str) + "\n")
                count += 1
    print(f"  Pathological q-errors (>100×): {count} nodes")


def process_task(task_key):
    """Run all post-processing for a completed task."""
    print(f"\nPost-processing {task_key}...")
    for rep in ["a", "b", "c"]:
        trace_path = TRACE_DIR / f"{task_key}_rep_{rep}.jsonl"
        if not trace_path.exists():
            print(f"  WARNING: {trace_path} not found, skipping")
            continue

        entries = []
        for line in open(trace_path):
            line = line.strip()
            if line:
                entries.append(json.loads(line))
        # Filter to this session only
        sid = f"{task_key}_rep_{rep}"
        entries = [e for e in entries if e.get("session_id") == sid]

        print(f"  Rep {rep}: {len(entries)} queries")
        save_qerror_distribution(task_key, rep, entries)
        process_cte_analysis(task_key, rep, entries)
        process_pathological_qerrors(task_key, rep, entries)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        for task in sys.argv[1:]:
            process_task(task)
    else:
        for task in ["task1", "task2", "task3", "task4", "task5"]:
            process_task(task)
