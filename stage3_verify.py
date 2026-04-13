#!/usr/bin/env python3
"""
Stage 3 verification: optimizer awareness and plan shape differences.

Runs three flag-28 queries against both agentic_poc and agentic_poc_scaled,
comparing estimated row counts and plan topology.
"""

import json
import statistics
import time
from pathlib import Path

import psycopg

CONNINFO_ORIG = "host=localhost port=5434 dbname=agentic_poc"
CONNINFO_SCALED = "host=localhost port=5434 dbname=agentic_poc_scaled"
STAGE3_DIR = Path("stage3")

# Three flag-28 queries spanning different operator types:
# Q0: simple scan + filter + aggregate (GROUP BY department)
# Q10: two-way join (flag_28 LEFT JOIN flag_28_sysuser)
# Q6: CTE with joins and aggregates (three-way with subquery)

QUERIES = {
    "Q0_scan_agg": """SELECT
  department,
  COUNT(*) AS total_goals,
  SUM(CASE WHEN state = 'Completed' THEN 1 ELSE 0 END) AS completed,
  ROUND(100.0 * SUM(CASE WHEN state = 'Completed' THEN 1 ELSE 0 END) / COUNT(*), 1) AS completion_rate_pct,
  ROUND(AVG(percent_complete), 1) AS avg_pct_complete,
  ROUND(AVG(CASE WHEN state = 'Completed' THEN percent_complete END), 1) AS avg_pct_complete_when_done
FROM flag_28
GROUP BY department
ORDER BY completion_rate_pct DESC""",

    "Q10_join_agg": """SELECT
  g.department,
  ROUND(AVG(u.tenure_years)::numeric, 2) AS avg_tenure_years,
  ROUND(AVG(u.decline_rate)::numeric, 4) AS avg_decline_rate,
  COUNT(DISTINCT g.owner) AS distinct_owners,
  COUNT(*) AS total_goals,
  ROUND(100.0 * SUM(CASE WHEN g.state = 'Completed' THEN 1 ELSE 0 END) / COUNT(*), 1) AS completion_rate_pct
FROM flag_28 g
LEFT JOIN flag_28_sysuser u ON u.department = g.department
GROUP BY g.department
ORDER BY completion_rate_pct DESC""",

    "Q6_cte_join": """WITH dept_priority_rates AS (
  SELECT
    department,
    priority,
    COUNT(*) AS goals,
    ROUND(100.0 * SUM(CASE WHEN state = 'Completed' THEN 1 ELSE 0 END) / COUNT(*), 1) AS completion_rate
  FROM flag_28
  GROUP BY department, priority
),
it_priority_mix AS (
  SELECT
    priority,
    COUNT(*) AS it_goals,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS it_pct
  FROM flag_28
  WHERE department = 'IT'
  GROUP BY priority
)
SELECT
  d.department,
  ROUND(SUM(d.completion_rate * i.it_pct / 100.0), 1) AS predicted_rate_with_IT_mix,
  MAX(CASE WHEN d.department = 'IT' THEN
    (SELECT ROUND(100.0 * SUM(CASE WHEN state='Completed' THEN 1 ELSE 0 END)/COUNT(*),1) FROM flag_28 WHERE department='IT')
  END) AS it_actual_rate
FROM dept_priority_rates d
JOIN it_priority_mix i ON d.priority = i.priority
GROUP BY d.department
ORDER BY predicted_rate_with_IT_mix DESC""",
}


def get_explain(conn, sql: str) -> dict:
    """Run EXPLAIN (FORMAT JSON) and return the plan."""
    row = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
    return row[0][0]["Plan"]


def get_explain_analyze(conn, sql: str) -> dict:
    """Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) and return the full JSON."""
    row = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}").fetchone()
    return row[0][0]


def flatten_plan(plan: dict, depth: int = 0) -> list[dict]:
    """Flatten a plan tree into a list of nodes with depth."""
    nodes = [{
        "depth": depth,
        "node_type": plan.get("Node Type", "?"),
        "estimated_rows": plan.get("Plan Rows", 0),
        "actual_rows": plan.get("Actual Rows"),
        "actual_time": plan.get("Actual Total Time"),
        "join_type": plan.get("Join Type"),
        "strategy": plan.get("Strategy"),
    }]
    for child in plan.get("Plans", []):
        nodes.extend(flatten_plan(child, depth + 1))
    return nodes


def format_plan_tree(plan: dict, depth: int = 0) -> str:
    """Format a plan tree as indented text."""
    indent = "  " * depth
    node_type = plan.get("Node Type", "?")
    est_rows = plan.get("Plan Rows", 0)
    actual_rows = plan.get("Actual Rows", "")
    join_type = plan.get("Join Type", "")
    strategy = plan.get("Strategy", "")

    extra = ""
    if join_type:
        extra += f" [{join_type}]"
    if strategy:
        extra += f" [{strategy}]"
    if actual_rows != "":
        line = f"{indent}{node_type}{extra} (est={est_rows}, actual={actual_rows})"
    else:
        line = f"{indent}{node_type}{extra} (est={est_rows})"

    lines = [line]
    for child in plan.get("Plans", []):
        lines.append(format_plan_tree(child, depth + 1))
    return "\n".join(lines)


def optimizer_awareness_check() -> str:
    """Compare EXPLAIN estimates between original and scaled databases."""
    conn_orig = psycopg.connect(CONNINFO_ORIG, autocommit=True)
    conn_scaled = psycopg.connect(CONNINFO_SCALED, autocommit=True)

    # Get scale factor
    orig_rows = conn_orig.execute("SELECT COUNT(*) FROM flag_28").fetchone()[0]
    scaled_rows = conn_scaled.execute("SELECT COUNT(*) FROM flag_28").fetchone()[0]
    scale_factor = scaled_rows / orig_rows

    md = []
    md.append("# Optimizer Awareness Check")
    md.append("")
    md.append(f"**flag_28:** {orig_rows} → {scaled_rows} rows (×{scale_factor:.0f})")
    md.append(f"**flag_28_sysuser:** dimension table, unchanged")
    md.append("")

    for qname, sql in QUERIES.items():
        md.append(f"## {qname}")
        md.append("")

        plan_orig = get_explain(conn_orig, sql)
        plan_scaled = get_explain(conn_scaled, sql)

        nodes_orig = flatten_plan(plan_orig)
        nodes_scaled = flatten_plan(plan_scaled)

        md.append("**Original (550 rows):**")
        md.append("```")
        md.append(format_plan_tree(plan_orig))
        md.append("```")
        md.append("")

        md.append(f"**Scaled ({scaled_rows} rows):**")
        md.append("```")
        md.append(format_plan_tree(plan_scaled))
        md.append("```")
        md.append("")

        # Compare node-by-node estimates
        md.append("**Estimate comparison:**")
        md.append("| Node | Original Est | Scaled Est | Ratio | Expected ×{:.0f} |".format(scale_factor))
        md.append("|------|-------------|-----------|-------|----------|")
        for no, ns in zip(nodes_orig, nodes_scaled):
            ratio = ns["estimated_rows"] / no["estimated_rows"] if no["estimated_rows"] > 0 else float("inf")
            expected = "~" if 0.5 * scale_factor <= ratio <= 2.0 * scale_factor else "OFF"
            md.append(f"| {no['node_type']} | {no['estimated_rows']} | {ns['estimated_rows']} | {ratio:.1f}× | {expected} |")
        md.append("")

    conn_orig.close()
    conn_scaled.close()
    return "\n".join(md)


def plan_shape_diffs() -> str:
    """Run EXPLAIN ANALYZE on both databases, compare plan shapes and latencies."""
    conn_orig = psycopg.connect(CONNINFO_ORIG, autocommit=True)
    conn_scaled = psycopg.connect(CONNINFO_SCALED, autocommit=True)

    orig_rows = conn_orig.execute("SELECT COUNT(*) FROM flag_28").fetchone()[0]
    scaled_rows = conn_scaled.execute("SELECT COUNT(*) FROM flag_28").fetchone()[0]
    scale_factor = scaled_rows / orig_rows

    md = []
    md.append("# Plan Shape Differences at Scale")
    md.append("")
    md.append(f"**Scale factor:** ×{scale_factor:.0f} ({orig_rows} → {scaled_rows} rows)")
    md.append(f"**Method:** 4 runs per query per database, discard first, median of last 3")
    md.append("")

    for qname, sql in QUERIES.items():
        md.append(f"## {qname}")
        md.append("")

        # Warm cache: 4 runs, discard first, median of last 3
        timings_orig = []
        timings_scaled = []
        plan_orig = None
        plan_scaled = None

        for i in range(4):
            ea_orig = get_explain_analyze(conn_orig, sql)
            if i > 0:
                timings_orig.append(ea_orig["Execution Time"])
            if i == 3:
                plan_orig = ea_orig["Plan"]

        for i in range(4):
            ea_scaled = get_explain_analyze(conn_scaled, sql)
            if i > 0:
                timings_scaled.append(ea_scaled["Execution Time"])
            if i == 3:
                plan_scaled = ea_scaled["Plan"]

        med_orig = statistics.median(timings_orig)
        med_scaled = statistics.median(timings_scaled)
        latency_ratio = med_scaled / med_orig if med_orig > 0 else float("inf")

        # Compare plan topology
        nodes_orig = flatten_plan(plan_orig)
        nodes_scaled = flatten_plan(plan_scaled)

        topology_same = True
        op_diffs = []
        for no, ns in zip(nodes_orig, nodes_scaled):
            if no["node_type"] != ns["node_type"]:
                topology_same = False
                op_diffs.append(f"{no['node_type']} → {ns['node_type']}")
            if no.get("join_type") != ns.get("join_type"):
                topology_same = False
                op_diffs.append(f"Join: {no.get('join_type')} → {ns.get('join_type')}")
        if len(nodes_orig) != len(nodes_scaled):
            topology_same = False
            op_diffs.append(f"Node count: {len(nodes_orig)} → {len(nodes_scaled)}")

        md.append(f"**Topology changed:** {'NO' if topology_same else 'YES'}")
        if op_diffs:
            for d in op_diffs:
                md.append(f"  - {d}")
        md.append("")

        md.append("**Original plan:**")
        md.append("```")
        md.append(format_plan_tree(plan_orig))
        md.append("```")
        md.append("")

        md.append("**Scaled plan:**")
        md.append("```")
        md.append(format_plan_tree(plan_scaled))
        md.append("```")
        md.append("")

        md.append(f"**Latency:** original={med_orig:.2f}ms, scaled={med_scaled:.2f}ms, "
                   f"ratio={latency_ratio:.1f}×")
        md.append(f"**Row count ratio:** {scale_factor:.0f}×")
        md.append(f"**Latency vs row ratio:** {'proportional' if 0.3 * scale_factor <= latency_ratio <= 3.0 * scale_factor else 'sub-proportional' if latency_ratio < 0.3 * scale_factor else 'super-proportional'}")
        md.append("")

    conn_orig.close()
    conn_scaled.close()
    return "\n".join(md)


def main():
    STAGE3_DIR.mkdir(parents=True, exist_ok=True)

    print("Running optimizer awareness check...")
    oa = optimizer_awareness_check()
    (STAGE3_DIR / "optimizer_awareness.md").write_text(oa)
    print(f"  Written: {STAGE3_DIR / 'optimizer_awareness.md'}")

    print("\nRunning plan shape difference analysis...")
    ps = plan_shape_diffs()
    (STAGE3_DIR / "scale_plan_diffs.md").write_text(ps)
    print(f"  Written: {STAGE3_DIR / 'scale_plan_diffs.md'}")


if __name__ == "__main__":
    main()
