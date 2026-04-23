#!/usr/bin/env python3
"""
Generate reports/scenario1_genre_evolution.md from session traces.
Designed for progressive emission: run after each rep completes.
"""

import json
import re
import subprocess
import time
from pathlib import Path

import psycopg

SESSION_DIR = Path("sessions")
REPORT_PATH = Path("reports/scenario1_genre_evolution.md")
CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"

REPS = ["a", "b", "c"]


def load_trace(rep: str) -> list[dict]:
    path = SESSION_DIR / f"scenario1_rep_{rep}.jsonl"
    if not path.exists():
        return []
    return [json.loads(l) for l in open(path)]


def load_summary(rep: str) -> dict | None:
    path = SESSION_DIR / f"scenario1_rep_{rep}_summary.json"
    if not path.exists():
        return None
    return json.load(open(path))


def load_workspace(rep: str) -> dict | None:
    path = SESSION_DIR / f"scenario1_rep_{rep}_workspace.json"
    if not path.exists():
        return None
    return json.load(open(path))


def compute_per_rep(rep: str) -> dict | None:
    trace = load_trace(rep)
    summary = load_summary(rep)
    ws = load_workspace(rep)
    if not trace or not summary:
        return None

    query_count = len(trace)
    successful = [r for r in trace if r.get("success")]
    tables_per_q = [len(r.get("tables", [])) for r in successful]
    plan_depths = []
    for r in successful:
        pt = r.get("plan_tree", [])
        if pt:
            # depth = max node_id + 1 (rough proxy)
            plan_depths.append(max(n.get("node_id", 0) for n in pt) + 1)

    db_compute_ms = sum(r.get("execution_ms", 0) for r in trace)

    return {
        "rep": rep,
        "query_count": query_count,
        "tables_mean": round(sum(tables_per_q) / len(tables_per_q), 1) if tables_per_q else 0,
        "tables_p50": sorted(tables_per_q)[len(tables_per_q) // 2] if tables_per_q else 0,
        "tables_p95": sorted(tables_per_q)[int(len(tables_per_q) * 0.95)] if tables_per_q else 0,
        "plan_depth_mean": round(sum(plan_depths) / len(plan_depths), 1) if plan_depths else 0,
        "plan_depth_max": max(plan_depths) if plan_depths else 0,
        "wall_clock_s": summary.get("wall_clock_seconds", 0),
        "db_compute_s": round(db_compute_ms / 1000, 1),
        "final_answer": summary.get("final_answer") is not None,
        "done_clean": summary.get("final_answer") is not None,
    }


def compute_m1_metrics(reps_data: list[str]) -> dict:
    """Compute M1 (workspace) metrics across completed reps."""
    totals = {
        "save_calls": 0, "state_acknowledged": 0,
        "reuse_count": 0,
        "save_but_never_reused": 0, "qualified_but_not_saved": 0,
        "total_queries": 0,
        "save_decisions_emitted": 0, "skip_decisions_emitted": 0,
        "protocol_turns": 0,
    }

    for rep in reps_data:
        trace = load_trace(rep)
        ws = load_workspace(rep)
        if not trace:
            continue

        # Count workspace save calls from trace flags
        save_calls = sum(1 for r in trace if r.get("workspace_save"))
        totals["save_calls"] += save_calls

        # Parse structured protocol markers from agent responses
        for r in trace:
            resp = r.get("agent_response", "")
            has_ws = bool(re.search(r'-- WORKSPACE STATE:', resp))
            has_reuse = bool(re.search(r'-- REUSE:', resp))
            has_save_dec = bool(re.search(r'-- SAVE DECISION:', resp))
            # A turn with any of the three protocol markers counts once
            if has_ws or has_reuse or has_save_dec:
                totals["protocol_turns"] += 1
            if re.search(r'-- SAVE DECISION.*?:\s*SAVE(?:_CTE)?\b', resp):
                totals["save_decisions_emitted"] += 1
            if re.search(r'-- SAVE DECISION.*?:\s*SKIP\b', resp):
                totals["skip_decisions_emitted"] += 1
            # State acknowledged: agent emits -- WORKSPACE STATE: with content
            # (not "empty" / "none" / blank after the colon)
            if has_ws:
                m = re.search(r'-- WORKSPACE STATE:\s*(.+)', resp)
                if m:
                    val = m.group(1).strip().lower()
                    if val and val not in ("empty", "none", "n/a", "{}"):
                        totals["state_acknowledged"] += 1

        # Reuse: detect queries referencing saved temp table names
        saved_tables = set()
        if ws and "activity" in ws:
            for evt in ws["activity"]:
                if evt.get("call_type") == "save" and evt.get("payload"):
                    hint = evt["payload"].get("usage_hint", "")
                    m = re.search(r'FROM\s+(\S+)', hint)
                    if m:
                        saved_tables.add(m.group(1))

        reuse_count = 0
        reused_names = set()
        for r in trace:
            sql = r.get("raw_sql", "").lower()
            for t in saved_tables:
                if t.lower() in sql and not r.get("workspace_save"):
                    reuse_count += 1
                    reused_names.add(t)
                    break

        totals["reuse_count"] += reuse_count

        # Save but never reused (regex-based: saved tables not found in any subsequent query)
        totals["save_but_never_reused"] += len(saved_tables - reused_names)

        # Qualified but not saved: queries with execution_ms > 3000 or 4+ tables
        for r in trace:
            if not r.get("success"):
                continue
            ms = r.get("execution_ms", 0)
            ntables = len(r.get("tables", []))
            rows = r.get("result_rows", 0)
            if (ms > 3000 or ntables >= 4) and rows < 200000:
                if not r.get("workspace_save"):
                    totals["qualified_but_not_saved"] += 1

        totals["total_queries"] += len(trace)

    totals["reuse_rate"] = (
        round(totals["reuse_count"] / totals["save_calls"], 2)
        if totals["save_calls"] > 0 else 0
    )
    totals["state_ack_rate"] = (
        round(totals["state_acknowledged"] /
              max(1, totals["total_queries"] - totals["save_calls"]), 2)
    )
    return totals


def compute_m2_metrics(reps_data: list[str]) -> dict:
    """Compute M2 (intent declaration) metrics across completed reps."""
    totals = {
        "declare_calls": 0, "declared_variants_total": 0,
        "actual_variants_issued": 0, "abandonment_with_justification": 0,
        "missed_clusters": 0,
    }

    for rep in reps_data:
        trace = load_trace(rep)
        if not trace:
            continue

        for r in trace:
            resp = r.get("agent_response", "")

            # Detect MATERIALIZE_INTENT blocks
            intents = re.findall(r'/\*\+\s*MATERIALIZE_INTENT.*?\*/', resp, re.DOTALL)
            totals["declare_calls"] += len(intents)
            for intent in intents:
                m = re.search(r"variants\s*=\s*(\d+)", intent)
                if m:
                    totals["declared_variants_total"] += int(m.group(1))

            # Detect abandonments
            abandons = re.findall(r'--\s*ABANDONING\s+variant', resp, re.IGNORECASE)
            totals["abandonment_with_justification"] += len(abandons)

    totals["declaration_precision"] = (
        round(totals["actual_variants_issued"] / totals["declared_variants_total"], 2)
        if totals["declared_variants_total"] > 0 else 0
    )
    return totals


def run_ground_truth() -> str:
    """Run the reference query to identify actual top-3 genre shifts."""
    sql = """
    WITH decade_ratings AS (
        SELECT mi.info AS genre,
               CASE WHEN t.production_year BETWEEN 1990 AND 1999 THEN '1990s'
                    WHEN t.production_year BETWEEN 2010 AND 2019 THEN '2010s'
               END AS decade,
               mii_r.info::numeric AS rating,
               mii_v.info::bigint AS votes
        FROM title t
        JOIN movie_info mi ON mi.movie_id = t.id
          AND mi.info_type_id = (SELECT id FROM info_type WHERE info = 'genres')
        JOIN movie_info_idx mii_r ON mii_r.movie_id = t.id
          AND mii_r.info_type_id = (SELECT id FROM info_type WHERE info = 'rating')
        JOIN movie_info_idx mii_v ON mii_v.movie_id = t.id
          AND mii_v.info_type_id = (SELECT id FROM info_type WHERE info = 'votes')
        WHERE t.kind_id = 1
          AND t.production_year BETWEEN 1990 AND 2019
          AND mii_v.info::bigint >= 100
    )
    SELECT genre,
           ROUND(SUM(CASE WHEN decade='1990s' THEN rating*votes END) /
                 NULLIF(SUM(CASE WHEN decade='1990s' THEN votes END), 0), 2) AS avg_90s,
           SUM(CASE WHEN decade='1990s' THEN 1 ELSE 0 END) AS n_90s,
           ROUND(SUM(CASE WHEN decade='2010s' THEN rating*votes END) /
                 NULLIF(SUM(CASE WHEN decade='2010s' THEN votes END), 0), 2) AS avg_10s,
           SUM(CASE WHEN decade='2010s' THEN 1 ELSE 0 END) AS n_10s,
           ROUND(SUM(CASE WHEN decade='2010s' THEN rating*votes END) /
                 NULLIF(SUM(CASE WHEN decade='2010s' THEN votes END), 0) -
                 SUM(CASE WHEN decade='1990s' THEN rating*votes END) /
                 NULLIF(SUM(CASE WHEN decade='1990s' THEN votes END), 0), 3) AS shift
    FROM decade_ratings
    WHERE decade IS NOT NULL
    GROUP BY genre
    HAVING SUM(CASE WHEN decade='1990s' THEN 1 ELSE 0 END) >= 50
       AND SUM(CASE WHEN decade='2010s' THEN 1 ELSE 0 END) >= 50
    ORDER BY ABS(
           SUM(CASE WHEN decade='2010s' THEN rating*votes END) /
           NULLIF(SUM(CASE WHEN decade='2010s' THEN votes END), 0) -
           SUM(CASE WHEN decade='1990s' THEN rating*votes END) /
           NULLIF(SUM(CASE WHEN decade='1990s' THEN votes END), 0)
         ) DESC
    LIMIT 10;
    """
    try:
        conn = psycopg.connect(CONNINFO, autocommit=True)
        rows = conn.execute(sql).fetchall()
        conn.close()
        lines = ["| Genre | Avg 90s | n 90s | Avg 10s | n 10s | Shift |",
                 "|---|---|---|---|---|---|"]
        for r in rows:
            lines.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]:+.3f} |")
        return "\n".join(lines)
    except Exception as e:
        return f"(ground truth query failed: {e})"


def generate_report():
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    completed_reps = []
    for rep in REPS:
        if load_summary(rep) is not None:
            completed_reps.append(rep)

    if not completed_reps:
        print("No completed reps found.")
        return

    lines = []

    # Header
    lines.append("# Scenario 1: Genre Evolution Analysis — Dry Run Report\n")
    lines.append("## Header\n")
    lines.append("- **Scenario**: Genre evolution, 1990–2020")
    lines.append("- **Goal**: Identify 3 genres with largest rating shift between 1990s and 2010s")
    lines.append("- **Model**: Opus (via claude -p --model opus)")
    lines.append("- **Temperature**: default (claude CLI)")
    lines.append(f"- **Reps completed**: {len(completed_reps)} / 3")

    total_wall = sum(load_summary(r).get("wall_clock_seconds", 0) for r in completed_reps)
    total_db = sum(sum(t.get("execution_ms", 0) for t in load_trace(r)) / 1000
                   for r in completed_reps)
    lines.append(f"- **Total wall-clock**: {total_wall:.0f}s")
    lines.append(f"- **Total DB compute**: {total_db:.1f}s")
    lines.append("")

    # Per-rep summary table
    lines.append("## Per-Rep Summary\n")
    lines.append("| Rep | Queries | Tables/q (mean/p50/p95) | Plan depth (mean/max) | Wall-clock | DB compute | Final answer | DONE |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for rep in completed_reps:
        d = compute_per_rep(rep)
        if d:
            lines.append(
                f"| {rep} | {d['query_count']} | "
                f"{d['tables_mean']}/{d['tables_p50']}/{d['tables_p95']} | "
                f"{d['plan_depth_mean']}/{d['plan_depth_max']} | "
                f"{d['wall_clock_s']:.0f}s | {d['db_compute_s']:.1f}s | "
                f"{'yes' if d['final_answer'] else 'no'} | "
                f"{'yes' if d['done_clean'] else 'no'} |"
            )
    lines.append("")

    # M1 metrics
    m1 = compute_m1_metrics(completed_reps)
    lines.append("## M1 Metrics (Workspace)\n")
    lines.append(f"- **m1_save_calls**: {m1['save_calls']}")
    lines.append(f"- **m1_state_acknowledged**: {m1['state_acknowledged']} (turns where agent acknowledged saved content)")
    lines.append(f"- **m1_state_ack_rate**: {m1['state_ack_rate']:.0%}")
    lines.append(f"- **m1_reuse_count**: {m1['reuse_count']} (queries referencing saved temp tables)")
    lines.append(f"- **m1_reuse_rate**: {m1['reuse_rate']} (reuse_count / save_calls)")
    lines.append(f"- **m1_save_but_never_reused**: {m1['save_but_never_reused']}")
    lines.append(f"- **m1_qualified_but_not_saved**: {m1['qualified_but_not_saved']}")
    lines.append(f"- **protocol_turns**: {m1['protocol_turns']} / {m1['total_queries']} (turns with any protocol marker)")
    lines.append(f"- **save_decisions_emitted**: {m1['save_decisions_emitted']} SAVE, {m1['skip_decisions_emitted']} SKIP")
    lines.append("")

    # M2 metrics
    m2 = compute_m2_metrics(completed_reps)
    lines.append("## M2 Metrics (Intent Declaration)\n")
    lines.append(f"- **m2_declare_calls**: {m2['declare_calls']}")
    lines.append(f"- **m2_declared_variants_total**: {m2['declared_variants_total']}")
    lines.append(f"- **m2_actual_variants_issued**: {m2['actual_variants_issued']} (requires manual count)")
    lines.append(f"- **m2_declaration_precision**: {m2['declaration_precision']}")
    lines.append(f"- **m2_abandonment_with_justification**: {m2['abandonment_with_justification']}")
    lines.append("")

    # Ground truth (only after all 3 reps)
    if len(completed_reps) == 3:
        lines.append("## Ground Truth Reference\n")
        lines.append("Vote-weighted average rating shift by genre (films with ≥100 votes, ≥50 films per decade):\n")
        lines.append(run_ground_truth())
        lines.append("")

        # Signal assessment
        lines.append("## Signal Assessment\n")
        save_per_session = m1["save_calls"] / len(completed_reps)
        state_ack_rate = m1["state_ack_rate"]

        signals = []
        if 1 <= save_per_session <= 3:
            signals.append("GREEN: save_calls 1-3/session")
        elif save_per_session > 0:
            signals.append(f"YELLOW: save_calls {save_per_session:.1f}/session (outside 1-3 range)")
        else:
            signals.append("RED: zero workspace saves despite opportunities")

        if state_ack_rate >= 0.6:
            signals.append(f"GREEN: state acknowledgment rate {state_ack_rate:.0%} ≥ 60%")
        elif state_ack_rate > 0:
            signals.append(f"YELLOW: state acknowledgment rate {state_ack_rate:.0%} < 60%")
        else:
            signals.append("RED: no workspace state acknowledgment")

        if m2["declare_calls"] > 0:
            signals.append(f"GREEN: {m2['declare_calls']} intent declarations emitted")
        else:
            signals.append("YELLOW: no MATERIALIZE_INTENT declarations")

        for s in signals:
            lines.append(f"- {s}")
        lines.append("")

        lines.append("## Verdict\n")
        lines.append("*(To be written after reviewing all metrics and final answers.)*\n")

    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))
    print(f"Report written to {REPORT_PATH} ({len(completed_reps)} reps)")


if __name__ == "__main__":
    generate_report()
