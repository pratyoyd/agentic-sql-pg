#!/usr/bin/env python3
"""
Stage 2: Replay all 31 tasks (rep a) against Postgres, compute all metrics,
generate side-by-side report with DuckDB numbers.
"""

import hashlib
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import psycopg

from pg_plan_parser import (
    extract_plan_tree, is_plan_critical, is_hintable,
    PLAN_CRITICAL_OPS, BOOKKEEPING_OPS,
)
from pg_loader import load_all_tasks, ALL_TASKS
from sql_adapter import adapt_sql_for_postgres

CONNINFO = "host=localhost port=5434 dbname=agentic_poc"
DUCKDB_LOG_DIR = Path("/scratch/agentic-sql/logs")
OUTPUT_DIR = Path("stage2/raw_plans")
REPORT_DIR = Path("stage2")


# ----------- Replay -----------

def load_duckdb_trace(task: str, rep: str = "a") -> list[dict]:
    path = DUCKDB_LOG_DIR / f"{task}_sweep_{rep}.jsonl"
    return [json.loads(line) for line in open(path)]


def replay_task(task: str) -> dict:
    """Replay rep a of a task against Postgres. Returns structured result."""
    trace = load_duckdb_trace(task, "a")
    plan_dir = OUTPUT_DIR / task
    plan_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg.connect(CONNINFO, autocommit=True)
    results = []
    errors = []

    for i, entry in enumerate(trace):
        raw_sql = entry.get("raw_sql", "")
        if not raw_sql.strip():
            continue

        pg_sql = adapt_sql_for_postgres(raw_sql)

        try:
            row = conn.execute(
                f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {pg_sql}"
            ).fetchone()
            plan_json = row[0]

            with open(plan_dir / f"{i}.json", "w") as f:
                json.dump(plan_json, f, indent=2)

            nodes = extract_plan_tree(plan_json)
            results.append({
                "query_idx": i,
                "raw_sql": raw_sql,
                "pg_sql": pg_sql,
                "nodes": nodes,
                "plan_json": plan_json,
                # Carry over DuckDB-recorded metadata for structural metrics
                "tables": entry.get("tables", []),
                "columns": entry.get("columns", []),
                "predicates": entry.get("predicates", []),
                "group_by_cols": entry.get("group_by_cols", []),
                "template": entry.get("template", ""),
                "result_rows": entry.get("result_rows", 0),
                "query_start_ts": entry.get("query_start_ts"),
                "prev_query_end_ts": entry.get("prev_query_end_ts"),
            })
        except Exception as e:
            err_msg = str(e).split('\n')[0]
            errors.append({"query_idx": i, "error": err_msg, "pg_sql": pg_sql[:300]})
            with open(plan_dir / f"{i}_error.txt", "w") as f:
                f.write(f"SQL: {pg_sql}\n\nError: {e}")

    conn.close()
    return {
        "task": task,
        "total_queries": len(trace),
        "parsed": len(results),
        "errors": errors,
        "results": results,
    }


# ----------- Metrics (mirrors DuckDB metrics.py) -----------

def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def _bootstrap_ci(values, n_boot=1000, ci=0.95):
    if not values:
        return {"mean": None, "ci_lower": None, "ci_upper": None, "n": 0}
    arr = np.array(values)
    observed_mean = float(np.mean(arr))
    rng = np.random.default_rng(42)
    boot_means = sorted(
        float(np.mean(rng.choice(arr, size=len(arr), replace=True)))
        for _ in range(n_boot)
    )
    alpha = (1 - ci) / 2
    return {
        "mean": observed_mean,
        "ci_lower": boot_means[int(alpha * n_boot)],
        "ci_upper": boot_means[int((1 - alpha) * n_boot)],
        "n": len(values),
    }


def _percentiles(values):
    if not values:
        return {}
    arr = np.array(values)
    return {
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "p25": float(np.percentile(arr, 25)),
        "p75": float(np.percentile(arr, 75)),
        "p95": float(np.percentile(arr, 95)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "n": len(values),
    }


def compute_all_metrics(task_results: dict[str, dict]) -> dict:
    """Compute all metrics across all tasks. Each task's results are a session."""
    # Build sessions list (list of query entry lists)
    sessions = []
    for task in ALL_TASKS:
        tr = task_results.get(task)
        if tr and tr["results"]:
            sessions.append(tr["results"])

    metrics = {}

    # --- Group A: Structural ---
    # Table Jaccard
    tj_vals = []
    for session in sessions:
        for i in range(1, len(session)):
            a = set(session[i-1].get("tables", []))
            b = set(session[i].get("tables", []))
            tj_vals.append(_jaccard(a, b))
    metrics["table_jaccard"] = {**_percentiles(tj_vals), "bootstrap": _bootstrap_ci(tj_vals)}

    # Column Jaccard
    for role, extractor in [
        ("select_cols", lambda e: set(e.get("columns", []))),
        ("where_cols", lambda e: set(p["column"] for p in e.get("predicates", []))),
        ("groupby_cols", lambda e: set(e.get("group_by_cols", []))),
    ]:
        vals = []
        for session in sessions:
            for i in range(1, len(session)):
                a = extractor(session[i-1])
                b = extractor(session[i])
                vals.append(_jaccard(a, b))
        metrics[f"col_jaccard_{role}"] = {**_percentiles(vals), "bootstrap": _bootstrap_ci(vals)}

    # Template repetition
    templ_vals = []
    for session in sessions:
        if len(session) < 2:
            continue
        seen = set()
        repeats = 0
        for e in session:
            t = e.get("template", "")
            if t in seen:
                repeats += 1
            seen.add(t)
        templ_vals.append(repeats / len(session))
    metrics["template_repetition"] = {**_percentiles(templ_vals), "bootstrap": _bootstrap_ci(templ_vals)}

    # Inter-query gap (from DuckDB-recorded timestamps)
    gaps = []
    for session in sessions:
        for e in session:
            s = e.get("query_start_ts")
            p = e.get("prev_query_end_ts")
            if s is not None and p is not None:
                gap = s - p
                if gap >= 0:
                    gaps.append(gap)
    metrics["inter_query_gap"] = {**_percentiles(gaps), "bootstrap": _bootstrap_ci(gaps)}

    # Session length
    lengths = [float(len(s)) for s in sessions]
    metrics["session_length"] = {**_percentiles(lengths), "bootstrap": _bootstrap_ci(lengths)}

    # --- Group B: Opportunity ---
    # Result cache hit rate
    hits = 0
    total_cacheable = 0
    rows_saved = []
    for session in sessions:
        for i in range(1, len(session)):
            qi = session[i]
            qi_tables = set(qi.get("tables", []))
            qi_preds = set((p["column"], p["operator"], p["value"]) for p in qi.get("predicates", []))
            qi_cols = set(qi.get("columns", []))
            qi_gb = set(qi.get("group_by_cols", []))
            total_cacheable += 1
            for j in range(i):
                qj = session[j]
                qj_tables = set(qj.get("tables", []))
                qj_preds = set((p["column"], p["operator"], p["value"]) for p in qj.get("predicates", []))
                qj_cols = set(qj.get("columns", []))
                qj_gb = set(qj.get("group_by_cols", []))
                if qi_tables != qj_tables:
                    continue
                if not qj_preds.issubset(qi_preds):
                    continue
                if not qi_cols.issubset(qj_cols):
                    continue
                if qj_gb and not qi_gb.issubset(qj_gb):
                    continue
                hits += 1
                saved = qj.get("result_rows", 0) - qi.get("result_rows", 0)
                rows_saved.append(max(0, saved))
                break
    metrics["result_cache_hit_rate"] = {
        "hit_rate": hits / total_cacheable if total_cacheable else 0,
        "hits": hits, "total": total_cacheable,
        "mean_rows_saved": float(np.mean(rows_saved)) if rows_saved else 0,
    }

    # Cardinality reuse rate (from Postgres plans)
    total_nodes = 0
    hit_nodes = 0
    hits_by_type = Counter()
    totals_by_type = Counter()
    hintable_by_type = Counter()
    q_errors_all = []
    q_errors_pc = []
    terminal_agg = 0
    nonterminal_agg = 0

    for session in sessions:
        seen_sigs = {}
        for entry in session:
            for node in entry.get("nodes", []):
                sig = node["operator_signature"]
                op = node["operator_type"]
                ec = node.get("estimated_card")
                ac = node.get("actual_card")
                if not sig or not op:
                    continue
                total_nodes += 1
                totals_by_type[op] += 1

                if sig in seen_sigs:
                    hit_nodes += 1
                    hits_by_type[op] += 1

                    hintable, _ = is_hintable(node)
                    if hintable:
                        hintable_by_type[op] += 1

                    if ec and ac and ec > 0 and ac > 0:
                        qe = max(ec, ac) / min(ec, ac)
                        q_errors_all.append(qe)
                        if is_plan_critical(op):
                            q_errors_pc.append(qe)
                else:
                    seen_sigs[sig] = ac

    # Split by importance
    def _split(op_set):
        h = sum(hits_by_type.get(op, 0) for op in op_set)
        t = sum(totals_by_type.get(op, 0) for op in op_set)
        hint = sum(hintable_by_type.get(op, 0) for op in op_set)
        return {"hits": h, "total": t, "hit_rate": h/t if t else 0,
                "hintable": hint, "hint_rate": hint/h if h else 0}

    metrics["cardinality_reuse"] = {
        "overall": {"hits": hit_nodes, "total": total_nodes,
                    "hit_rate": hit_nodes/total_nodes if total_nodes else 0},
        "plan_critical": _split(PLAN_CRITICAL_OPS),
        "bookkeeping": _split(BOOKKEEPING_OPS),
        "by_type": {
            op: {"hits": hits_by_type.get(op, 0), "total": totals_by_type[op],
                 "hit_rate": hits_by_type.get(op, 0)/totals_by_type[op] if totals_by_type[op] else 0,
                 "hintable": hintable_by_type.get(op, 0),
                 "hint_rate": hintable_by_type.get(op, 0)/hits_by_type.get(op, 1) if hits_by_type.get(op, 0) else 0}
            for op in sorted(totals_by_type.keys())
        },
        "q_error_all": _percentiles(q_errors_all) if q_errors_all else {},
        "q_error_plan_critical": _percentiles(q_errors_pc) if q_errors_pc else {},
    }

    # --- Group C: Prediction inputs ---
    # GROUP BY prediction
    mf_top1 = mf_top3 = ls_top1 = ls_top3 = mk_top1 = mk_top3 = 0
    total_preds = 0
    for session in sessions:
        gb_history = []
        transitions = defaultdict(Counter)
        for entry in session:
            gb = tuple(sorted(entry.get("group_by_cols", [])))
            if not gb:
                gb_history.append(gb)
                continue
            if not gb_history:
                gb_history.append(gb)
                continue
            total_preds += 1
            # Most frequent
            freq = Counter(gb_history)
            mc = freq.most_common(3)
            if mc[0][0] == gb:
                mf_top1 += 1
            if gb in [m[0] for m in mc]:
                mf_top3 += 1
            # Last seen
            if gb_history[-1] == gb:
                ls_top1 += 1
                ls_top3 += 1
            else:
                recent = []
                seen = set()
                for prev in reversed(gb_history):
                    if prev not in seen:
                        recent.append(prev)
                        seen.add(prev)
                    if len(recent) >= 3:
                        break
                if gb in recent:
                    ls_top3 += 1
            # Markov-1
            if transitions[gb_history[-1]]:
                mp = transitions[gb_history[-1]].most_common(3)
                if mp[0][0] == gb:
                    mk_top1 += 1
                if gb in [m[0] for m in mp]:
                    mk_top3 += 1
            if gb_history:
                transitions[gb_history[-1]][gb] += 1
            gb_history.append(gb)

    metrics["groupby_prediction"] = {
        "total": total_preds,
        "most_frequent": {"top1": mf_top1/total_preds if total_preds else 0,
                          "top3": mf_top3/total_preds if total_preds else 0},
        "last_seen": {"top1": ls_top1/total_preds if total_preds else 0,
                      "top3": ls_top3/total_preds if total_preds else 0},
        "markov_1": {"top1": mk_top1/total_preds if total_preds else 0,
                     "top3": mk_top3/total_preds if total_preds else 0},
    }

    # Move sequences
    move_freq = Counter()
    bigram_counts = Counter()
    trigram_counts = Counter()
    for session in sessions:
        moves = []
        for i, entry in enumerate(session):
            if i == 0:
                moves.append("overview" if not entry.get("predicates") else "other")
                continue
            prev = session[i-1]
            cur = entry
            ct = set(cur.get("tables", []))
            pt = set(prev.get("tables", []))
            cg = set(cur.get("group_by_cols", []))
            pg_ = set(prev.get("group_by_cols", []))
            cp = set((p["column"], p["operator"], p["value"]) for p in cur.get("predicates", []))
            pp = set((p["column"], p["operator"], p["value"]) for p in prev.get("predicates", []))
            if ct != pt:
                moves.append("cross_table")
            elif cg > pg_ and pg_ and ct == pt:
                moves.append("drill_down")
            elif cp > pp and ct == pt and cg == pg_:
                moves.append("deepen")
            elif cp - pp and not pp - cp and ct == pt and cg == pg_ and cp != pp:
                moves.append("narrow")
            elif pp > cp and ct == pt:
                moves.append("widen")
            elif cg != pg_ and ct == pt:
                moves.append("pivot")
            elif ct == pt and cp == pp and cg == pg_:
                moves.append("reframe")
            else:
                moves.append("other")
        move_freq.update(moves)
        for j in range(1, len(moves)):
            bigram_counts[(moves[j-1], moves[j])] += 1
        for j in range(2, len(moves)):
            trigram_counts[(moves[j-2], moves[j-1], moves[j])] += 1

    metrics["move_sequences"] = {
        "frequencies": dict(move_freq.most_common()),
        "top_bigrams": [{"seq": list(k), "count": v} for k, v in bigram_counts.most_common(10)],
        "top_trigrams": [{"seq": list(k), "count": v} for k, v in trigram_counts.most_common(10)],
    }

    # Anchor dimensions
    anchors = {}
    for session in sessions:
        sid = session[0].get("task", session[0].get("tables", ["unknown"])[0]) if session else "unknown"
        # Use the task name from the plan dir
        gb_queries = [e for e in session if e.get("group_by_cols")]
        if not gb_queries:
            continue
        col_counts = Counter()
        for e in gb_queries:
            for col in e.get("group_by_cols", []):
                col_counts[col] += 1
        n_gb = len(gb_queries)
        anch = sorted(col for col, count in col_counts.items() if count / n_gb >= 0.5)
        if anch:
            anchors[sid] = anch

    metrics["anchor_dimensions"] = anchors

    return metrics


# ----------- Report generation -----------

# DuckDB reference numbers (from sweep_20260411_metrics.json and report)
DUCKDB_REF = {
    "table_jaccard_mean": 0.931,
    "col_jaccard_select_mean": 0.520,
    "col_jaccard_where_mean": 0.640,
    "col_jaccard_groupby_mean": 0.318,
    "template_repetition_mean": 0.001,
    "inter_query_gap_median": 11.568,
    "session_length_mean": 14.106,
    "result_cache_hit_rate": 0.147,
    "card_reuse_overall": 0.886,
    "card_reuse_plan_critical": 0.748,
    "q_error_pc_mean": 5.91,
    "q_error_pc_p95": 19.64,
    "groupby_mf_top1": 0.133,
    "groupby_ls_top1": 0.133,
    "groupby_mk_top1": 0.055,
}


def generate_report(metrics: dict, task_results: dict, task_errors: dict) -> str:
    md = []
    md.append("# Stage 2: Postgres Baseline Replay — Full 31-Task Characterization")
    md.append("")
    md.append("## Environment")
    md.append("- PostgreSQL 16.13, pg_hint_plan 1.6.1")
    md.append("- Python 3.12.3, psycopg 3.3.3")
    md.append("- Linux 6.8.0-57-generic")
    md.append("- Data: InsightBench 31 tasks, rep a, ~500 rows per table")
    md.append("")

    # Replay summary
    total_q = sum(tr["parsed"] for tr in task_results.values())
    total_err = sum(len(tr["errors"]) for tr in task_results.values())
    md.append("## Replay Summary")
    md.append(f"- **Tasks replayed:** {len(task_results)}")
    md.append(f"- **Queries executed:** {total_q}")
    md.append(f"- **Query errors:** {total_err}")
    if task_errors:
        md.append("")
        md.append("**Tasks with errors:**")
        for task, errs in sorted(task_errors.items()):
            md.append(f"- `{task}`: {len(errs)} errors")
            for e in errs[:3]:
                md.append(f"  - Q{e['query_idx']}: {e['error'][:100]}")
    md.append("")

    # Group A
    md.append("## Structural Characterization (Group A)")
    md.append("")
    md.append("| Metric | Postgres | DuckDB | Delta |")
    md.append("|--------|----------|--------|-------|")

    def _row(name, pg_val, dk_key):
        dk = DUCKDB_REF.get(dk_key, 0)
        delta = pg_val - dk if pg_val is not None else "N/A"
        return f"| {name} | {pg_val:.3f} | {dk:.3f} | {delta:+.3f} |" if pg_val is not None else f"| {name} | N/A | {dk:.3f} | N/A |"

    tj = metrics["table_jaccard"]
    md.append(_row("Table Jaccard (mean)", tj.get("mean"), "table_jaccard_mean"))
    for role, dk_key in [("select_cols", "col_jaccard_select_mean"),
                          ("where_cols", "col_jaccard_where_mean"),
                          ("groupby_cols", "col_jaccard_groupby_mean")]:
        cj = metrics.get(f"col_jaccard_{role}", {})
        md.append(_row(f"Col Jaccard ({role})", cj.get("mean"), dk_key))
    tr = metrics.get("template_repetition", {})
    md.append(_row("Template repetition", tr.get("mean"), "template_repetition_mean"))
    gap = metrics.get("inter_query_gap", {})
    md.append(_row("Inter-query gap median (s)", gap.get("median"), "inter_query_gap_median"))
    sl = metrics.get("session_length", {})
    md.append(_row("Session length (mean)", sl.get("mean"), "session_length_mean"))
    md.append("")

    # Group B
    md.append("## Opportunity Quantification (Group B)")
    md.append("")

    rch = metrics["result_cache_hit_rate"]
    md.append(f"### Result Cache Hit Rate")
    md.append(f"- Postgres: **{rch['hit_rate']:.3f}** ({rch['hits']}/{rch['total']})")
    md.append(f"- DuckDB: {DUCKDB_REF['result_cache_hit_rate']:.3f}")
    md.append("")

    cr = metrics["cardinality_reuse"]
    md.append("### Cardinality Reuse Rate")
    md.append("")
    md.append("| Split | Postgres Hit Rate | DuckDB Hit Rate | Postgres Hintable |")
    md.append("|-------|-------------------|-----------------|-------------------|")
    pc = cr["plan_critical"]
    md.append(f"| **Plan-critical** | **{pc['hit_rate']:.3f}** ({pc['hits']}/{pc['total']}) "
              f"| {DUCKDB_REF['card_reuse_plan_critical']:.3f} "
              f"| **{pc['hint_rate']:.3f}** ({pc['hintable']}/{pc['hits']}) |")
    bk = cr["bookkeeping"]
    md.append(f"| Bookkeeping | {bk['hit_rate']:.3f} ({bk['hits']}/{bk['total']}) "
              f"| 0.958 "
              f"| {bk['hint_rate']:.3f} ({bk['hintable']}/{bk['hits']}) |")
    ov = cr["overall"]
    md.append(f"| Overall | {ov['hit_rate']:.3f} ({ov['hits']}/{ov['total']}) "
              f"| {DUCKDB_REF['card_reuse_overall']:.3f} | — |")
    md.append("")

    # By operator type
    md.append("**By operator type (plan-critical, ≥5 total):**")
    md.append("| Operator | Hits | Total | Hit Rate | Hintable | Hint Rate |")
    md.append("|----------|------|-------|----------|----------|-----------|")
    for op in sorted(cr["by_type"].keys()):
        d = cr["by_type"][op]
        if d["total"] >= 5 and is_plan_critical(op):
            md.append(f"| {op} | {d['hits']} | {d['total']} | {d['hit_rate']:.3f} "
                      f"| {d['hintable']} | {d['hint_rate']:.3f} |")
    md.append("")

    # Q-error
    qe_pc = cr.get("q_error_plan_critical", {})
    if qe_pc:
        md.append("### Q-error on Plan-Critical Reuse Hits")
        md.append(f"- Postgres: mean={qe_pc.get('mean',0):.2f}, P95={qe_pc.get('p95',0):.2f}, N={qe_pc.get('n',0)}")
        md.append(f"- DuckDB: mean={DUCKDB_REF['q_error_pc_mean']:.2f}, P95={DUCKDB_REF['q_error_pc_p95']:.2f}")
        md.append("")

    # Group C
    md.append("## Prediction Inputs (Group C)")
    md.append("")

    gp = metrics["groupby_prediction"]
    md.append(f"### GROUP BY Prediction (N={gp['total']})")
    md.append("| Predictor | Postgres Top-1 | DuckDB Top-1 | Postgres Top-3 |")
    md.append("|-----------|---------------|--------------|----------------|")
    for name, dk_key, label in [
        ("most_frequent", "groupby_mf_top1", "Most Frequent"),
        ("last_seen", "groupby_ls_top1", "Last Seen"),
        ("markov_1", "groupby_mk_top1", "Markov-1"),
    ]:
        d = gp[name]
        md.append(f"| {label} | {d['top1']:.3f} | {DUCKDB_REF[dk_key]:.3f} | {d['top3']:.3f} |")
    md.append("")

    ms = metrics["move_sequences"]
    md.append("### Move Sequence Frequencies")
    md.append("| Move | Count | Fraction |")
    md.append("|------|-------|----------|")
    total_moves = sum(ms["frequencies"].values())
    for move, count in sorted(ms["frequencies"].items(), key=lambda x: -x[1]):
        md.append(f"| {move} | {count} | {count/total_moves:.3f} |")
    md.append("")

    md.append("**Top-5 bigrams:**")
    md.append("| Sequence | Count |")
    md.append("|----------|-------|")
    for bg in ms["top_bigrams"][:5]:
        md.append(f"| {' → '.join(bg['seq'])} | {bg['count']} |")
    md.append("")

    # Anchors
    ad = metrics["anchor_dimensions"]
    if ad:
        md.append("### Anchor Dimensions (≥50% of GROUP BYs)")
        md.append("| Task/Session | Anchors |")
        md.append("|-------------|---------|")
        for sid, anch in sorted(ad.items()):
            md.append(f"| {sid} | {', '.join(anch)} |")
        md.append("")

    return "\n".join(md)


def main():
    print("[stage2] Loading all 31 InsightBench tasks into Postgres...")
    conn = psycopg.connect(CONNINFO, autocommit=True)
    load_all_tasks(conn)
    conn.close()

    print("\n[stage2] Replaying all 31 tasks (rep a)...")
    task_results = {}
    task_errors = {}
    for task in ALL_TASKS:
        print(f"  [stage2] Replaying {task}...", end=" ", flush=True)
        tr = replay_task(task)
        task_results[task] = tr
        if tr["errors"]:
            task_errors[task] = tr["errors"]
        print(f"{tr['parsed']}/{tr['total_queries']} ok, {len(tr['errors'])} errors")

    # Summary
    total_ok = sum(tr["parsed"] for tr in task_results.values())
    total_err = sum(len(tr["errors"]) for tr in task_results.values())
    total_q = sum(tr["total_queries"] for tr in task_results.values())
    print(f"\n[stage2] Replay complete: {total_ok}/{total_q} queries ({total_err} errors)")

    if task_errors:
        print("\n[stage2] Tasks with errors:")
        for task, errs in sorted(task_errors.items()):
            print(f"  {task}: {len(errs)} errors")
            for e in errs[:2]:
                print(f"    Q{e['query_idx']}: {e['error'][:120]}")

    print("\n[stage2] Computing metrics...")
    metrics = compute_all_metrics(task_results)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("[stage2] Generating report...")
    report = generate_report(metrics, task_results, task_errors)
    report_path = REPORT_DIR / "stage2_report.md"
    with open(report_path, "w") as f:
        f.write(report)
    print(f"[stage2] Report: {report_path}")

    # Save metrics JSON
    metrics_path = REPORT_DIR / "stage2_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"[stage2] Metrics: {metrics_path}")

    # Print headline numbers
    cr = metrics["cardinality_reuse"]
    pc = cr["plan_critical"]
    print(f"\n[stage2] === HEADLINE NUMBERS ===")
    print(f"  Plan-critical reuse rate: {pc['hit_rate']:.3f} (flag-28 was 0.449)")
    print(f"  Plan-critical hint applicability: {pc['hint_rate']:.3f} (flag-28 was 0.688)")
    print(f"  Q-error plan-critical: mean={cr['q_error_plan_critical'].get('mean',0):.2f} "
          f"(DuckDB was {DUCKDB_REF['q_error_pc_mean']:.2f})")


if __name__ == "__main__":
    main()
