#!/usr/bin/env python3
"""
Hinted replay pilot: task 2 only.
Three-condition replay on IMDB task 2 traces (3 reps).
Baseline (no hints) then Hinted (session-local cardinality feedback).
"""

import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pg_plan_parser import extract_plan_tree
from hint_constructor import build_signature_history, construct_hints

CONNINFO = "host=localhost port=5434 dbname=agentic_imdb"
TRACE_DIR = Path(__file__).resolve().parent.parent / "traces"
PILOT_DIR = Path(__file__).resolve().parent

WARM_TABLES = [
    "title", "cast_info", "movie_info", "movie_info_idx", "name",
    "movie_companies", "keyword", "movie_keyword", "role_type",
    "info_type", "kind_type", "company_name", "company_type",
]

REPS = ["a", "b", "c"]
TASK = "task2"
RUNS_PER_QUERY = 6
DISCARD_RUNS = 2
STATEMENT_TIMEOUT = "600s"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def restart_postgres():
    """Restart Postgres and wait for it to accept connections."""
    log("Restarting Postgres...")
    subprocess.run(
        ["sudo", "pg_ctlcluster", "16", "poc", "restart"],
        check=True, capture_output=True, timeout=30,
    )
    # Wait for ready
    for _ in range(30):
        try:
            conn = psycopg.connect(CONNINFO, autocommit=True)
            conn.execute("SELECT 1")
            conn.close()
            log("Postgres ready.")
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Postgres did not come up after restart")


def warm_cache(conn):
    """Run COUNT(*) on key tables to warm shared buffers."""
    log("Warming cache...")
    for table in WARM_TABLES:
        conn.execute(f"SELECT COUNT(*) FROM {table}")
    log("Cache warm.")


def load_trace(rep):
    """Load trace entries for a rep, return only successful queries."""
    path = TRACE_DIR / f"{TASK}_rep_{rep}.jsonl"
    entries = []
    for line in open(path):
        e = json.loads(line.strip())
        if e.get("success", False):
            entries.append(e)
    return entries


def run_explain_analyze(conn, sql):
    """Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) and return (plan_json, plan_tree, exec_ms)."""
    t0 = time.time()
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
    row = conn.execute(explain_sql).fetchone()
    exec_ms = (time.time() - t0) * 1000
    plan_json = row[0]
    plan_tree = extract_plan_tree(plan_json)
    return plan_json, plan_tree, exec_ms


def run_timed(conn, sql, n_runs=6):
    """Run a query n_runs times and return list of wall-clock ms per run."""
    times = []
    for _ in range(n_runs):
        t0 = time.time()
        try:
            conn.execute(sql)
        except Exception:
            pass  # still record time
        times.append((time.time() - t0) * 1000)
    return times


def median_of_kept(times, discard=2):
    """Discard first `discard` runs, return median of rest."""
    kept = times[discard:]
    return statistics.median(kept) if kept else 0.0


def get_join_nodes(plan_tree):
    """Return list of join nodes from a plan tree."""
    return [n for n in plan_tree if any(
        kw in n.get("operator_type", "")
        for kw in ("Join", "Nested Loop")
    )]


def compute_qerror(est, act):
    """Compute q-error, handling zeros."""
    if act == 0 or est == 0:
        return None
    return max(est / act, act / est)


def max_qerror_depth3(plan_tree):
    """Max q-error on join nodes with >= 3 relation aliases."""
    worst = 0.0
    worst_node = None
    for node in plan_tree:
        if not any(kw in node.get("operator_type", "") for kw in ("Join", "Nested Loop")):
            continue
        if len(node.get("relation_aliases", [])) < 3:
            continue
        est = node.get("estimated_card", 0)
        act = node.get("actual_card", 0)
        if est and act:
            qe = max(est / act, act / est)
            if qe > worst:
                worst = qe
                worst_node = node
    return worst, worst_node


def construct_hints_for_query(sql, conn, plan_history):
    """Build hint block for a query using session history."""
    t0 = time.time()
    sig_history = build_signature_history(plan_history)
    if not sig_history:
        return sql, "", [], (time.time() - t0) * 1000

    # EXPLAIN without ANALYZE to get predicted plan
    row = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
    vanilla_json = row[0]
    vanilla_tree = extract_plan_tree(vanilla_json)

    hint_block, applied = construct_hints(vanilla_tree, sig_history)
    if hint_block:
        hinted_sql = f"{hint_block}\n{sql}"
    else:
        hinted_sql = sql

    overhead_ms = (time.time() - t0) * 1000
    return hinted_sql, hint_block, applied, overhead_ms


# ─── Step 1: Validation ───

def validate():
    log("=== Step 1: Validation ===")
    conn = psycopg.connect(CONNINFO, autocommit=True)

    # 1. Row counts
    log("Checking row counts...")
    expected = {
        "cast_info": 36_244_344, "title": 2_528_312,
        "movie_info": 14_835_720, "movie_info_idx": 1_380_035,
        "name": 4_167_491, "movie_companies": 2_609_129,
        "keyword": 134_170, "movie_keyword": 4_523_930,
    }
    validation = {"row_counts": {}, "indexes": [], "pg_hint_plan": False, "traces": {}}
    for table, exp in expected.items():
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        validation["row_counts"][table] = cnt
        status = "OK" if abs(cnt - exp) / exp < 0.01 else f"MISMATCH (expected ~{exp})"
        log(f"  {table}: {cnt:,} {status}")

    # 2. pg_hint_plan
    log("Checking pg_hint_plan...")
    try:
        conn.execute("LOAD 'pg_hint_plan'")
        row = conn.execute("SHOW pg_hint_plan.enable_hint").fetchone()
        validation["pg_hint_plan"] = row[0] == "on"
        log(f"  pg_hint_plan.enable_hint = {row[0]}")
    except Exception as e:
        log(f"  pg_hint_plan FAILED: {e}")
        validation["pg_hint_plan"] = False

    # 3. FK indexes
    log("Checking FK indexes...")
    idx_rows = conn.execute("""
        SELECT schemaname, tablename, indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname
    """).fetchall()
    idx_by_table = {}
    for schema, table, idx in idx_rows:
        idx_by_table.setdefault(table, []).append(idx)
        validation["indexes"].append(f"{table}.{idx}")

    needed_tables = ["cast_info", "movie_info", "movie_info_idx",
                     "movie_keyword", "movie_companies"]
    for t in needed_tables:
        idxs = idx_by_table.get(t, [])
        log(f"  {t}: {len(idxs)} indexes: {', '.join(idxs[:5])}")

    # 4. Traces
    log("Checking traces...")
    for rep in REPS:
        entries = load_trace(rep)
        validation["traces"][rep] = len(entries)
        log(f"  rep {rep}: {len(entries)} successful queries")

    conn.close()

    # Save validation report
    val_path = PILOT_DIR / "validation.json"
    with open(val_path, "w") as f:
        json.dump(validation, f, indent=2)
    log(f"Validation saved to {val_path}")

    # Check for failures
    if not validation["pg_hint_plan"]:
        log("FATAL: pg_hint_plan not available")
        sys.exit(1)

    return validation


# ─── Step 3: Baseline condition ───

def run_baseline():
    log("\n=== Step 3: Baseline Condition ===")
    baseline_dir = PILOT_DIR / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    all_results = {}

    for rep in REPS:
        log(f"\n--- Baseline rep {rep} ---")
        restart_postgres()

        conn = psycopg.connect(CONNINFO, autocommit=True)
        conn.execute("LOAD 'pg_hint_plan'")
        conn.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
        warm_cache(conn)

        entries = load_trace(rep)
        rep_dir = baseline_dir / rep
        rep_dir.mkdir(parents=True, exist_ok=True)

        rep_results = []
        plan_history = []  # baseline's own plan history

        for i, entry in enumerate(entries):
            sql = entry["raw_sql"]
            seq = entry.get("query_seq", i)
            log(f"  Q{seq}: {sql[:80]}...")

            try:
                # EXPLAIN ANALYZE
                plan_json, plan_tree, explain_ms = run_explain_analyze(conn, sql)
                with open(rep_dir / f"plan_{seq}.json", "w") as f:
                    json.dump(plan_json, f, indent=2)

                # 6 timed runs
                run_times = run_timed(conn, sql, RUNS_PER_QUERY)
                median_lat = median_of_kept(run_times, DISCARD_RUNS)

                qe_max, qe_node = max_qerror_depth3(plan_tree)

                result = {
                    "query_seq": seq,
                    "sql": sql,
                    "plan_tree_summary": [
                        {"node_id": n["node_id"], "operator_type": n["operator_type"],
                         "estimated_card": n["estimated_card"], "actual_card": n["actual_card"],
                         "relation_aliases": n["relation_aliases"],
                         "operator_signature": n["operator_signature"]}
                        for n in plan_tree
                    ],
                    "run_times_ms": run_times,
                    "median_latency_ms": round(median_lat, 2),
                    "q_error_max": round(qe_max, 2),
                    "success": True,
                    "error": None,
                }
                plan_history.append(plan_tree)
                log(f"    median={median_lat:.1f}ms, qe_max={qe_max:.1f}, runs={[round(t,1) for t in run_times]}")

            except Exception as e:
                log(f"    FAILED: {e}")
                result = {
                    "query_seq": seq, "sql": sql,
                    "plan_tree_summary": [], "run_times_ms": [],
                    "median_latency_ms": None, "q_error_max": None,
                    "success": False, "error": str(e),
                }
                plan_history.append([])  # placeholder

            with open(rep_dir / f"timings_{seq}.json", "w") as f:
                json.dump(result, f, indent=2)
            rep_results.append(result)

        conn.close()

        # Per-rep summary
        summary = {
            "condition": "baseline", "rep": rep,
            "num_queries": len(rep_results),
            "results": rep_results,
        }
        with open(baseline_dir / f"summary_{rep}.json", "w") as f:
            json.dump(summary, f, indent=2)
        all_results[rep] = rep_results

    return all_results


# ─── Step 4: Hinted condition ───

def run_hinted():
    log("\n=== Step 4: Hinted Condition ===")
    hinted_dir = PILOT_DIR / "hinted"
    hinted_dir.mkdir(parents=True, exist_ok=True)
    skipped_hints_log = PILOT_DIR / "skipped_hints.log"

    all_results = {}

    for rep in REPS:
        log(f"\n--- Hinted rep {rep} ---")
        restart_postgres()

        conn = psycopg.connect(CONNINFO, autocommit=True)
        conn.execute("LOAD 'pg_hint_plan'")
        conn.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
        warm_cache(conn)

        entries = load_trace(rep)
        rep_dir = hinted_dir / rep
        rep_dir.mkdir(parents=True, exist_ok=True)

        rep_results = []
        plan_history = []  # hinted condition's own plan history

        for i, entry in enumerate(entries):
            sql = entry["raw_sql"]
            seq = entry.get("query_seq", i)
            log(f"  Q{seq}: {sql[:80]}...")

            try:
                # Construct hints from hinted condition's own history
                hinted_sql, hint_block, applied_hints, overhead_ms = \
                    construct_hints_for_query(sql, conn, plan_history)

                n_hints = len(applied_hints)
                log(f"    hints={n_hints}, overhead={overhead_ms:.1f}ms" +
                    (f", block={hint_block[:100]}" if hint_block else ""))

                # EXPLAIN ANALYZE on hinted query
                plan_json, plan_tree, explain_ms = run_explain_analyze(conn, hinted_sql)
                with open(rep_dir / f"plan_{seq}.json", "w") as f:
                    json.dump(plan_json, f, indent=2)

                # 6 timed runs on hinted query
                run_times = run_timed(conn, hinted_sql, RUNS_PER_QUERY)
                median_lat = median_of_kept(run_times, DISCARD_RUNS)

                qe_max, qe_node = max_qerror_depth3(plan_tree)

                result = {
                    "query_seq": seq,
                    "sql": sql,
                    "hint_block": hint_block,
                    "hinted_sql": hinted_sql,
                    "hints_injected": n_hints,
                    "applied_hints": applied_hints,
                    "injection_overhead_ms": round(overhead_ms, 2),
                    "plan_tree_summary": [
                        {"node_id": n["node_id"], "operator_type": n["operator_type"],
                         "estimated_card": n["estimated_card"], "actual_card": n["actual_card"],
                         "relation_aliases": n["relation_aliases"],
                         "operator_signature": n["operator_signature"]}
                        for n in plan_tree
                    ],
                    "run_times_ms": run_times,
                    "median_latency_ms": round(median_lat, 2),
                    "q_error_max": round(qe_max, 2),
                    "success": True,
                    "error": None,
                }
                plan_history.append(plan_tree)
                log(f"    median={median_lat:.1f}ms, qe_max={qe_max:.1f}, runs={[round(t,1) for t in run_times]}")

            except Exception as e:
                log(f"    FAILED: {e}")
                result = {
                    "query_seq": seq, "sql": sql,
                    "hint_block": "", "hinted_sql": sql,
                    "hints_injected": 0, "applied_hints": [],
                    "injection_overhead_ms": 0,
                    "plan_tree_summary": [], "run_times_ms": [],
                    "median_latency_ms": None, "q_error_max": None,
                    "success": False, "error": str(e),
                }
                plan_history.append([])

            with open(rep_dir / f"timings_{seq}.json", "w") as f:
                json.dump(result, f, indent=2)
            rep_results.append(result)

        conn.close()

        summary = {
            "condition": "hinted", "rep": rep,
            "num_queries": len(rep_results),
            "results": rep_results,
        }
        with open(hinted_dir / f"summary_{rep}.json", "w") as f:
            json.dump(summary, f, indent=2)
        all_results[rep] = rep_results

    return all_results


# ─── Step 5 & 6: Analysis ───

def analyze(baseline_results, hinted_results):
    log("\n=== Step 5-6: Analysis ===")

    per_query = []

    for rep in REPS:
        base_list = baseline_results[rep]
        hint_list = hinted_results[rep]

        # Match by query_seq
        base_by_seq = {r["query_seq"]: r for r in base_list if r["success"]}
        hint_by_seq = {r["query_seq"]: r for r in hint_list if r["success"]}

        for seq in sorted(set(base_by_seq.keys()) & set(hint_by_seq.keys())):
            b = base_by_seq[seq]
            h = hint_by_seq[seq]

            # Compare plan topologies
            b_ops = [(n["node_id"], n["operator_type"], n.get("relation_aliases", []))
                     for n in b["plan_tree_summary"]]
            h_ops = [(n["node_id"], n["operator_type"], n.get("relation_aliases", []))
                     for n in h["plan_tree_summary"]]

            topology_changed = b_ops != h_ops

            # Compare join orders and methods
            b_joins = [(n["operator_type"], tuple(sorted(n.get("relation_aliases", []))))
                       for n in b["plan_tree_summary"]
                       if any(kw in n["operator_type"] for kw in ("Join", "Nested Loop"))]
            h_joins = [(n["operator_type"], tuple(sorted(n.get("relation_aliases", []))))
                       for n in h["plan_tree_summary"]
                       if any(kw in n["operator_type"] for kw in ("Join", "Nested Loop"))]

            b_join_orders = [j[1] for j in b_joins]
            h_join_orders = [j[1] for j in h_joins]
            join_order_changed = b_join_orders != h_join_orders

            b_join_methods = {j[1]: j[0] for j in b_joins}
            h_join_methods = {j[1]: j[0] for j in h_joins}
            join_method_changed = any(
                b_join_methods.get(k) != h_join_methods.get(k)
                for k in set(b_join_methods.keys()) | set(h_join_methods.keys())
            )

            lat_b = b["median_latency_ms"]
            lat_h = h["median_latency_ms"]
            delta_ms = lat_h - lat_b
            delta_pct = (delta_ms / lat_b * 100) if lat_b else 0

            # Count useful hints
            hints_useful = 0
            for ah in h.get("applied_hints", []):
                # Find the matching baseline node's q-error
                for bn in b["plan_tree_summary"]:
                    if bn["operator_signature"] == ah["signature"]:
                        est = bn.get("estimated_card", 0)
                        act = bn.get("actual_card", 0)
                        if est and act:
                            qe = max(est/act, act/est)
                            if qe > 10:
                                hints_useful += 1
                        break

            record = {
                "rep": rep,
                "query_seq": seq,
                "sql": b["sql"][:300],
                "plan_topology_changed": topology_changed,
                "plan_join_order_changed": join_order_changed,
                "plan_join_method_changed": join_method_changed,
                "latency_baseline_ms": round(lat_b, 2),
                "latency_hinted_ms": round(lat_h, 2),
                "latency_delta_ms": round(delta_ms, 2),
                "latency_delta_pct": round(delta_pct, 2),
                "q_error_max_baseline": b.get("q_error_max", 0),
                "q_error_max_hinted": h.get("q_error_max", 0),
                "hints_injected": h.get("hints_injected", 0),
                "hints_useful": hints_useful,
                "injection_overhead_ms": h.get("injection_overhead_ms", 0),
                "is_regression": delta_pct > 10,
                "baseline_run_times": b.get("run_times_ms", []),
                "hinted_run_times": h.get("run_times_ms", []),
            }
            per_query.append(record)

    # Save per-query analysis
    with open(PILOT_DIR / "per_query_analysis.json", "w") as f:
        json.dump(per_query, f, indent=2)

    return per_query


def compute_aggregates(per_query):
    """Compute and return aggregate metrics."""
    total = len(per_query)
    topology_changes = sum(1 for q in per_query if q["plan_topology_changed"])
    order_changes = sum(1 for q in per_query if q["plan_join_order_changed"])
    method_changes = sum(1 for q in per_query if q["plan_join_method_changed"])

    deltas_ms = [q["latency_delta_ms"] for q in per_query]
    deltas_pct = [q["latency_delta_pct"] for q in per_query]

    changed_q = [q for q in per_query if q["plan_topology_changed"]]
    changed_deltas_ms = [q["latency_delta_ms"] for q in changed_q]
    changed_deltas_pct = [q["latency_delta_pct"] for q in changed_q]

    # Per-rep session totals
    rep_totals = {}
    for rep in REPS:
        rq = [q for q in per_query if q["rep"] == rep]
        base_total = sum(q["latency_baseline_ms"] for q in rq)
        hint_total = sum(q["latency_hinted_ms"] for q in rq)
        overhead_total = sum(q["injection_overhead_ms"] for q in rq)
        rep_totals[rep] = {
            "baseline_total_ms": round(base_total, 2),
            "hinted_total_ms": round(hint_total, 2),
            "improvement_ms": round(base_total - hint_total, 2),
            "improvement_pct": round((base_total - hint_total) / base_total * 100, 2) if base_total else 0,
            "overhead_total_ms": round(overhead_total, 2),
            "net_improvement_ms": round(base_total - hint_total - overhead_total, 2),
        }

    overheads = [q["injection_overhead_ms"] for q in per_query]
    regressions = [q for q in per_query if q["is_regression"]]

    qe_reductions = [
        q["q_error_max_baseline"] - q["q_error_max_hinted"]
        for q in per_query
        if q["q_error_max_baseline"] and q["q_error_max_hinted"]
    ]

    def pctl(arr, p):
        if not arr:
            return 0
        arr_sorted = sorted(arr)
        idx = int(len(arr_sorted) * p / 100)
        idx = min(idx, len(arr_sorted) - 1)
        return arr_sorted[idx]

    return {
        "total_queries": total,
        "topology_changes": topology_changes,
        "order_changes": order_changes,
        "method_changes": method_changes,
        "latency_delta_all": {
            "median": round(statistics.median(deltas_ms), 2) if deltas_ms else 0,
            "p25": round(pctl(deltas_ms, 25), 2),
            "p75": round(pctl(deltas_ms, 75), 2),
            "p95": round(pctl(deltas_ms, 95), 2),
            "min": round(min(deltas_ms), 2) if deltas_ms else 0,
            "max": round(max(deltas_ms), 2) if deltas_ms else 0,
        },
        "latency_delta_all_pct": {
            "median": round(statistics.median(deltas_pct), 2) if deltas_pct else 0,
            "min": round(min(deltas_pct), 2) if deltas_pct else 0,
            "max": round(max(deltas_pct), 2) if deltas_pct else 0,
        },
        "latency_delta_changed": {
            "n": len(changed_deltas_ms),
            "median": round(statistics.median(changed_deltas_ms), 2) if changed_deltas_ms else 0,
            "p25": round(pctl(changed_deltas_ms, 25), 2),
            "p75": round(pctl(changed_deltas_ms, 75), 2),
            "min": round(min(changed_deltas_ms), 2) if changed_deltas_ms else 0,
            "max": round(max(changed_deltas_ms), 2) if changed_deltas_ms else 0,
        },
        "rep_totals": rep_totals,
        "overhead": {
            "median": round(statistics.median(overheads), 2) if overheads else 0,
            "p95": round(pctl(overheads, 95), 2),
            "max": round(max(overheads), 2) if overheads else 0,
            "total": round(sum(overheads), 2),
        },
        "regressions": len(regressions),
        "qe_reduction": {
            "median": round(statistics.median(qe_reductions), 2) if qe_reductions else 0,
            "p95": round(pctl(qe_reductions, 95), 2),
            "max": round(max(qe_reductions), 2) if qe_reductions else 0,
            "min": round(min(qe_reductions), 2) if qe_reductions else 0,
        },
    }


# ─── Step 7-8: Report ───

def generate_report(per_query, agg):
    log("\n=== Step 7-8: Report ===")

    lines = ["# Hinted Replay Pilot Report: Task 2 (IMDB)", ""]

    # Section: Aggregate metrics
    lines.append("## Aggregate Metrics")
    lines.append("")
    lines.append(f"**Total queries analyzed**: {agg['total_queries']}")
    lines.append("")

    lines.append("### Plan Change Rates")
    lines.append(f"- Topology changes: {agg['topology_changes']}/{agg['total_queries']} ({agg['topology_changes']/agg['total_queries']*100:.1f}%)")
    lines.append(f"- Join order changes: {agg['order_changes']}/{agg['total_queries']} ({agg['order_changes']/agg['total_queries']*100:.1f}%)")
    lines.append(f"- Join method changes: {agg['method_changes']}/{agg['total_queries']} ({agg['method_changes']/agg['total_queries']*100:.1f}%)")
    lines.append("")

    lines.append("### Latency Delta (all queries)")
    d = agg["latency_delta_all"]
    dp = agg["latency_delta_all_pct"]
    lines.append(f"- Median: {d['median']:.1f}ms ({dp['median']:.1f}%)")
    lines.append(f"- P25: {d['p25']:.1f}ms, P75: {d['p75']:.1f}ms, P95: {d['p95']:.1f}ms")
    lines.append(f"- Min: {d['min']:.1f}ms, Max: {d['max']:.1f}ms")
    lines.append(f"- Range %: {dp['min']:.1f}% to {dp['max']:.1f}%")
    lines.append("")

    if agg["latency_delta_changed"]["n"] > 0:
        lines.append("### Latency Delta (plan-changed queries only)")
        dc = agg["latency_delta_changed"]
        lines.append(f"- N: {dc['n']} queries")
        lines.append(f"- Median: {dc['median']:.1f}ms")
        lines.append(f"- P25: {dc['p25']:.1f}ms, P75: {dc['p75']:.1f}ms")
        lines.append(f"- Min: {dc['min']:.1f}ms, Max: {dc['max']:.1f}ms")
        lines.append("")

    lines.append("### Per-Rep Session Wall-Clock")
    lines.append("")
    lines.append("| Rep | Baseline (ms) | Hinted (ms) | Improvement (ms) | Improvement % | Overhead (ms) | Net (ms) |")
    lines.append("|-----|--------------|-------------|------------------|---------------|---------------|----------|")
    for rep in REPS:
        rt = agg["rep_totals"][rep]
        lines.append(f"| {rep} | {rt['baseline_total_ms']:.0f} | {rt['hinted_total_ms']:.0f} | {rt['improvement_ms']:.0f} | {rt['improvement_pct']:.1f}% | {rt['overhead_total_ms']:.0f} | {rt['net_improvement_ms']:.0f} |")
    total_base = sum(agg["rep_totals"][r]["baseline_total_ms"] for r in REPS)
    total_hint = sum(agg["rep_totals"][r]["hinted_total_ms"] for r in REPS)
    total_overhead = sum(agg["rep_totals"][r]["overhead_total_ms"] for r in REPS)
    total_net = sum(agg["rep_totals"][r]["net_improvement_ms"] for r in REPS)
    lines.append(f"| **Total** | {total_base:.0f} | {total_hint:.0f} | {total_base-total_hint:.0f} | {(total_base-total_hint)/total_base*100:.1f}% | {total_overhead:.0f} | {total_net:.0f} |")
    lines.append("")

    lines.append("### Q-Error Reduction")
    qr = agg["qe_reduction"]
    lines.append(f"- Median reduction: {qr['median']:.1f}")
    lines.append(f"- P95 reduction: {qr['p95']:.1f}")
    lines.append(f"- Max reduction: {qr['max']:.1f}")
    lines.append(f"- Max increase (regression): {qr['min']:.1f}")
    lines.append("")

    lines.append("### Hint Injection Overhead")
    oh = agg["overhead"]
    lines.append(f"- Median: {oh['median']:.1f}ms, P95: {oh['p95']:.1f}ms, Max: {oh['max']:.1f}ms")
    lines.append(f"- Total overhead: {oh['total']:.0f}ms ({oh['total']/total_hint*100:.2f}% of hinted wall-clock)")
    lines.append("")

    lines.append(f"### Regressions: {agg['regressions']}")
    if agg["regressions"] > 0:
        reg_queries = sorted([q for q in per_query if q["is_regression"]],
                             key=lambda x: x["latency_delta_pct"], reverse=True)
        for q in reg_queries[:5]:
            lines.append(f"- rep {q['rep']} Q{q['query_seq']}: +{q['latency_delta_ms']:.1f}ms (+{q['latency_delta_pct']:.1f}%), hints={q['hints_injected']}")
    lines.append("")

    # Per-query detail table
    lines.append("## Per-Query Detail")
    lines.append("")
    lines.append("| Rep | Seq | Baseline (ms) | Hinted (ms) | Delta (ms) | Delta % | Topo? | Hints | Useful | QE Base | QE Hint |")
    lines.append("|-----|-----|--------------|-------------|------------|---------|-------|-------|--------|---------|---------|")
    for q in sorted(per_query, key=lambda x: (x["rep"], x["query_seq"])):
        topo = "Y" if q["plan_topology_changed"] else ""
        lines.append(f"| {q['rep']} | {q['query_seq']} | {q['latency_baseline_ms']:.1f} | {q['latency_hinted_ms']:.1f} | {q['latency_delta_ms']:.1f} | {q['latency_delta_pct']:.1f}% | {topo} | {q['hints_injected']} | {q['hints_useful']} | {q['q_error_max_baseline']:.0f} | {q['q_error_max_hinted']:.0f} |")
    lines.append("")

    # Case studies
    lines.append("## Case Studies")
    lines.append("")

    # 1. Largest absolute improvement
    best_abs = min(per_query, key=lambda q: q["latency_delta_ms"])
    # 2. Largest percentage improvement
    best_pct = min(per_query, key=lambda q: q["latency_delta_pct"])
    # 3. Most dramatic topology change
    changed_queries = [q for q in per_query if q["plan_topology_changed"]]
    most_dramatic = max(changed_queries, key=lambda q: q["hints_injected"]) if changed_queries else None
    # 4. Hints injected but plan didn't change
    no_change_with_hints = [q for q in per_query if not q["plan_topology_changed"] and q["hints_injected"] > 0]
    robust_plan = max(no_change_with_hints, key=lambda q: q["hints_injected"]) if no_change_with_hints else None
    # 5. Worst regression (or smallest improvement)
    worst = max(per_query, key=lambda q: q["latency_delta_ms"])

    cases = [
        ("Largest absolute improvement", best_abs),
        ("Largest percentage improvement", best_pct),
        ("Most dramatic topology change", most_dramatic),
        ("Hints injected, plan unchanged (robust optimizer)", robust_plan),
        ("Worst regression / smallest improvement", worst),
    ]

    for title, q in cases:
        lines.append(f"### Case Study: {title}")
        lines.append("")
        if q is None:
            lines.append("*(No qualifying query found)*")
            lines.append("")
            continue
        lines.append(f"**Rep {q['rep']}, Query {q['query_seq']}**")
        lines.append("")
        lines.append(f"- Baseline: {q['latency_baseline_ms']:.1f}ms")
        lines.append(f"- Hinted: {q['latency_hinted_ms']:.1f}ms")
        lines.append(f"- Delta: {q['latency_delta_ms']:.1f}ms ({q['latency_delta_pct']:.1f}%)")
        lines.append(f"- Hints injected: {q['hints_injected']}, useful: {q['hints_useful']}")
        lines.append(f"- Plan topology changed: {q['plan_topology_changed']}")
        lines.append(f"- Q-error baseline: {q['q_error_max_baseline']:.0f}, hinted: {q['q_error_max_hinted']:.0f}")
        lines.append(f"- Baseline run times: {[round(t,1) for t in q.get('baseline_run_times',[])]}")
        lines.append(f"- Hinted run times: {[round(t,1) for t in q.get('hinted_run_times',[])]}")
        lines.append("")
        lines.append("**SQL (first 500 chars):**")
        lines.append(f"```sql\n{q['sql'][:500]}\n```")
        lines.append("")

    # Assessment
    lines.append("## Assessment")
    lines.append("")

    total_impr = total_base - total_hint
    total_impr_pct = total_impr / total_base * 100 if total_base else 0
    net_pct = total_net / total_base * 100 if total_base else 0

    lines.append(
        f"Across {agg['total_queries']} queries in task 2's three reps, "
        f"the hinted condition produced plan topology changes on "
        f"{agg['topology_changes']} queries ({agg['topology_changes']/agg['total_queries']*100:.1f}%). "
        f"The median latency delta across all queries was {agg['latency_delta_all']['median']:.1f}ms "
        f"({agg['latency_delta_all_pct']['median']:.1f}%). "
    )
    if changed_queries:
        lines.append(
            f"On the {len(changed_queries)} queries whose plans changed, "
            f"the median delta was {agg['latency_delta_changed']['median']:.1f}ms. "
        )
    lines.append(
        f"Total session wall-clock improvement before overhead: {total_impr:.0f}ms ({total_impr_pct:.1f}%). "
        f"Hint construction overhead totaled {total_overhead:.0f}ms, "
        f"yielding a net improvement of {total_net:.0f}ms ({net_pct:.1f}%). "
        f"There were {agg['regressions']} regressions (hinted > baseline by >10%). "
        f"The q-error reduction median was {qr['median']:.1f}, max {qr['max']:.1f}. "
    )
    lines.append("")

    report_path = PILOT_DIR / "pilot_report.md"
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    log(f"Report saved to {report_path}")

    # Also save aggregates
    with open(PILOT_DIR / "aggregate_metrics.json", "w") as f:
        json.dump(agg, f, indent=2)


# ─── Main ───

def main():
    PILOT_DIR.mkdir(parents=True, exist_ok=True)

    log("=" * 60)
    log("HINTED REPLAY PILOT: TASK 2")
    log("=" * 60)

    # Step 1
    validate()

    # Step 3
    baseline_results = run_baseline()

    # Step 4
    hinted_results = run_hinted()

    # Step 5-6
    per_query = analyze(baseline_results, hinted_results)
    agg = compute_aggregates(per_query)

    # Step 7-8
    generate_report(per_query, agg)

    log("\n=== PILOT COMPLETE ===")
    log(f"Results in {PILOT_DIR}")


if __name__ == "__main__":
    main()
