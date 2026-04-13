#!/usr/bin/env python3
"""
Metrics suite for Postgres agentic SQL workload characterization.

Port of agentic-sql/metrics.py adapted for Postgres plan tree format.
Consumes session JSONL files and computes structural, opportunity,
and prediction metrics. Uses bootstrap for confidence intervals.
"""

import json
import warnings
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import sqlglot

from pg_plan_parser import PLAN_CRITICAL_OPS, BOOKKEEPING_OPS

Session = list[dict]


def _resolve_ordinal_groupby(entry: dict) -> list[str]:
    """Resolve ordinal GROUP BY references to actual column names."""
    gb_cols = entry.get("group_by_cols", [])
    if not gb_cols:
        return []
    has_ordinals = any(g.strip().isdigit() for g in gb_cols)
    if not has_ordinals:
        return gb_cols

    raw_sql = entry.get("raw_sql", "")
    if not raw_sql:
        return gb_cols

    try:
        parsed = sqlglot.parse_one(raw_sql, dialect="postgres")
        select_exprs = parsed.expressions
    except Exception:
        return gb_cols

    resolved = []
    for g in gb_cols:
        g_stripped = g.strip()
        if g_stripped.isdigit():
            idx = int(g_stripped) - 1
            if 0 <= idx < len(select_exprs):
                expr = select_exprs[idx]
                if expr.alias:
                    resolved.append(expr.alias)
                else:
                    resolved.append(expr.sql(dialect="postgres"))
            else:
                resolved.append(g)
        else:
            resolved.append(g)
    return resolved


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def load_sessions(log_dir: str | Path, glob_pattern: str = "*.jsonl") -> list[Session]:
    """Load all sessions from a log directory. Returns list of sessions."""
    log_dir = Path(log_dir)
    sessions = []
    for f in sorted(log_dir.glob(glob_pattern)):
        entries = []
        for line in open(f):
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        if entries:
            sessions.append(entries)
    return sessions


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def _bootstrap_ci(values: list[float], n_boot: int = 1000, ci: float = 0.95) -> dict:
    if not values:
        return {"mean": None, "ci_lower": None, "ci_upper": None, "n": 0}
    arr = np.array(values)
    observed_mean = float(np.mean(arr))
    boot_means = []
    rng = np.random.default_rng(42)
    for _ in range(n_boot):
        sample = rng.choice(arr, size=len(arr), replace=True)
        boot_means.append(float(np.mean(sample)))
    boot_means.sort()
    alpha = (1 - ci) / 2
    lo = boot_means[int(alpha * n_boot)]
    hi = boot_means[int((1 - alpha) * n_boot)]
    return {"mean": observed_mean, "ci_lower": lo, "ci_upper": hi, "n": len(values)}


def _percentiles(values: list[float]) -> dict:
    if not values:
        return {}
    arr = np.array(values)
    return {
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "p05": float(np.percentile(arr, 5)),
        "p25": float(np.percentile(arr, 25)),
        "p75": float(np.percentile(arr, 75)),
        "p95": float(np.percentile(arr, 95)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "n": len(values),
    }


# ---------------------------------------------------------------------------
# Group A — Structural characterization
# ---------------------------------------------------------------------------

def consecutive_table_jaccard(sessions: list[Session]) -> dict:
    values = []
    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        for i in range(1, len(successful)):
            a = set(successful[i - 1].get("tables", []))
            b = set(successful[i].get("tables", []))
            values.append(_jaccard(a, b))
    result = _percentiles(values)
    result["bootstrap"] = _bootstrap_ci(values)
    result["n_pairs"] = len(values)
    return result


def consecutive_column_jaccard(sessions: list[Session]) -> dict:
    def _where_cols(entry: dict) -> set:
        return set(p["column"] for p in entry.get("predicates", []))

    def _groupby_cols(entry: dict) -> set:
        return set(_resolve_ordinal_groupby(entry))

    def _select_cols(entry: dict) -> set:
        return set(entry.get("columns", []))

    results = {}
    for role_name, extractor in [("select_cols", _select_cols),
                                  ("where_cols", _where_cols),
                                  ("groupby_cols", _groupby_cols)]:
        values = []
        for session in sessions:
            successful = [e for e in session if e.get("success", True)]
            for i in range(1, len(successful)):
                a = extractor(successful[i - 1])
                b = extractor(successful[i])
                values.append(_jaccard(a, b))
        results[role_name] = _percentiles(values)
        results[role_name]["bootstrap"] = _bootstrap_ci(values)
    return results


def template_repetition_rate(sessions: list[Session]) -> dict:
    per_session = []
    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        if len(successful) < 2:
            continue
        seen = set()
        repeats = 0
        for e in successful:
            t = e.get("template", "")
            if t in seen:
                repeats += 1
            seen.add(t)
        per_session.append(repeats / len(successful))
    result = _percentiles(per_session)
    result["bootstrap"] = _bootstrap_ci(per_session)
    return result


def inter_query_gap_distribution(sessions: list[Session]) -> dict:
    gaps = []
    for session in sessions:
        for e in session:
            start = e.get("query_start_ts")
            prev_end = e.get("prev_query_end_ts")
            if start is not None and prev_end is not None:
                gap = start - prev_end
                if gap >= 0:
                    gaps.append(gap)
    result = _percentiles(gaps)
    result["bootstrap"] = _bootstrap_ci(gaps)
    return result


def session_length_distribution(sessions: list[Session]) -> dict:
    lengths = []
    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        lengths.append(len(successful))
    result = _percentiles([float(l) for l in lengths])
    if lengths:
        counts, bin_edges = np.histogram(lengths, bins=range(0, max(lengths) + 5, 5))
        result["histogram"] = {
            "bin_edges": [int(b) for b in bin_edges],
            "counts": [int(c) for c in counts],
        }
    result["bootstrap"] = _bootstrap_ci([float(l) for l in lengths])
    return result


# ---------------------------------------------------------------------------
# Group B — Opportunity quantification
# ---------------------------------------------------------------------------

def result_cache_hit_rate(sessions: list[Session]) -> dict:
    hits = 0
    total = 0
    rows_saved_on_hits = []

    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        for i in range(1, len(successful)):
            qi = successful[i]
            qi_tables = set(qi.get("tables", []))
            qi_preds = set((p["column"], p["operator"], p["value"])
                          for p in qi.get("predicates", []))
            qi_cols = set(qi.get("columns", []))
            qi_gb = set(qi.get("group_by_cols", []))
            total += 1

            for j in range(i):
                qj = successful[j]
                qj_tables = set(qj.get("tables", []))
                qj_preds = set((p["column"], p["operator"], p["value"])
                              for p in qj.get("predicates", []))
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
                rows_saved_on_hits.append(max(0, saved))
                break

    return {
        "hit_rate": hits / total if total > 0 else 0.0,
        "hits": hits,
        "total": total,
        "mean_rows_saved_on_hits": (
            float(np.mean(rows_saved_on_hits)) if rows_saved_on_hits else 0.0
        ),
        "bootstrap": _bootstrap_ci(
            [1.0] * hits + [0.0] * (total - hits) if total > 0 else []
        ),
    }


def cardinality_reuse_rate(sessions: list[Session]) -> dict:
    """
    Metric 7 (HEADLINE): For each plan node, check if a prior query had a node
    with the same operator_signature. Uses Postgres operator names.
    """
    total_nodes = 0
    hit_nodes = 0
    hits_by_type: dict[str, int] = Counter()
    totals_by_type: dict[str, int] = Counter()
    q_errors_before: list[float] = []
    q_errors_plan_critical: list[float] = []

    # Hintability tracking (Postgres-specific)
    hintable_hits = 0
    hintable_total = 0

    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        seen_sigs: dict[str, int] = {}

        for i, entry in enumerate(successful):
            plan_tree = entry.get("plan_tree", [])
            for node in plan_tree:
                op_type = node.get("operator_type", "")
                sig = node.get("operator_signature", "")
                actual = node.get("actual_card")
                ec = node.get("estimated_card")

                if not sig or not op_type:
                    continue

                total_nodes += 1
                totals_by_type[op_type] += 1

                # Check hintability (scan + join nodes)
                is_hint = op_type in (
                    "Seq Scan", "Index Scan", "Index Only Scan",
                    "Bitmap Heap Scan", "Hash Join", "Merge Join", "Nested Loop"
                )
                if is_hint:
                    hintable_total += 1

                if sig in seen_sigs:
                    hit_nodes += 1
                    hits_by_type[op_type] += 1
                    if is_hint:
                        hintable_hits += 1

                    if ec is not None and actual is not None and ec > 0 and actual > 0:
                        q_error = max(ec, actual) / min(ec, actual)
                        q_errors_before.append(q_error)
                        if op_type in PLAN_CRITICAL_OPS:
                            q_errors_plan_critical.append(q_error)
                else:
                    seen_sigs[sig] = actual if actual is not None else 0

    by_type = {}
    for op_type in sorted(totals_by_type.keys()):
        by_type[op_type] = {
            "hits": hits_by_type.get(op_type, 0),
            "total": totals_by_type[op_type],
            "hit_rate": hits_by_type.get(op_type, 0) / totals_by_type[op_type]
            if totals_by_type[op_type] > 0 else 0.0,
        }

    def _split_rate(op_set):
        h = sum(hits_by_type.get(op, 0) for op in op_set)
        t = sum(totals_by_type.get(op, 0) for op in op_set)
        return {"hits": h, "total": t, "hit_rate": h / t if t > 0 else 0.0}

    return {
        "overall_hit_rate": hit_nodes / total_nodes if total_nodes > 0 else 0.0,
        "hit_nodes": hit_nodes,
        "total_nodes": total_nodes,
        "plan_critical": _split_rate(PLAN_CRITICAL_OPS),
        "bookkeeping": _split_rate(BOOKKEEPING_OPS),
        "hintable": {
            "hits": hintable_hits,
            "total": hintable_total,
            "hit_rate": hintable_hits / hintable_total if hintable_total > 0 else 0.0,
        },
        "by_operator_type": by_type,
        "q_error_before": _percentiles(q_errors_before) if q_errors_before else {},
        "q_error_plan_critical": _percentiles(q_errors_plan_critical) if q_errors_plan_critical else {},
        "q_error_reduction_note": "q-error after reuse would be 1.0 (perfect)",
    }


def groupby_prediction_accuracy(sessions: list[Session]) -> dict:
    most_freq_top1 = 0
    most_freq_top3 = 0
    last_seen_top1 = 0
    last_seen_top3 = 0
    markov_top1 = 0
    markov_top3 = 0
    total = 0

    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        gb_history: list[tuple] = []
        transitions: dict[tuple, Counter] = defaultdict(Counter)

        for i, entry in enumerate(successful):
            gb = tuple(sorted(_resolve_ordinal_groupby(entry)))
            if not gb:
                gb_history.append(gb)
                continue
            if not gb_history:
                gb_history.append(gb)
                continue

            total += 1

            freq = Counter(gb_history)
            most_common = freq.most_common(3)
            if most_common[0][0] == gb:
                most_freq_top1 += 1
            if gb in [mc[0] for mc in most_common]:
                most_freq_top3 += 1

            last = gb_history[-1]
            if last == gb:
                last_seen_top1 += 1
                last_seen_top3 += 1
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
                    last_seen_top3 += 1

            prev_gb = gb_history[-1]
            if transitions[prev_gb]:
                markov_preds = transitions[prev_gb].most_common(3)
                if markov_preds[0][0] == gb:
                    markov_top1 += 1
                if gb in [mp[0] for mp in markov_preds]:
                    markov_top3 += 1

            if gb_history:
                transitions[gb_history[-1]][gb] += 1
            gb_history.append(gb)

    return {
        "total_predictions": total,
        "most_frequent": {
            "top1": most_freq_top1 / total if total else 0.0,
            "top3": most_freq_top3 / total if total else 0.0,
        },
        "last_seen": {
            "top1": last_seen_top1 / total if total else 0.0,
            "top3": last_seen_top3 / total if total else 0.0,
        },
        "markov_1": {
            "top1": markov_top1 / total if total else 0.0,
            "top3": markov_top3 / total if total else 0.0,
        },
    }


# ---------------------------------------------------------------------------
# Group C — Prediction inputs (feature extraction)
# ---------------------------------------------------------------------------

def extract_move_sequences(sessions: list[Session]) -> dict:
    all_sequences = []
    bigram_counts: Counter = Counter()
    trigram_counts: Counter = Counter()

    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        moves = []
        for i, entry in enumerate(successful):
            if i == 0:
                if not entry.get("predicates"):
                    moves.append("overview")
                else:
                    moves.append("other")
                continue

            prev = successful[i - 1]
            cur = entry

            cur_tables = set(cur.get("tables", []))
            prev_tables = set(prev.get("tables", []))
            cur_gb = set(cur.get("group_by_cols", []))
            prev_gb = set(prev.get("group_by_cols", []))
            cur_preds = set((p["column"], p["operator"], p["value"])
                           for p in cur.get("predicates", []))
            prev_preds = set((p["column"], p["operator"], p["value"])
                            for p in prev.get("predicates", []))

            if cur_tables != prev_tables:
                moves.append("cross_table")
            elif cur_gb > prev_gb and prev_gb and cur_tables == prev_tables:
                moves.append("drill_down")
            elif (cur_preds > prev_preds and cur_tables == prev_tables
                  and cur_gb == prev_gb):
                moves.append("deepen")
            elif (cur_preds - prev_preds and not prev_preds - cur_preds
                  and cur_tables == prev_tables and cur_gb == prev_gb
                  and cur_preds != prev_preds):
                moves.append("narrow")
            elif (prev_preds > cur_preds and cur_tables == prev_tables):
                moves.append("widen")
            elif (cur_gb != prev_gb and cur_tables == prev_tables):
                moves.append("pivot")
            elif (cur_tables == prev_tables and cur_preds == prev_preds
                  and cur_gb == prev_gb):
                moves.append("reframe")
            else:
                moves.append("other")

        all_sequences.append(moves)

        for j in range(1, len(moves)):
            bigram_counts[(moves[j - 1], moves[j])] += 1
        for j in range(2, len(moves)):
            trigram_counts[(moves[j - 2], moves[j - 1], moves[j])] += 1

    move_freq = Counter()
    for seq in all_sequences:
        move_freq.update(seq)

    return {
        "sequences": all_sequences,
        "move_frequencies": dict(move_freq.most_common()),
        "top_bigrams": [
            {"sequence": list(k), "count": v}
            for k, v in bigram_counts.most_common(10)
        ],
        "top_trigrams": [
            {"sequence": list(k), "count": v}
            for k, v in trigram_counts.most_common(10)
        ],
    }


def predicate_persistence_profiles(sessions: list[Session]) -> dict:
    result = {}
    for session in sessions:
        sid = session[0].get("session_id", "unknown") if session else "unknown"
        col_positions: dict[str, list[int]] = defaultdict(list)
        for entry in session:
            seq = entry.get("query_seq", 0)
            for pred in entry.get("predicates", []):
                col_positions[pred["column"]].append(seq)
        result[sid] = dict(col_positions)
    return result


def anchor_dimensions(sessions: list[Session], threshold: float = 0.5) -> dict:
    result = {}
    for session in sessions:
        sid = session[0].get("session_id", "unknown") if session else "unknown"
        successful = [e for e in session if e.get("success", True)]
        gb_queries = [e for e in successful if e.get("group_by_cols")]
        if not gb_queries:
            result[sid] = []
            continue

        col_counts: Counter = Counter()
        for e in gb_queries:
            for col in _resolve_ordinal_groupby(e):
                col_counts[col] += 1

        n_gb = len(gb_queries)
        anchors = [col for col, count in col_counts.items()
                    if count / n_gb >= threshold]
        result[sid] = sorted(anchors)
    return result


# ---------------------------------------------------------------------------
# Run all metrics
# ---------------------------------------------------------------------------

def compute_all(sessions: list[Session]) -> dict:
    return {
        "consecutive_table_jaccard": consecutive_table_jaccard(sessions),
        "consecutive_column_jaccard": consecutive_column_jaccard(sessions),
        "template_repetition_rate": template_repetition_rate(sessions),
        "inter_query_gap": inter_query_gap_distribution(sessions),
        "session_length": session_length_distribution(sessions),
        "result_cache_hit_rate": result_cache_hit_rate(sessions),
        "cardinality_reuse_rate": cardinality_reuse_rate(sessions),
        "groupby_prediction_accuracy": groupby_prediction_accuracy(sessions),
        "move_sequences": extract_move_sequences(sessions),
        "predicate_persistence": predicate_persistence_profiles(sessions),
        "anchor_dimensions": anchor_dimensions(sessions),
    }


if __name__ == "__main__":
    import sys
    log_dir = sys.argv[1] if len(sys.argv) > 1 else "stage2/traces"
    sessions = load_sessions(log_dir)
    print(f"Loaded {len(sessions)} sessions")
    results = compute_all(sessions)

    print(f"\n=== Structural (Group A) ===")
    tj = results["consecutive_table_jaccard"]
    print(f"Table Jaccard: mean={tj['mean']:.3f} [{tj['bootstrap']['ci_lower']:.3f}, {tj['bootstrap']['ci_upper']:.3f}]")

    cj = results["consecutive_column_jaccard"]
    for role in ["select_cols", "where_cols", "groupby_cols"]:
        r = cj[role]
        print(f"Column Jaccard ({role}): mean={r['mean']:.3f} [{r['bootstrap']['ci_lower']:.3f}, {r['bootstrap']['ci_upper']:.3f}]")

    tr = results["template_repetition_rate"]
    print(f"Template repetition: mean={tr.get('mean', 0):.3f}")

    gap = results["inter_query_gap"]
    print(f"Inter-query gap: median={gap.get('median', 0):.2f}s min={gap.get('min', 0):.2f}s")

    sl = results["session_length"]
    print(f"Session length: mean={sl.get('mean', 0):.1f} min={sl.get('min', 0):.0f} max={sl.get('max', 0):.0f}")

    print(f"\n=== Opportunity (Group B) ===")
    rch = results["result_cache_hit_rate"]
    print(f"Result cache hit rate: {rch['hit_rate']:.3f} ({rch['hits']}/{rch['total']})")

    cr = results["cardinality_reuse_rate"]
    print(f"Cardinality reuse (overall): {cr['overall_hit_rate']:.3f} ({cr['hit_nodes']}/{cr['total_nodes']})")
    pc = cr["plan_critical"]
    print(f"Cardinality reuse (plan-critical): {pc['hit_rate']:.3f} ({pc['hits']}/{pc['total']})")
    hint = cr["hintable"]
    print(f"Cardinality reuse (hintable): {hint['hit_rate']:.3f} ({hint['hits']}/{hint['total']})")
    for op, data in sorted(cr["by_operator_type"].items()):
        if data["total"] >= 5:
            print(f"  {op}: {data['hit_rate']:.3f} ({data['hits']}/{data['total']})")

    if cr.get("q_error_before"):
        qe = cr["q_error_before"]
        print(f"Q-error before reuse: mean={qe.get('mean', 0):.2f} median={qe.get('median', 0):.2f} p95={qe.get('p95', 0):.2f}")
    if cr.get("q_error_plan_critical"):
        qe = cr["q_error_plan_critical"]
        print(f"Q-error plan-critical: mean={qe.get('mean', 0):.2f} median={qe.get('median', 0):.2f} p95={qe.get('p95', 0):.2f}")

    gp = results["groupby_prediction_accuracy"]
    print(f"\nGROUP BY prediction (n={gp['total_predictions']}):")
    for name in ["most_frequent", "last_seen", "markov_1"]:
        d = gp[name]
        print(f"  {name}: top-1={d['top1']:.3f} top-3={d['top3']:.3f}")

    print(f"\n=== Moves (Group C) ===")
    ms = results["move_sequences"]
    print(f"Move frequencies: {ms['move_frequencies']}")
    print(f"Top bigrams: {ms['top_bigrams'][:5]}")

    # Save full results JSON
    out_path = Path(log_dir).parent / "stage2_metrics.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nFull results saved to {out_path}")
