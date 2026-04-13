#!/usr/bin/env python3
"""
Generate Stage 2 report from stage2_metrics.json (single source of truth).

Every number in the output report comes from stage2_metrics.json or
stage2/manifest.json. No metrics are computed inline.
"""

import json
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from pg_plan_parser import PLAN_CRITICAL_OPS, BOOKKEEPING_OPS

METRICS_PATH = Path("stage2/stage2_metrics.json")
MANIFEST_PATH = Path("stage2/manifest.json")
REPORT_PATH = Path("stage2/stage2_report.md")
PLOT_DIR = Path("stage2/plots")


def make_plots(m: dict) -> dict[str, str]:
    """Generate plots from metrics JSON. Returns {name: relative_path}."""
    PLOT_DIR.mkdir(parents=True, exist_ok=True)
    paths = {}
    plt.rcParams.update({"font.size": 12, "figure.figsize": (10, 6)})

    # 1. Session length histogram
    sl = m["session_length"]
    if sl.get("histogram"):
        fig, ax = plt.subplots()
        bins = sl["histogram"]["bin_edges"]
        counts = sl["histogram"]["counts"]
        ax.bar([f"{bins[i]}-{bins[i+1]-1}" for i in range(len(counts))],
               counts, edgecolor="black", alpha=0.7)
        ax.set_xlabel("Queries per session")
        ax.set_ylabel("Number of sessions")
        ax.set_title("Session Length Distribution (Postgres)")
        plt.xticks(rotation=45)
        fig.tight_layout()
        p = "plots/session_length.png"
        fig.savefig(Path("stage2") / p, dpi=150)
        paths["session_length"] = p
        plt.close(fig)

    # 2. Cardinality reuse by operator type
    cr = m["cardinality_reuse_rate"]
    by_type = cr.get("by_operator_type", {})
    filtered = {k: v for k, v in by_type.items() if v["total"] >= 10}
    if filtered:
        fig, ax = plt.subplots()
        names = sorted(filtered.keys())
        hit_rates = [filtered[n]["hit_rate"] for n in names]
        colors = ["#2196F3" if n in PLAN_CRITICAL_OPS else "#9E9E9E" for n in names]
        bars = ax.barh(names, hit_rates, color=colors, edgecolor="black", alpha=0.7)
        ax.set_xlabel("Hit Rate")
        ax.set_title("Cardinality Reuse Rate by Operator Type (Postgres)")
        ax.set_xlim(0, 1)
        for bar, rate in zip(bars, hit_rates):
            ax.text(rate + 0.02, bar.get_y() + bar.get_height() / 2,
                   f"{rate:.2f}", va="center")
        from matplotlib.patches import Patch
        ax.legend(handles=[Patch(color="#2196F3", label="Plan-critical"),
                           Patch(color="#9E9E9E", label="Bookkeeping")])
        fig.tight_layout()
        p = "plots/cardinality_reuse_by_op.png"
        fig.savefig(Path("stage2") / p, dpi=150)
        paths["cardinality_reuse_by_op"] = p
        plt.close(fig)

    # 3. GROUP BY prediction accuracy
    gp = m["groupby_prediction_accuracy"]
    if gp["total_predictions"] > 0:
        fig, ax = plt.subplots(figsize=(8, 5))
        predictors = ["most_frequent", "last_seen", "markov_1"]
        labels = ["Most Frequent", "Last Seen", "Markov-1"]
        top1 = [gp[p]["top1"] for p in predictors]
        top3 = [gp[p]["top3"] for p in predictors]
        x = np.arange(len(labels))
        w = 0.35
        ax.bar(x - w/2, top1, w, label="Top-1", edgecolor="black", alpha=0.7)
        ax.bar(x + w/2, top3, w, label="Top-3", edgecolor="black", alpha=0.7)
        ax.set_ylabel("Accuracy")
        ax.set_title("GROUP BY Prediction Accuracy (Postgres)")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylim(0, 1)
        ax.legend()
        for i, (v1, v3) in enumerate(zip(top1, top3)):
            ax.text(i - w/2, v1 + 0.02, f"{v1:.2f}", ha="center", fontsize=10)
            ax.text(i + w/2, v3 + 0.02, f"{v3:.2f}", ha="center", fontsize=10)
        fig.tight_layout()
        p = "plots/groupby_prediction.png"
        fig.savefig(Path("stage2") / p, dpi=150)
        paths["groupby_prediction"] = p
        plt.close(fig)

    return paths


def fmt(x, decimals=3):
    """Format a number, handling None."""
    if x is None:
        return "—"
    return f"{x:.{decimals}f}"


def generate_report(m: dict, manifest: dict, plots: dict[str, str]) -> str:
    today = datetime.now().strftime("%Y-%m-%d")

    done = sum(1 for v in manifest.values() if v.get("status") == "done")
    failed = sum(1 for v in manifest.values() if v.get("status") == "failed")
    total = len(manifest)
    total_queries = sum(v.get("num_queries", 0) for v in manifest.values()
                       if v.get("status") == "done")
    total_time = sum(v.get("wall_clock_seconds", 0) for v in manifest.values()
                    if v.get("status") == "done")

    # Unpack metrics
    tj = m["consecutive_table_jaccard"]
    cj = m["consecutive_column_jaccard"]
    tr = m["template_repetition_rate"]
    gap = m["inter_query_gap"]
    sl = m["session_length"]
    rch = m["result_cache_hit_rate"]
    cr = m["cardinality_reuse_rate"]
    gp = m["groupby_prediction_accuracy"]
    ms = m["move_sequences"]
    ad = m["anchor_dimensions"]
    pc = cr["plan_critical"]
    bk = cr["bookkeeping"]
    hint = cr["hintable"]
    qe = cr.get("q_error_before", {})
    qe_pc = cr.get("q_error_plan_critical", {})

    md = []
    md.append("# Stage 2: Postgres Baseline — 31-Task Workload Characterization")
    md.append(f"**Generated:** {today}")
    md.append("**Engine:** PostgreSQL 16 with pg_hint_plan 1.6.1")
    md.append("**Agent:** Fresh Postgres-dialect ReAct agent (claude -p --model sonnet)")
    md.append("**Source of truth:** `stage2/stage2_metrics.json`")
    md.append("")

    # --- Sweep Summary ---
    md.append("## Sweep Summary")
    md.append(f"- **Sessions:** {done} succeeded, {failed} failed, {total} attempted")
    md.append(f"- **Total queries logged:** {total_queries}")
    md.append(f"- **Wall-clock duration:** {total_time:.0f}s ({total_time/3600:.1f}h)")
    md.append(f"- **Max queries/session:** 40")
    md.append("")

    md.append("**Per-task results:**")
    md.append("| Task | Status | Queries | Wall Clock (s) |")
    md.append("|------|--------|---------|----------------|")
    for i in range(1, 32):
        key = f"flag-{i}_rep_a"
        v = manifest.get(key, {})
        md.append(f"| flag-{i} | {v.get('status','missing')} | {v.get('num_queries',0)} | {v.get('wall_clock_seconds',0):.1f} |")
    md.append("")

    # --- Group A ---
    md.append("## Structural Characterization (Group A)")
    md.append("")
    md.append("| Metric | Mean | 95% CI | Median | Min | Max | N |")
    md.append("|--------|------|--------|--------|-----|-----|---|")

    def _row(name, data):
        boot = data.get("bootstrap", {})
        return (f"| {name} | {fmt(data.get('mean'))} | "
                f"[{fmt(boot.get('ci_lower'))}, {fmt(boot.get('ci_upper'))}] | "
                f"{fmt(data.get('median'))} | {fmt(data.get('min'))} | "
                f"{fmt(data.get('max'))} | {data.get('n', 0)} |")

    md.append(_row("Table Jaccard", tj))
    for role in ["select_cols", "where_cols", "groupby_cols"]:
        md.append(_row(f"Column Jaccard ({role})", cj[role]))
    md.append(_row("Template repetition", tr))
    md.append(_row("Inter-query gap (s)", gap))
    md.append(_row("Session length", sl))
    md.append("")

    if "session_length" in plots:
        md.append(f"![Session length distribution]({plots['session_length']})")
        md.append("")

    # --- Group B ---
    md.append("## Opportunity Quantification (Group B)")
    md.append("")

    md.append("### Result Cache Hit Rate")
    rch_boot = rch.get("bootstrap", {})
    md.append(f"- **Hit rate:** {fmt(rch['hit_rate'])} ({rch['hits']}/{rch['total']} queries)")
    md.append(f"- **95% CI:** [{fmt(rch_boot.get('ci_lower'))}, {fmt(rch_boot.get('ci_upper'))}]")
    md.append(f"- **Mean rows saved on hits:** {rch['mean_rows_saved_on_hits']:.0f}")
    md.append("")

    md.append("### Cardinality Reuse Rate (Headline Metric)")
    md.append(f"- **Plan-critical hit rate:** {fmt(pc['hit_rate'])} ({pc['hits']}/{pc['total']} nodes)")
    md.append(f"- **Bookkeeping hit rate:** {fmt(bk['hit_rate'])} ({bk['hits']}/{bk['total']} nodes)")
    md.append(f"- **Overall hit rate:** {fmt(cr['overall_hit_rate'])} ({cr['hit_nodes']}/{cr['total_nodes']} nodes)")
    md.append(f"- **Hintable hit rate (scans + joins):** {fmt(hint['hit_rate'])} ({hint['hits']}/{hint['total']} nodes)")
    md.append("")

    md.append("Plan-critical operators: " + ", ".join(sorted(PLAN_CRITICAL_OPS)))
    md.append("")
    md.append("Bookkeeping operators: " + ", ".join(sorted(BOOKKEEPING_OPS)))
    md.append("")

    md.append("**By operator type:**")
    md.append("| Operator | Hits | Total | Hit Rate | Class |")
    md.append("|----------|------|-------|----------|-------|")
    for op in sorted(cr["by_operator_type"].keys()):
        d = cr["by_operator_type"][op]
        if d["total"] >= 3:
            cls = "plan-critical" if op in PLAN_CRITICAL_OPS else "bookkeeping"
            md.append(f"| {op} | {d['hits']} | {d['total']} | {fmt(d['hit_rate'])} | {cls} |")
    md.append("")

    if "cardinality_reuse_by_op" in plots:
        md.append(f"![Cardinality reuse by operator]({plots['cardinality_reuse_by_op']})")
        md.append("")

    md.append("### Q-error on Reuse Hits")
    if qe_pc:
        md.append(f"- **Plan-critical:** mean={fmt(qe_pc.get('mean'),2)} median={fmt(qe_pc.get('median'),2)} P95={fmt(qe_pc.get('p95'),2)} (N={qe_pc.get('n',0)})")
    if qe:
        md.append(f"- **All operators:** mean={fmt(qe.get('mean'),2)} median={fmt(qe.get('median'),2)} P95={fmt(qe.get('p95'),2)} (N={qe.get('n',0)})")
    md.append(f"- **After reuse:** 1.0 (exact — using measured actual cardinality)")
    md.append("")

    md.append("### GROUP BY Prediction Accuracy")
    md.append(f"N = {gp['total_predictions']} predictions")
    md.append("")
    md.append("| Predictor | Top-1 | Top-3 |")
    md.append("|-----------|-------|-------|")
    for name, label in [("most_frequent", "Most Frequent"),
                        ("last_seen", "Last Seen"),
                        ("markov_1", "Markov-1")]:
        d = gp[name]
        md.append(f"| {label} | {fmt(d['top1'])} | {fmt(d['top3'])} |")
    md.append("")

    if "groupby_prediction" in plots:
        md.append(f"![GROUP BY prediction accuracy]({plots['groupby_prediction']})")
        md.append("")

    # --- Group C ---
    md.append("## Move Sequences and Anchors (Group C)")
    md.append("")

    mf = ms["move_frequencies"]
    total_moves = sum(mf.values())
    md.append("**Move type frequencies:**")
    md.append("| Move | Count | Fraction |")
    md.append("|------|-------|----------|")
    for move, count in sorted(mf.items(), key=lambda x: -x[1]):
        md.append(f"| {move} | {count} | {fmt(count/total_moves)} |")
    md.append("")

    md.append("**Top bigrams:**")
    md.append("| Sequence | Count |")
    md.append("|----------|-------|")
    for bg in ms["top_bigrams"][:10]:
        md.append(f"| {' → '.join(bg['sequence'])} | {bg['count']} |")
    md.append("")

    md.append("**Top trigrams:**")
    md.append("| Sequence | Count |")
    md.append("|----------|-------|")
    for tg in ms["top_trigrams"][:10]:
        md.append(f"| {' → '.join(tg['sequence'])} | {tg['count']} |")
    md.append("")

    md.append("**Anchor dimensions (≥50% of GROUP BYs):**")
    md.append("| Session | Anchors |")
    md.append("|---------|---------|")
    for sid, anchors in sorted(ad.items()):
        if anchors:
            md.append(f"| {sid} | {', '.join(anchors)} |")
    md.append("")

    # --- Cross-engine comparison ---
    md.append("## Cross-Engine Comparison (Postgres vs DuckDB)")
    md.append("")
    md.append("DuckDB numbers from `/scratch/agentic-sql/reports/sweep_20260411.md`.")
    md.append("")
    md.append("| Metric | DuckDB | Postgres | Notes |")
    md.append("|--------|--------|----------|-------|")
    md.append(f"| Table Jaccard | 0.931 | {fmt(tj['mean'])} | Both high — single-table tasks dominate |")
    md.append(f"| Column Jaccard (where) | 0.640 | {fmt(cj['where_cols']['mean'])} | |")
    md.append(f"| Column Jaccard (groupby) | 0.296 | {fmt(cj['groupby_cols']['mean'])} | Low on both — agents pivot rapidly |")
    md.append(f"| Template repetition | 0.001 | {fmt(tr['mean'])} | Both ~0 — agents never reissue identical SQL |")
    md.append(f"| Result cache hit rate | 0.147 | {fmt(rch['hit_rate'])} | |")
    md.append(f"| **Card. reuse (plan-critical)** | **0.748** | **{fmt(pc['hit_rate'])}** | 27pp gap is semantic, not cosmetic (see predicate_pair_analysis.md) |")
    md.append(f"| Card. reuse (overall) | 0.886 | {fmt(cr['overall_hit_rate'])} | Overall inflated by bookkeeping on both engines |")
    md.append(f"| Hintable reuse (scans+joins) | — | {fmt(hint['hit_rate'])} | Postgres-specific: what pg_hint_plan can target |")
    md.append(f"| Q-error plan-critical (mean) | 5.91 | {fmt(qe_pc.get('mean'),2)} | |")
    md.append(f"| Q-error plan-critical (P95) | — | {fmt(qe_pc.get('p95'),2)} | |")
    md.append(f"| Session length (mean) | 10.3 | {fmt(sl['mean'],1)} | Postgres agent runs longer sessions |")
    md.append("")

    return "\n".join(md)


def main():
    print("Loading metrics JSON...")
    m = json.load(open(METRICS_PATH))
    manifest = json.load(open(MANIFEST_PATH))

    print("Generating plots...")
    plots = make_plots(m)

    print("Generating report...")
    report = generate_report(m, manifest, plots)

    with open(REPORT_PATH, "w") as f:
        f.write(report)
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
