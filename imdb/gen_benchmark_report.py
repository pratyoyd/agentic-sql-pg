#!/usr/bin/env python3
"""Generate reports/baseline_benchmark.md from benchmark results + traces."""

import json
import re
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))
from scenario_common import SCENARIOS

SESSION_DIR = Path("sessions")
BENCH_PATH = Path("reports/baseline_benchmark.json")
REPORT_PATH = Path("reports/baseline_benchmark.md")


def load_trace(sid, rep):
    if sid == 1:
        p = SESSION_DIR / f"scenario1_rep_{rep}.jsonl"
    else:
        p = SESSION_DIR / f"scenario{sid}_rep_{rep}.jsonl"
    if not p.exists():
        return []
    return [json.loads(l) for l in open(p)]


def load_workspace(sid, rep):
    if sid == 1:
        p = SESSION_DIR / f"scenario1_rep_{rep}_workspace.json"
    else:
        p = SESSION_DIR / f"scenario{sid}_rep_{rep}_workspace.json"
    if not p.exists():
        return None
    return json.load(open(p))


def get_saved_tables(ws):
    saved = set()
    if ws and "activity" in ws:
        for evt in ws["activity"]:
            if evt.get("call_type") == "save" and evt.get("payload"):
                hint = evt["payload"].get("usage_hint", "")
                m = re.search(r'FROM\s+(\S+)', hint)
                if m:
                    saved.add(m.group(1).lower())
    return saved


def sql_preview(sql, maxlen=55):
    s = sql.replace("\n", " ").replace("|", "/").strip()
    s = re.sub(r'\s+', ' ', s)
    if len(s) > maxlen:
        s = s[:maxlen] + "..."
    return s


def main():
    bench = json.load(open(BENCH_PATH))

    # Build lookup
    bench_lookup = {}
    for b in bench:
        key = (b.get("scenario_id"), b.get("rep"), b.get("query_seq"))
        bench_lookup[key] = b

    all_scenarios = [(1, ["a", "b", "c"])] + [(sid, SCENARIOS[sid]["reps"]) for sid in range(2, 11)]

    lines = []
    lines.append("# Baseline Benchmark Report: M1 Latency Savings")
    lines.append("")
    lines.append("Measured by running each reuse query two ways:")
    lines.append("- **Reuse**: query runs against a materialized temp table (what M1 provides)")
    lines.append("- **Baseline**: temp table replaced by a CTE wrapping the original base SQL (raw tables only)")
    lines.append("")
    lines.append("Non-reuse queries (no temp table involved) show their original execution time as both reuse and baseline.")
    lines.append("")

    for sid, reps in all_scenarios:
        title = "Genre Evolution" if sid == 1 else SCENARIOS[sid]["title"]
        lines.append(f"## Scenario {sid}: {title}")
        lines.append("")

        for rep in reps:
            trace = load_trace(sid, rep)
            ws = load_workspace(sid, rep)
            if not trace:
                continue

            saved_tables = get_saved_tables(ws)

            lines.append(f"### Rep {rep}")
            lines.append("")
            lines.append("| Query | Type | Reuse (ms) | Baseline (ms) | Speedup | Tables | SQL preview |")
            lines.append("|-------|------|-----------|--------------|---------|--------|-------------|")

            rep_reuse_total = 0.0
            rep_baseline_total = 0.0

            for i, t in enumerate(trace):
                sql = t.get("raw_sql", "")
                exec_ms = t.get("execution_ms", 0)
                is_save = t.get("workspace_save", False)
                is_reuse = (any(st in sql.lower() for st in saved_tables) and not is_save) if saved_tables else False
                ntables = len(t.get("tables", []))
                preview = sql_preview(sql)

                bench_entry = bench_lookup.get((sid, rep, i))

                if is_save:
                    qtype = "SAVE"
                    reuse_ms = exec_ms
                    baseline_ms = exec_ms
                    speedup_str = "\u2014"
                    rep_reuse_total += reuse_ms
                    rep_baseline_total += baseline_ms
                elif is_reuse and bench_entry and bench_entry.get("speedup") is not None:
                    qtype = "REUSE"
                    reuse_ms = bench_entry["reuse_ms"]
                    baseline_ms = bench_entry["baseline_ms"]
                    speedup_str = f"{bench_entry['speedup']:.1f}x"
                    rep_reuse_total += reuse_ms
                    rep_baseline_total += baseline_ms
                elif is_reuse and bench_entry and bench_entry.get("baseline_error"):
                    qtype = "REUSE"
                    reuse_ms = bench_entry.get("reuse_ms", exec_ms)
                    err = bench_entry.get("baseline_error", "")
                    rep_reuse_total += reuse_ms
                    if "timeout" in err:
                        lines.append(f"| q{i} | {qtype} | {reuse_ms:.0f} | >1200000 | >timeout | {ntables} | `{preview}` |")
                    else:
                        lines.append(f"| q{i} | {qtype} | {reuse_ms:.0f} | ERR | ERR | {ntables} | `{preview}` |")
                    continue
                else:
                    qtype = "OTHER"
                    reuse_ms = exec_ms
                    baseline_ms = exec_ms
                    speedup_str = "1.0x"
                    rep_reuse_total += reuse_ms
                    rep_baseline_total += baseline_ms

                lines.append(
                    f"| q{i} | {qtype} | {reuse_ms:.0f} | {baseline_ms:.0f} | "
                    f"{speedup_str} | {ntables} | `{preview}` |"
                )

            speedup_total = rep_baseline_total / max(1, rep_reuse_total)
            lines.append(
                f"| **Total** | | **{rep_reuse_total:.0f}** | **{rep_baseline_total:.0f}** | "
                f"**{speedup_total:.1f}x** | | |"
            )
            lines.append("")

        lines.append("")

    # Grand summary
    lines.append("## Grand Summary")
    lines.append("")

    successful = [r for r in bench if r.get("speedup") is not None]
    failed = [r for r in bench if r.get("speedup") is None]
    total_reuse = sum(r["reuse_ms"] for r in successful)
    total_baseline = sum(r["baseline_ms"] for r in successful)
    total_savings = sum(r["savings_ms"] for r in successful)

    lines.append(f"- **Reuse queries benchmarked**: {len(bench)} ({len(successful)} OK, {len(failed)} failed)")
    lines.append(f"- **Total reuse time**: {total_reuse/1000:.1f}s")
    lines.append(f"- **Total baseline time**: {total_baseline/1000:.1f}s")
    lines.append(f"- **Total savings**: {total_savings/1000:.1f}s")
    lines.append(f"- **Overall speedup**: {total_baseline/max(1,total_reuse):.1f}x")
    lines.append(f"- **Reduction**: {total_savings/max(1,total_baseline)*100:.1f}%")
    lines.append("")

    lines.append("### Per-Scenario Summary")
    lines.append("")
    lines.append("| Scenario | Reuse (s) | Baseline (s) | Savings (s) | Speedup | N |")
    lines.append("|----------|-----------|-------------|-------------|---------|---|")

    for sid in sorted(set(r["scenario_id"] for r in successful)):
        sc_r = [r for r in successful if r["scenario_id"] == sid]
        sc_reuse = sum(r["reuse_ms"] for r in sc_r)
        sc_base = sum(r["baseline_ms"] for r in sc_r)
        sc_save = sum(r["savings_ms"] for r in sc_r)
        title = "Genre Evolution" if sid == 1 else SCENARIOS[sid]["title"]
        lines.append(
            f"| {sid}. {title} | {sc_reuse/1000:.1f} | {sc_base/1000:.1f} | "
            f"{sc_save/1000:.1f} | {sc_base/max(1,sc_reuse):.1f}x | {len(sc_r)} |"
        )

    lines.append("")

    # Full session cost comparison (fair: includes OTHER + SAVE overhead)
    lines.append("### Full Session Cost Comparison")
    lines.append("")
    lines.append("Includes all query types for a fair apples-to-apples comparison:")
    lines.append("- **With M1** = OTHER + SAVE + REUSE (temp table)")
    lines.append("- **Without M1** = OTHER + BASELINE (no save needed, CTE from raw tables)")
    lines.append("")
    lines.append("| Scenario | OTHER (s) | SAVE (s) | REUSE (s) | With M1 (s) | Without M1 (s) | Net savings (s) | Speedup |")
    lines.append("|----------|-----------|----------|-----------|-------------|----------------|-----------------|---------|")

    all_scenarios_list = [(1, ["a", "b", "c"])] + [(sid, SCENARIOS[sid]["reps"]) for sid in range(2, 11)]
    grand_with = 0.0
    grand_without = 0.0

    for sid, reps in all_scenarios_list:
        title = "Genre Evolution" if sid == 1 else SCENARIOS[sid]["title"]

        other_ms = 0.0
        save_ms = 0.0
        reuse_ms = 0.0
        baseline_ms = 0.0

        for rep in reps:
            trace = load_trace(sid, rep)
            ws = load_workspace(sid, rep)
            if not trace:
                continue

            saved_tables = get_saved_tables(ws)

            for i, t in enumerate(trace):
                sql = t.get("raw_sql", "")
                exec_ms = t.get("execution_ms", 0)
                is_save = t.get("workspace_save", False)
                is_reuse = (any(st in sql.lower() for st in saved_tables) and not is_save) if saved_tables else False

                if is_save:
                    save_ms += exec_ms
                elif is_reuse:
                    bench_entry = bench_lookup.get((sid, rep, i))
                    if bench_entry and bench_entry.get("speedup") is not None:
                        reuse_ms += bench_entry["reuse_ms"]
                        baseline_ms += bench_entry["baseline_ms"]
                    else:
                        # Timeout or excluded: use original exec_ms as reuse,
                        # baseline unknown — use reuse as lower bound
                        reuse_ms += exec_ms
                        baseline_ms += exec_ms  # conservative: no savings assumed
                else:
                    other_ms += exec_ms

        with_m1 = other_ms + save_ms + reuse_ms
        without_m1 = other_ms + baseline_ms
        net_savings = without_m1 - with_m1
        speedup = without_m1 / max(1, with_m1)
        grand_with += with_m1
        grand_without += without_m1

        lines.append(
            f"| {sid}. {title} | {other_ms/1000:.1f} | {save_ms/1000:.1f} | "
            f"{reuse_ms/1000:.1f} | {with_m1/1000:.1f} | {without_m1/1000:.1f} | "
            f"{net_savings/1000:.1f} | {speedup:.1f}x |"
        )

    grand_savings = grand_without - grand_with
    lines.append(
        f"| **TOTAL** | | | | **{grand_with/1000:.1f}** | **{grand_without/1000:.1f}** | "
        f"**{grand_savings/1000:.1f}** | **{grand_without/max(1,grand_with):.1f}x** |"
    )
    lines.append("")

    lines.append("### Failed Queries")
    lines.append("")
    for r in failed:
        sid = r.get("scenario_id", "?")
        rep = r.get("rep", "?")
        seq = r.get("query_seq", "?")
        err = r.get("baseline_error") or r.get("reuse_error") or "?"
        err_clean = err.split("\n")[0][:120]
        lines.append(f"- **Sc{sid} rep {rep} q{seq}**: {err_clean}")

    lines.append("")
    lines.append("### Notes")
    lines.append("")
    lines.append("- Sc2 rep a q7 timed out at 20 min \u2014 REGR_SLOPE over inlined 9s CTE is genuinely >1200s without materialization")
    lines.append("- 3 queries failed due to CTE translation artifacts (column visibility / nested aggregate issues when temp table becomes CTE)")
    lines.append("- Sc8 shows extreme speedups (>10000x) because the base is a 23s 36M-row cast_info join and reuse queries are sub-millisecond lookups")
    lines.append("- \"OTHER\" queries run the same with or without M1 \u2014 included for total session time context")

    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))
    print(f"Report written to {REPORT_PATH} ({len(lines)} lines)")


if __name__ == "__main__":
    main()
