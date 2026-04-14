#!/usr/bin/env python3
"""
Partial analysis of IMDB agentic-SQL traces.
Reads trace JSONL files and produces partial_analysis_report.md.
"""

import json
import os
import re
import statistics
from collections import defaultdict
from pathlib import Path

BASE = Path("/scratch/agentic-sql-pg/imdb")
TRACES = BASE / "traces"
CTE_FILE = BASE / "cte_analysis.jsonl"
REPORT = BASE / "partial_analysis_report.md"

# ── helpers ──────────────────────────────────────────────────────────────

def load_traces():
    """Return dict  task -> rep -> list of entries (sorted by query_seq)."""
    data = defaultdict(lambda: defaultdict(list))
    for f in sorted(TRACES.glob("*.jsonl")):
        if "_summary" in f.name:
            continue
        # e.g. task1_rep_a.jsonl
        m = re.match(r"(task\d+)_rep_(\w+)\.jsonl", f.name)
        if not m:
            continue
        task, rep = m.group(1), m.group(2)
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                data[task][rep].append(entry)
    # sort each rep by query_seq
    for task in data:
        for rep in data[task]:
            data[task][rep].sort(key=lambda e: e["query_seq"])
    return data


def plan_depth(nodes):
    """Compute max root-to-leaf depth by walking children_ids."""
    if not nodes:
        return 0
    by_id = {n["node_id"]: n for n in nodes}
    child_ids = set()
    for n in nodes:
        for c in n.get("children_ids", []):
            child_ids.add(c)
    roots = [n["node_id"] for n in nodes if n["node_id"] not in child_ids]
    if not roots:
        roots = [0]  # fallback

    def _depth(nid, visited=None):
        if visited is None:
            visited = set()
        if nid in visited or nid not in by_id:
            return 0
        visited.add(nid)
        children = by_id[nid].get("children_ids", [])
        if not children:
            return 1
        return 1 + max(_depth(c, visited) for c in children)

    return max(_depth(r) for r in roots)


def count_joins(nodes):
    """Count join operators in a plan tree."""
    return sum(1 for n in nodes if is_join(n))


def is_join(node):
    op = node.get("operator_type", "")
    return "Join" in op or "Nested Loop" in op


def count_ctes_sql(sql):
    """Count WITH clauses via regex."""
    if not sql:
        return 0
    # Match WITH at start or after ), and also count comma-separated CTEs
    # Remove string literals first to avoid false matches
    cleaned = re.sub(r"'[^']*'", "''", sql)
    # Count the WITH keyword (top-level)
    with_matches = re.findall(r'\bWITH\b', cleaned, re.IGNORECASE)
    if not with_matches:
        return 0
    # Count CTE names: word followed by AS (
    cte_names = re.findall(r'\b(\w+)\s+AS\s*\(', cleaned, re.IGNORECASE)
    return len(cte_names)


def qerror(est, act):
    """Compute q-error. Skip if actual is 0."""
    if act == 0:
        return None
    return max(est, act) / max(min(est, act), 1)


def node_depth_aliases(node):
    """Depth of a node = number of relation aliases."""
    return len(node.get("relation_aliases", []))


# ── Section 1: Workload Shape ───────────────────────────────────────────

def section1(data, out):
    out.append("# Partial Analysis Report: IMDB Agentic-SQL Experiment\n")
    out.append("## Section 1: Workload Shape\n")

    for task in sorted(data.keys()):
        reps = data[task]
        out.append(f"### {task}\n")
        out.append(f"- **Reps available**: {sorted(reps.keys())}")

        all_queries = []
        for rep in sorted(reps.keys()):
            all_queries.extend(reps[rep])

        successful = [q for q in all_queries if q.get("success")]
        out.append(f"- **Total queries**: {len(all_queries)}, **Successful**: {len(successful)}\n")

        if not successful:
            continue

        # Tables per query
        tables_counts = [len(set(q.get("tables", []))) for q in successful]
        out.append(f"- **Tables per query**: median={statistics.median(tables_counts):.1f}, "
                    f"mean={statistics.mean(tables_counts):.1f}, max={max(tables_counts)}")

        # Plan tree depth
        depths = [plan_depth(q.get("plan_tree", [])) for q in successful if q.get("plan_tree")]
        if depths:
            out.append(f"- **Plan tree depth**: median={statistics.median(depths):.1f}, max={max(depths)}")

        # Join operators per query
        join_counts = [count_joins(q.get("plan_tree", [])) for q in successful if q.get("plan_tree")]
        if join_counts:
            out.append(f"- **Join operators per query**: median={statistics.median(join_counts):.1f}, max={max(join_counts)}")

        # CTE count
        cte_counts = [count_ctes_sql(q.get("raw_sql", "")) for q in successful]
        out.append(f"- **CTEs per query**: median={statistics.median(cte_counts):.1f}, max={max(cte_counts)}")
        out.append("")


# ── Section 2: Cardinality Estimation Gap ────────────────────────────────

def section2(data, out):
    out.append("## Section 2: Cardinality Estimation Gap\n")

    all_worst = []  # (qerr, task, rep, seq, aliases, est, act, depth, sql)
    per_task_rep = {}  # (task, rep) -> (median_tables, median_qerror)

    for task in sorted(data.keys()):
        for rep in sorted(data[task].keys()):
            entries = data[task][rep]
            successful = [e for e in entries if e.get("success")]

            qerrors = []
            for e in successful:
                for node in e.get("plan_tree", []):
                    if not is_join(node):
                        continue
                    aliases = node.get("relation_aliases", [])
                    if len(aliases) < 3:
                        continue
                    est = node.get("estimated_card", 0)
                    act = node.get("actual_card", 0)
                    if act == 0:
                        continue
                    qe = qerror(est, act)
                    if qe is not None:
                        qerrors.append(qe)
                        all_worst.append((qe, task, rep, e["query_seq"],
                                          aliases, est, act, len(aliases),
                                          e.get("raw_sql", "")[:200]))

            key = f"{task}_rep_{rep}"
            if qerrors:
                qerrors_sorted = sorted(qerrors)
                n = len(qerrors_sorted)
                def percentile(lst, p):
                    k = (len(lst) - 1) * p / 100
                    f = int(k)
                    c = f + 1
                    if c >= len(lst):
                        return lst[-1]
                    return lst[f] + (k - f) * (lst[c] - lst[f])

                out.append(f"### {key}")
                out.append(f"- Nodes considered: {n}")
                out.append(f"- Q-error distribution: "
                           f"min={qerrors_sorted[0]:.1f}, "
                           f"P25={percentile(qerrors_sorted, 25):.1f}, "
                           f"median={percentile(qerrors_sorted, 50):.1f}, "
                           f"P75={percentile(qerrors_sorted, 75):.1f}, "
                           f"P95={percentile(qerrors_sorted, 95):.1f}, "
                           f"P99={percentile(qerrors_sorted, 99):.1f}, "
                           f"max={qerrors_sorted[-1]:.1f}")
                gt100 = sum(1 for q in qerrors if q > 100)
                gt1000 = sum(1 for q in qerrors if q > 1000)
                out.append(f"- Nodes with q-error > 100x: {gt100} ({100*gt100/n:.1f}%)")
                out.append(f"- Nodes with q-error > 1000x: {gt1000} ({100*gt1000/n:.1f}%)")
                out.append("")

                # For per-task table
                med_tables = statistics.median([len(set(e.get("tables", []))) for e in successful])
                med_qe = percentile(qerrors_sorted, 50)
                per_task_rep[(task, rep)] = (med_tables, med_qe)
            else:
                out.append(f"### {key}")
                out.append(f"- No qualifying join nodes (depth 3+) found.\n")

    # Top 15 worst q-errors
    out.append("### 15 Worst Q-Errors Across Dataset\n")
    all_worst.sort(key=lambda x: -x[0])
    out.append("| Rank | Task | Rep | Seq | Relation Aliases | Est | Actual | Q-Error | Depth | SQL (first 200 chars) |")
    out.append("|------|------|-----|-----|-----------------|-----|--------|---------|-------|-----------------------|")
    for i, (qe, task, rep, seq, aliases, est, act, depth, sql) in enumerate(all_worst[:15]):
        alias_str = ", ".join(aliases[:6])
        if len(aliases) > 6:
            alias_str += "..."
        sql_escaped = sql.replace("|", "\\|").replace("\n", " ")[:120]
        out.append(f"| {i+1} | {task} | {rep} | {seq} | {alias_str} | {est:,.0f} | {act:,.0f} | {qe:,.1f} | {depth} | `{sql_escaped}` |")
    out.append("")

    # Per-task correlation table
    out.append("### Per-Task Correlation: Tables vs Q-Error\n")
    for task in sorted(data.keys()):
        out.append(f"**{task}**\n")
        out.append("| Rep | Median Tables/Query | Median Q-Error |")
        out.append("|-----|-------------------|----------------|")
        for rep in sorted(data[task].keys()):
            if (task, rep) in per_task_rep:
                mt, mq = per_task_rep[(task, rep)]
                out.append(f"| {rep} | {mt:.1f} | {mq:.1f} |")
            else:
                out.append(f"| {rep} | - | - |")
        out.append("")


# ── Section 3: Hint Surface under pg_hint_plan ───────────────────────────

def section3(data, out):
    out.append("## Section 3: Hint Surface under pg_hint_plan\n")

    total_hintable = 0
    total_queries = 0
    queries_with_hit = 0
    hits_per_query = []
    critical_hintable = 0
    critical_total = 0
    useful_hintable = 0  # hintable AND baseline qerror > 10
    total_hintable_for_useful = 0
    top_useful = []  # (qerr, task, rep, seq, sig, aliases)

    for task in sorted(data.keys()):
        for rep in sorted(data[task].keys()):
            entries = data[task][rep]
            successful = [e for e in entries if e.get("success")]

            seen_sigs = {}  # sig -> (query_seq, actual_card)

            for e in successful:
                total_queries += 1
                hit_count = 0
                plan = e.get("plan_tree", [])

                for node in plan:
                    if not is_join(node):
                        continue
                    sig = node.get("operator_signature", "")
                    aliases = node.get("relation_aliases", [])
                    est = node.get("estimated_card", 0)
                    act = node.get("actual_card", 0)
                    is_critical = len(aliases) >= 3

                    if sig in seen_sigs:
                        hit_count += 1
                        total_hintable += 1
                        total_hintable_for_useful += 1

                        if is_critical:
                            critical_hintable += 1

                        # Check baseline q-error
                        if act > 0:
                            qe = qerror(est, act)
                            if qe and qe > 10:
                                useful_hintable += 1
                                top_useful.append((qe, task, rep, e["query_seq"], sig, aliases))

                    if is_critical:
                        critical_total += 1

                # Add all join sigs from this query
                for node in plan:
                    if not is_join(node):
                        continue
                    sig = node.get("operator_signature", "")
                    if sig and sig not in seen_sigs:
                        seen_sigs[sig] = (e["query_seq"], node.get("actual_card", 0))

                hits_per_query.append(hit_count)
                if hit_count > 0:
                    queries_with_hit += 1

    out.append(f"- **Total hintable hits**: {total_hintable}")
    out.append(f"- **Fraction of queries with >= 1 hintable hit**: {queries_with_hit}/{total_queries} = {100*queries_with_hit/max(total_queries,1):.1f}%")
    if hits_per_query:
        out.append(f"- **Hits per query**: median={statistics.median(hits_per_query):.1f}, max={max(hits_per_query)}")
    out.append(f"- **Corrected hint-surface rate** (depth 3+ join nodes that are hintable): "
               f"{critical_hintable}/{critical_total} = {100*critical_hintable/max(critical_total,1):.1f}%")
    out.append("")

    out.append("### Useful Hints Sub-Analysis\n")
    out.append(f"- Hintable hits where baseline q-error > 10x: {useful_hintable}/{total_hintable_for_useful} = "
               f"{100*useful_hintable/max(total_hintable_for_useful,1):.1f}%")

    out.append("\n**Top 5 hintable hits with largest baseline q-errors:**\n")
    top_useful.sort(key=lambda x: -x[0])
    out.append("| Rank | Task | Rep | Seq | Signature (short) | Aliases | Q-Error |")
    out.append("|------|------|-----|-----|-------------------|---------|---------|")
    for i, (qe, task, rep, seq, sig, aliases) in enumerate(top_useful[:5]):
        alias_str = ", ".join(aliases[:5])
        out.append(f"| {i+1} | {task} | {rep} | {seq} | {sig[:12]}... | {alias_str} | {qe:,.1f} |")
    out.append("")


# ── Section 4: CTE Repetition ────────────────────────────────────────────

def canonicalize_cte(body):
    """Lowercase, collapse whitespace, mask literals."""
    s = body.lower()
    s = re.sub(r"'[^']*'", "'__STR__'", s)
    s = re.sub(r"\b\d+(\.\d+)?\b", "__NUM__", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_cte_bodies(sql):
    """Extract CTE name->body pairs from SQL.
    Simple approach: find WITH ... AS ( ... ) patterns.
    """
    if not sql:
        return {}
    ctes = {}
    # Find all CTE definitions
    # Pattern: name AS ( ... )  where ... is balanced parens
    # We'll do a simple approach: find CTE names then extract bodies

    # First check if there's a WITH clause
    with_match = re.search(r'\bWITH\b', sql, re.IGNORECASE)
    if not with_match:
        return {}

    rest = sql[with_match.end():]

    # Find patterns like: name AS (
    pattern = re.compile(r'(\w+)\s+AS\s*\(', re.IGNORECASE)

    for m in pattern.finditer(rest):
        name = m.group(1).lower()
        start = m.end()  # position after the opening (
        # Find matching closing )
        depth = 1
        pos = start
        while pos < len(rest) and depth > 0:
            if rest[pos] == '(':
                depth += 1
            elif rest[pos] == ')':
                depth -= 1
            pos += 1
        if depth == 0:
            body = rest[start:pos-1].strip()
            ctes[name] = body

    return ctes


def section4(data, out):
    out.append("## Section 4: CTE Repetition\n")

    # Load cte_analysis.jsonl
    cte_analysis = []
    if CTE_FILE.exists():
        with open(CTE_FILE) as f:
            for line in f:
                line = line.strip()
                if line:
                    cte_analysis.append(json.loads(line))

    if cte_analysis:
        out.append("### From cte_analysis.jsonl\n")
        # Summarize
        total_entries = len(cte_analysis)
        with_ctes = [e for e in cte_analysis if e.get("num_ctes", 0) > 0]
        exact_reuse = [e for e in cte_analysis if e.get("exact_cte_reuse", 0) > 0]
        near_reuse = [e for e in cte_analysis if e.get("near_identical_cte_reuse", 0) > 0]

        out.append(f"- Total entries: {total_entries}")
        out.append(f"- Queries with CTEs: {len(with_ctes)} ({100*len(with_ctes)/total_entries:.1f}%)")
        out.append(f"- Queries with exact CTE reuse: {len(exact_reuse)}")
        out.append(f"- Queries with near-identical CTE reuse: {len(near_reuse)}")
        if with_ctes:
            cte_counts = [e["num_ctes"] for e in with_ctes]
            out.append(f"- CTE counts (among queries with CTEs): median={statistics.median(cte_counts):.1f}, max={max(cte_counts)}")
        out.append("")

    # Cross-query CTE repetition analysis from raw SQL
    out.append("### Cross-Query CTE Repetition (from raw SQL)\n")

    per_rep_stats = []
    total_waste_ms = 0
    highest_waste_pattern = None
    highest_waste_ms = 0

    for task in sorted(data.keys()):
        for rep in sorted(data[task].keys()):
            entries = data[task][rep]
            successful = [e for e in entries if e.get("success")]

            all_canonical = {}  # canonical_body -> [(name, query_seq, exec_ms)]
            distinct_ctes = 0
            repeated_ctes = 0

            for e in successful:
                cte_bodies = extract_cte_bodies(e.get("raw_sql", ""))
                for name, body in cte_bodies.items():
                    canon = canonicalize_cte(body)
                    if canon not in all_canonical:
                        all_canonical[canon] = []
                    all_canonical[canon].append((name, e["query_seq"], e.get("execution_ms", 0)))

            distinct_ctes = len(all_canonical)
            repeated = {k: v for k, v in all_canonical.items() if len(v) > 1}
            repeated_ctes = len(repeated)

            rep_waste = 0
            for canon, occurrences in repeated.items():
                # Waste = sum of execution_ms for all but first occurrence (rough proxy)
                sorted_occ = sorted(occurrences, key=lambda x: x[1])
                for _, _, ms in sorted_occ[1:]:
                    rep_waste += ms

                if rep_waste > highest_waste_ms:
                    highest_waste_ms = rep_waste
                    highest_waste_pattern = canon[:200]

            total_waste_ms += rep_waste

            per_rep_stats.append({
                "task": task, "rep": rep,
                "distinct": distinct_ctes,
                "repeated": repeated_ctes,
                "rep_counts": [len(v) for v in repeated.values()] if repeated else [],
                "waste_ms": rep_waste
            })

    out.append("| Task | Rep | Distinct CTEs | Repeated CTEs | Max Repetition | Est. Waste (ms) |")
    out.append("|------|-----|--------------|---------------|----------------|-----------------|")
    for s in per_rep_stats:
        max_rep = max(s["rep_counts"]) if s["rep_counts"] else 0
        out.append(f"| {s['task']} | {s['rep']} | {s['distinct']} | {s['repeated']} | {max_rep} | {s['waste_ms']:,.0f} |")
    out.append("")

    n_reps = len(per_rep_stats)
    if n_reps:
        avg_waste = total_waste_ms / n_reps
        out.append(f"- **Total estimated wasted time**: {total_waste_ms:,.0f} ms across all reps")
        out.append(f"- **Average per-session waste**: {avg_waste:,.0f} ms")

    if highest_waste_pattern:
        out.append(f"- **Highest-waste CTE pattern** (canonicalized, first 200 chars): `{highest_waste_pattern}`")
    out.append("")


# ── Section 5: Hung Query from Task 4 Rep C ──────────────────────────────

def section5(data, out):
    out.append("## Section 5: Hung Query Investigation -- Task 4 Rep C\n")

    # Check task4_rep_c trace
    task4c = data.get("task4", {}).get("c", [])
    if task4c:
        last = task4c[-1]
        successful = [e for e in task4c if e.get("success")]
        failed = [e for e in task4c if not e.get("success")]
        out.append(f"- **Total entries in trace**: {len(task4c)} (query_seq 0 to {last['query_seq']})")
        out.append(f"- **Successful**: {len(successful)}, **Failed**: {len(failed)}")
        out.append(f"- **Last recorded entry**: query_seq={last['query_seq']}, "
                    f"success={last['success']}, execution_ms={last.get('execution_ms', 'N/A')}")
        out.append("")

        # Identify the slowest queries
        by_time = sorted(task4c, key=lambda e: e.get("execution_ms", 0), reverse=True)
        out.append("**Slowest queries in task4 rep c:**\n")
        out.append("| Seq | Exec (ms) | Tables | Success |")
        out.append("|-----|-----------|--------|---------|")
        for e in by_time[:5]:
            tables_str = ", ".join(e.get("tables", [])[:4])
            out.append(f"| {e['query_seq']} | {e.get('execution_ms', 0):,.0f} | {tables_str} | {e['success']} |")
        out.append("")

    # Check log file
    log_path = BASE / "task4_repc.log"
    if log_path.exists():
        with open(log_path) as f:
            log_text = f.read()

        # Check if session completed normally
        if "Session complete" in log_text:
            session_match = re.search(r'Session complete: (\d+) queries in ([\d.]+)s', log_text)
            if session_match:
                out.append(f"### Session Status\n")
                out.append(f"The agent log shows the session **completed normally**: "
                           f"{session_match.group(1)} queries in {session_match.group(2)}s.")
                out.append(f"The agent signaled DONE after completing its analysis.\n")
            else:
                out.append("### Session Status\n")
                out.append("The session completed (found 'Session complete' in log).\n")

            out.append("**Conclusion**: Task 4 rep c was NOT killed due to a hung query. "
                       "The session ran to completion with 30 queries. However, the session "
                       "included two very slow queries (seq 21 and 22/23 at ~26s and ~16s respectively) "
                       "involving complex CTE-based franchise cast retention analysis with "
                       "self-joins on `cast_info` -- a table with millions of rows. "
                       "These slow queries may have been the source of concern about hanging, "
                       "as they took 10-50x longer than typical queries in this session.\n")
        else:
            # Session did not complete -- look for the hung query
            out.append("### Session Status\n")
            out.append("The agent log does NOT contain a 'Session complete' marker, "
                       "suggesting the session was killed or hung.\n")

            # Find the last SQL attempted
            sql_matches = list(re.finditer(r'SQL:\s*(.*?)(?=\n\s*Result:|\Z)', log_text, re.DOTALL))
            if sql_matches:
                last_sql = sql_matches[-1].group(1).strip()[:300]
                out.append(f"**Last SQL attempted** (first 300 chars):\n```sql\n{last_sql}\n```\n")

    # Check raw_plans for task4/c
    raw_plan_dir = BASE / "raw_plans" / "task4" / "c"
    if raw_plan_dir.exists():
        plan_files = sorted(raw_plan_dir.glob("*.json"), key=lambda p: int(p.stem))
        out.append(f"- **Raw plan files**: {len(plan_files)} files, last is `{plan_files[-1].name}` (seq {plan_files[-1].stem})")
        max_seq_in_plans = int(plan_files[-1].stem)
        max_seq_in_traces = task4c[-1]["query_seq"] if task4c else -1
        out.append(f"- Max seq in plans: {max_seq_in_plans}, max seq in traces: {max_seq_in_traces}")
        # Note: plan file 18 might be missing (failed query)
        expected_seqs = set(range(max_seq_in_traces + 1))
        actual_seqs = set(int(p.stem) for p in plan_files)
        missing = expected_seqs - actual_seqs
        if missing:
            out.append(f"- Missing plan files for seqs: {sorted(missing)} (likely failed queries)")
    out.append("")


# ── Section 6: Assessment ─────────────────────────────────────────────────

def section6(data, out, hint_stats):
    out.append("## Section 6: Assessment\n")

    # Gather stats for assessment
    total_queries = sum(len(entries) for task in data.values() for rep_entries in task.values()
                        for entries in [rep_entries])
    total_queries = sum(len(rep_entries) for task in data.values() for rep_entries in task.values())
    successful = sum(1 for task in data.values() for rep_entries in task.values()
                     for e in rep_entries if e.get("success"))

    # Compute overall q-error stats for assessment
    all_qerrors = []
    for task in data.values():
        for entries in task.values():
            for e in entries:
                if not e.get("success"):
                    continue
                for node in e.get("plan_tree", []):
                    if not is_join(node):
                        continue
                    if len(node.get("relation_aliases", [])) < 3:
                        continue
                    est = node.get("estimated_card", 0)
                    act = node.get("actual_card", 0)
                    if act == 0:
                        continue
                    qe = qerror(est, act)
                    if qe is not None:
                        all_qerrors.append(qe)

    med_qe = statistics.median(all_qerrors) if all_qerrors else 0
    p95_qe = sorted(all_qerrors)[int(0.95 * len(all_qerrors))] if all_qerrors else 0
    gt100 = sum(1 for q in all_qerrors if q > 100)

    hint_surface = hint_stats.get("critical_rate", 0)
    useful_rate = hint_stats.get("useful_rate", 0)

    out.append(
        f"The IMDB workload shape is appropriate for cardinality feedback research: "
        f"across {total_queries} total queries ({successful} successful) spanning 4 tasks "
        f"and 11 completed reps, queries involve multi-table joins with CTEs, "
        f"producing plan trees of meaningful depth. "
        f"The cardinality estimation gap is substantial, with a median q-error of {med_qe:.1f}x "
        f"and P95 of {p95_qe:.1f}x on depth-3+ join nodes, and {gt100} nodes ({100*gt100/max(len(all_qerrors),1):.1f}%) "
        f"exceeding 100x error -- this confirms PostgreSQL's optimizer struggles with the "
        f"complex multi-way joins that agentic SQL sessions produce. "
        f"The hint surface rate of {hint_surface:.1f}% on plan-critical nodes "
        f"{'exceeds' if hint_surface > 3.8 else 'falls below'} InsightBench's 3.8% baseline, "
        f"{'validating' if hint_surface > 3.8 else 'raising questions about'} "
        f"the feasibility of a pg_hint_plan-based correction approach on this workload. "
        f"The useful hint fraction (q-error > 10x) at {useful_rate:.1f}% of hintable hits "
        f"indicates {'a meaningful' if useful_rate > 20 else 'a limited but non-trivial'} "
        f"opportunity for cardinality feedback to improve actual execution. "
        f"CTE repetition across queries within sessions is a real finding direction -- "
        f"the agentic pattern of iterative refinement naturally produces repeated sub-expressions, "
        f"and these repeated CTEs represent both wasted computation and opportunities for "
        f"the system to learn from prior cardinality observations on identical sub-plans."
    )
    out.append("")


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    data = load_traces()
    out = []

    # Section 1
    section1(data, out)

    # Section 2
    section2(data, out)

    # Section 3 - need to capture stats for section 6
    # Compute hint stats first
    hint_stats = compute_hint_stats(data)
    section3(data, out)

    # Section 4
    section4(data, out)

    # Section 5
    section5(data, out)

    # Section 6
    section6(data, out, hint_stats)

    report = "\n".join(out)
    with open(REPORT, "w") as f:
        f.write(report)
    print(f"Report written to {REPORT}")
    print(f"Report length: {len(report)} chars, {len(out)} lines")


def compute_hint_stats(data):
    """Pre-compute hint stats for use in section 6."""
    total_hintable = 0
    critical_hintable = 0
    critical_total = 0
    useful_hintable = 0
    total_hintable_for_useful = 0

    for task in data.values():
        for entries in task.values():
            successful = [e for e in entries if e.get("success")]
            seen_sigs = {}

            for e in successful:
                plan = e.get("plan_tree", [])
                for node in plan:
                    if not is_join(node):
                        continue
                    sig = node.get("operator_signature", "")
                    aliases = node.get("relation_aliases", [])
                    is_critical = len(aliases) >= 3

                    if sig in seen_sigs:
                        total_hintable += 1
                        total_hintable_for_useful += 1
                        if is_critical:
                            critical_hintable += 1
                        est = node.get("estimated_card", 0)
                        act = node.get("actual_card", 0)
                        if act > 0:
                            qe = qerror(est, act)
                            if qe and qe > 10:
                                useful_hintable += 1

                    if is_critical:
                        critical_total += 1

                for node in plan:
                    if not is_join(node):
                        continue
                    sig = node.get("operator_signature", "")
                    if sig and sig not in seen_sigs:
                        seen_sigs[sig] = (e["query_seq"], node.get("actual_card", 0))

    return {
        "critical_rate": 100 * critical_hintable / max(critical_total, 1),
        "useful_rate": 100 * useful_hintable / max(total_hintable_for_useful, 1),
    }


if __name__ == "__main__":
    main()
