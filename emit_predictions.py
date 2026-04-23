#!/usr/bin/env python3
"""Emit d2_predictions.csv from simulator results on recorded traces."""

import csv
import json
import sys
from pathlib import Path

from d2_sim import simulate, plan_base_tables

SESSIONS = [
    ("task4_rep_c", Path("imdb/traces/task4_rep_c.jsonl")),
    ("task2_rep_b", Path("imdb/traces/task2_rep_b.jsonl")),
]

OUTPUT = Path("imdb/traces/d2_predictions.csv")

COLUMNS = [
    "session", "query_seq", "action", "handle_id", "handle_name",
    "description", "save_sql", "rewritten_sql", "predicted_saved_ms",
    "cov", "original_ms",
]


def auto_description(row):
    """Generate description for save rows."""
    plan_tree = row.get("plan_tree", [])
    tables = sorted(plan_base_tables(plan_tree))
    n = len(tables)
    ms = round(row.get("execution_ms", 0))
    root_card = 0
    if plan_tree:
        root = next((n for n in plan_tree if n["node_id"] == 0), plan_tree[0])
        root_card = root.get("actual_card", 0) or root.get("estimated_card", 0) or 0
    return (
        f"Result of {n}-table join across [{', '.join(tables)}]. "
        f"Runs in ~{ms} ms. Produces {root_card} rows."
    )


def main():
    all_rows = []
    session_stats = {}

    for session_name, trace_path in SESSIONS:
        trace_rows = [json.loads(line) for line in open(trace_path)]
        result = simulate(trace_rows)

        # Build lookup from query_seq -> trace row for save_sql and description
        trace_by_seq = {r["query_seq"]: r for r in trace_rows}

        # Validation: unique handle_ids within session
        save_ids = [p["handle_id"] for p in result["per_query"] if p["action"] == "save"]
        if len(save_ids) != len(set(save_ids)):
            print(f"ERROR: duplicate handle_ids in {session_name}: {save_ids}", file=sys.stderr)
            sys.exit(1)

        # Validation: reuse handle_ids must reference earlier saves
        save_id_set = set()
        for p in result["per_query"]:
            if p["action"] == "save":
                save_id_set.add(p["handle_id"])
            elif p["action"] == "reuse":
                if p["handle_id"] not in save_id_set:
                    print(
                        f"ERROR: reuse of {p['handle_id']} at query_seq={p['query_seq']} "
                        f"in {session_name} but no prior save",
                        file=sys.stderr,
                    )
                    sys.exit(1)

        for p in result["per_query"]:
            qseq = p["query_seq"]
            trace_row = trace_by_seq[qseq]
            action = p["action"]

            csv_row = {
                "session": session_name,
                "query_seq": qseq,
                "action": action,
                "handle_id": p["handle_id"] or "",
                "handle_name": "",
                "description": "",
                "save_sql": "",
                "rewritten_sql": "",
                "predicted_saved_ms": round(p["predicted_saved_ms"], 2),
                "cov": "",
                "original_ms": round(p["original_ms"], 2),
            }

            if action == "save":
                csv_row["description"] = auto_description(trace_row)
                csv_row["save_sql"] = trace_row["raw_sql"].strip()
            elif action == "reuse":
                csv_row["cov"] = round(p["cov"], 4)

            all_rows.append(csv_row)

        session_stats[session_name] = {
            "save": sum(1 for p in result["per_query"] if p["action"] == "save"),
            "reuse": sum(1 for p in result["per_query"] if p["action"] == "reuse"),
            "passthrough": sum(1 for p in result["per_query"] if p["action"] == "passthrough"),
            "failed": sum(1 for p in result["per_query"] if p["action"] == "failed"),
            "savings_pct": result["savings_pct"],
            "n_queries": len(trace_rows),
        }

    # Validation: row count
    expected_total = sum(s["n_queries"] for s in session_stats.values())
    if len(all_rows) != expected_total:
        print(
            f"ERROR: row count mismatch: {len(all_rows)} rows vs {expected_total} expected",
            file=sys.stderr,
        )
        sys.exit(1)

    # Write CSV
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(all_rows)

    # Summary
    print(f"Wrote {OUTPUT} with {len(all_rows)} rows across {len(SESSIONS)} sessions.\n")

    total_saves = 0
    total_reuses = 0
    for session_name, stats in session_stats.items():
        print(f"  {session_name}:")
        print(f"    save rows:        {stats['save']}  (need handle_name)")
        print(f"    reuse rows:       {stats['reuse']}  (need rewritten_sql)")
        print(f"    passthrough:      {stats['passthrough']}")
        print(f"    failed:           {stats['failed']}")
        print(f"    predicted savings: {stats['savings_pct']:.1f}%")
        print()
        total_saves += stats["save"]
        total_reuses += stats["reuse"]

    print(
        f"  MANUAL TODO: fill in handle_name for {total_saves} save rows and\n"
        f"               rewritten_sql for {total_reuses} reuse rows."
    )


if __name__ == "__main__":
    main()
