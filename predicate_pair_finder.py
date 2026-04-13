#!/usr/bin/env python3
"""
Part A: Find 20 near-miss predicate pairs from Postgres traces.

A near-miss pair has:
- Same operator type
- Same table set
- Same group-by keys (or both none)
- Different signatures (hasher said no)
- Different predicate strings
- From different queries in the same session
"""

import json
from collections import defaultdict
from pathlib import Path

from pg_plan_parser import PLAN_CRITICAL_OPS, _templatize_predicate

LOG_DIR = Path("stage2/traces")


def load_sessions():
    sessions = []
    for f in sorted(LOG_DIR.glob("*.jsonl")):
        entries = []
        for line in open(f):
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        if entries:
            sessions.append(entries)
    return sessions


def find_near_miss_pairs(sessions, max_pairs=20):
    """Find pairs of nodes that almost match but differ on predicates."""
    pairs = []
    # Track which sessions/op types we've already sampled from for diversity
    session_counts = defaultdict(int)
    optype_counts = defaultdict(int)

    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        sid = successful[0]["session_id"] if successful else "?"

        # Group all plan nodes by (operator_type, tuple(tables), tuple(groupby_keys))
        groups = defaultdict(list)
        for entry in successful:
            for node in entry.get("plan_tree", []):
                op = node["operator_type"]
                if op not in PLAN_CRITICAL_OPS:
                    continue
                key = (op, tuple(sorted(node.get("tables", []))),
                       tuple(sorted(node.get("groupby_keys", []))))
                groups[key].append({
                    "node": node,
                    "query_seq": entry["query_seq"],
                    "raw_sql": entry.get("raw_sql", "")[:200],
                    "session_id": sid,
                })

        # Within each group, find pairs with different signatures and different predicates
        for key, nodes in groups.items():
            if len(nodes) < 2:
                continue
            seen_sig_pairs = set()
            for i in range(len(nodes)):
                for j in range(i + 1, len(nodes)):
                    ni, nj = nodes[i], nodes[j]
                    sig_i = ni["node"]["operator_signature"]
                    sig_j = nj["node"]["operator_signature"]
                    if sig_i == sig_j:
                        continue  # they matched - not a near miss
                    pred_i = ni["node"].get("predicates", [])
                    pred_j = nj["node"].get("predicates", [])
                    if pred_i == pred_j:
                        continue  # same predicates, shouldn't happen but skip
                    # Templatize and check if templates differ
                    tmpl_i = sorted(_templatize_predicate(p) for p in pred_i)
                    tmpl_j = sorted(_templatize_predicate(p) for p in pred_j)
                    if tmpl_i == tmpl_j:
                        continue  # templates match but sigs differ - shouldn't happen

                    pair_key = (sig_i, sig_j) if sig_i < sig_j else (sig_j, sig_i)
                    if pair_key in seen_sig_pairs:
                        continue
                    seen_sig_pairs.add(pair_key)

                    # Prefer diversity: don't over-sample one session or op type
                    if session_counts[sid] >= 6 or optype_counts[key[0]] >= 8:
                        continue

                    pairs.append({
                        "session_id": sid,
                        "operator_type": key[0],
                        "tables": list(key[1]),
                        "groupby_keys": list(key[2]),
                        "pred_a": pred_i,
                        "pred_b": pred_j,
                        "tmpl_a": tmpl_i,
                        "tmpl_b": tmpl_j,
                        "query_a_seq": ni["query_seq"],
                        "query_b_seq": nj["query_seq"],
                        "sql_a": ni["raw_sql"],
                        "sql_b": nj["raw_sql"],
                    })
                    session_counts[sid] += 1
                    optype_counts[key[0]] += 1

                    if len(pairs) >= max_pairs:
                        return pairs
    return pairs


def main():
    sessions = load_sessions()
    print(f"Loaded {len(sessions)} sessions")
    pairs = find_near_miss_pairs(sessions, max_pairs=20)
    print(f"Found {len(pairs)} near-miss pairs")
    print()

    for i, p in enumerate(pairs):
        print(f"--- Pair {i+1} ---")
        print(f"  Session: {p['session_id']}")
        print(f"  Op: {p['operator_type']}, Tables: {p['tables']}, GB: {p['groupby_keys']}")
        print(f"  Q{p['query_a_seq']} preds: {p['pred_a']}")
        print(f"  Q{p['query_b_seq']} preds: {p['pred_b']}")
        print(f"  Templatized A: {p['tmpl_a']}")
        print(f"  Templatized B: {p['tmpl_b']}")
        print()

    # Summary by session and op type
    from collections import Counter
    print("By session:", Counter(p["session_id"] for p in pairs))
    print("By op type:", Counter(p["operator_type"] for p in pairs))


if __name__ == "__main__":
    main()
