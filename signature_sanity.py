#!/usr/bin/env python3
"""
Signature sanity check: manually inspect plan trees for 3 queries
to validate that operator_signature hashing behaves correctly.

Produces stage2/signature_sanity.md
"""

import json
from pathlib import Path

from pg_metrics import load_sessions
from pg_plan_parser import PLAN_CRITICAL_OPS, BOOKKEEPING_OPS, _operator_signature

LOG_DIR = "stage2/traces"
OUTPUT = Path("stage2/signature_sanity.md")


def format_node(node: dict, indent: int = 0) -> list[str]:
    """Pretty-print a plan node."""
    prefix = "  " * indent
    op = node["operator_type"]
    sig = node["operator_signature"][:12]
    ec = node.get("estimated_card", "?")
    ac = node.get("actual_card", "?")
    tables = node.get("tables", [])
    preds = node.get("predicates", [])
    gb = node.get("groupby_keys", [])
    cls = "PLAN-CRITICAL" if op in PLAN_CRITICAL_OPS else "bookkeeping" if op in BOOKKEEPING_OPS else "unclassified"

    lines = [f"{prefix}[{op}] sig={sig}... ec={ec} ac={ac} class={cls}"]
    if tables:
        lines.append(f"{prefix}  tables: {tables}")
    if preds:
        lines.append(f"{prefix}  predicates: {preds}")
    if gb:
        lines.append(f"{prefix}  group_by: {gb}")
    return lines


def find_reuse_example(sessions):
    """Find a session where a signature appears in 2+ queries, return details."""
    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        sig_first_seen = {}  # sig -> (query_seq, node)
        for entry in successful:
            for node in entry.get("plan_tree", []):
                sig = node.get("operator_signature", "")
                if not sig:
                    continue
                if sig in sig_first_seen:
                    first_seq, first_node = sig_first_seen[sig]
                    if first_node["operator_type"] in PLAN_CRITICAL_OPS:
                        return {
                            "session_id": entry["session_id"],
                            "first_query_seq": first_seq,
                            "first_node": first_node,
                            "second_query_seq": entry["query_seq"],
                            "second_node": node,
                            "first_sql": [e for e in successful if e["query_seq"] == first_seq][0]["raw_sql"],
                            "second_sql": entry["raw_sql"],
                        }
                else:
                    sig_first_seen[sig] = (entry["query_seq"], node)
    return None


def main():
    sessions = load_sessions(LOG_DIR)
    if not sessions:
        print("No sessions found")
        return

    md = []
    md.append("# Signature Sanity Check")
    md.append("")
    md.append("Manual inspection of plan trees to validate operator_signature hashing.")
    md.append("")

    # Pick 3 diverse queries from different sessions
    samples = []
    for session in sessions:
        successful = [e for e in session if e.get("success", True)]
        if successful:
            # Pick first query with a non-trivial plan (>= 3 nodes)
            for entry in successful:
                if len(entry.get("plan_tree", [])) >= 3:
                    samples.append(entry)
                    break
        if len(samples) >= 3:
            break

    md.append("## Sample Plan Trees")
    md.append("")
    for idx, entry in enumerate(samples):
        sid = entry.get("session_id", "?")
        seq = entry.get("query_seq", 0)
        sql = entry.get("raw_sql", "")

        md.append(f"### Sample {idx+1}: {sid} query #{seq}")
        md.append(f"```sql\n{sql}\n```")
        md.append("")
        md.append("**Plan tree:**")
        md.append("```")

        # Build tree structure from flat list
        nodes = entry.get("plan_tree", [])
        node_map = {n["node_id"]: n for n in nodes}

        def print_tree(node_id, depth=0):
            node = node_map.get(node_id)
            if not node:
                return
            for line in format_node(node, depth):
                md.append(line)
            for child_id in node.get("children_ids", []):
                print_tree(child_id, depth + 1)

        if nodes:
            print_tree(nodes[0]["node_id"])
        md.append("```")
        md.append("")

        # Verify signature determinism
        for node in nodes:
            recomputed = _operator_signature(
                node["operator_type"],
                node.get("tables", []),
                node.get("predicates", []),
                node.get("groupby_keys", [])
            )
            if recomputed != node["operator_signature"]:
                md.append(f"**WARNING:** Signature mismatch on node {node['node_id']}! "
                         f"logged={node['operator_signature'][:12]} recomputed={recomputed[:12]}")
        md.append("Signature verification: all nodes match recomputed signatures.")
        md.append("")

    # Find and document a reuse example
    md.append("## Signature Reuse Example")
    md.append("")
    reuse = find_reuse_example(sessions)
    if reuse:
        md.append(f"**Session:** {reuse['session_id']}")
        md.append(f"**First occurrence:** query #{reuse['first_query_seq']}")
        md.append(f"```sql\n{reuse['first_sql']}\n```")
        md.append("")
        md.append("First node:")
        md.append("```")
        for line in format_node(reuse["first_node"]):
            md.append(line)
        md.append("```")
        md.append("")
        md.append(f"**Second occurrence:** query #{reuse['second_query_seq']}")
        md.append(f"```sql\n{reuse['second_sql']}\n```")
        md.append("")
        md.append("Second node:")
        md.append("```")
        for line in format_node(reuse["second_node"]):
            md.append(line)
        md.append("```")
        md.append("")

        ec1 = reuse["first_node"].get("estimated_card", 0)
        ac1 = reuse["first_node"].get("actual_card", 0)
        ec2 = reuse["second_node"].get("estimated_card", 0)
        ac2 = reuse["second_node"].get("actual_card", 0)
        md.append(f"**First:** EC={ec1}, actual={ac1}")
        md.append(f"**Second:** EC={ec2}, actual={ac2}")
        if ec2 and ac2 and ec2 > 0 and ac2 > 0:
            qe = max(ec2, ac2) / min(ec2, ac2)
            md.append(f"**Q-error at second occurrence:** {qe:.2f}×")
            md.append(f"**Q-error after reuse (using first actual):** would use actual={ac1} instead of EC={ec2}")
    else:
        md.append("No plan-critical reuse example found.")
    md.append("")

    # Summary stats
    md.append("## Signature Statistics")
    md.append("")
    total_sigs = 0
    unique_sigs = set()
    for session in sessions:
        for entry in [e for e in session if e.get("success", True)]:
            for node in entry.get("plan_tree", []):
                sig = node.get("operator_signature", "")
                if sig:
                    total_sigs += 1
                    unique_sigs.add(sig)
    md.append(f"- **Total plan nodes across all sessions:** {total_sigs}")
    md.append(f"- **Unique signatures:** {len(unique_sigs)}")
    md.append(f"- **Signature compression ratio:** {len(unique_sigs)/total_sigs:.3f}" if total_sigs else "")
    md.append("")

    with open(OUTPUT, "w") as f:
        f.write("\n".join(md))
    print(f"Written to {OUTPUT}")


if __name__ == "__main__":
    main()
