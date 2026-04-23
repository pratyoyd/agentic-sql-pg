"""
Direction 2 simulator.

For each recorded session, simulate what the workspace.save/catalog/reuse
protocol would do. Save a handle for any query that is expensive AND does
real join work. Reuse a handle for any later query whose plan structurally
subsumes the handle.

Matching:
  A handle H matches query Q iff:
    1. tables(H) ⊆ tables(Q), where tables() means BASE tables from the
       plan tree leaves (NOT CTE aliases from row['tables']).
    2. At least 40% of H's cardinality-weighted signatures appear in Q's
       plan. Weighting by actual_card ensures matching the expensive parts
       of H, not cheap Sort/Hash nodes.

Saving:
  Save Q's result if:
    - scans(Q) ≥ 3 AND execution_ms > 500, OR
    - execution_ms > 1500 AND scans(Q) ≥ 2
  where scans = count of leaf base-table scans in the plan.

If a query hits an existing handle, reuse and do NOT also save.
"""

import json
import re
from pathlib import Path
from dataclasses import dataclass

def normalize_sql(sql: str) -> str:
    s = sql.lower()
    s = re.sub(r'--[^\n]*', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def plan_signatures(plan_tree):
    return {n['operator_signature'] for n in plan_tree}

def plan_base_tables(plan_tree):
    ts = set()
    for n in plan_tree:
        for t in n.get('tables', []):
            if t:
                ts.add(t)
    return frozenset(ts)

def plan_base_scans(plan_tree):
    count = 0
    for n in plan_tree:
        if n['operator_type'] in ('Seq Scan', 'Index Scan',
                                   'Bitmap Heap Scan', 'Index Only Scan'):
            if n.get('tables'):
                count += 1
    return count

def sig_weights(plan_tree):
    w = {}
    for n in plan_tree:
        s = n['operator_signature']
        c = n.get('actual_card', 0) or n.get('estimated_card', 0) or 1
        w[s] = max(w.get(s, 0), c)
    return w

def tables_of(row):
    return plan_base_tables(row.get('plan_tree', []))

def estimate_handle_bytes(plan_tree):
    if not plan_tree:
        return 10_000
    root = next((n for n in plan_tree if n['node_id'] == 0), plan_tree[0])
    rows = root.get('actual_card', 0) or root.get('estimated_card', 0) or 100
    return max(rows * 100, 4096)

@dataclass
class Handle:
    name: str
    tables: frozenset
    sql_norm: str
    plan_sig_set: frozenset
    sig_weights: dict
    creation_ms: float
    byte_size: int
    source_query_seq: int

def match(query_row, handles):
    q_tables = tables_of(query_row)
    q_plan = query_row.get('plan_tree', [])
    q_sigs = plan_signatures(q_plan)
    q_weights = sig_weights(q_plan)
    q_total_weight = sum(q_weights.values()) or 1
    q_sql = normalize_sql(query_row['raw_sql'])

    best = None
    best_cov = 0.0

    for h in handles:
        if not h.tables or not h.tables.issubset(q_tables):
            continue

        h_total = sum(h.sig_weights.values()) or 1
        covered_in_h = sum(w for s, w in h.sig_weights.items() if s in q_sigs)
        struct_cov = covered_in_h / h_total

        textual_hit = h.sql_norm in q_sql and len(h.sql_norm) > 80

        if struct_cov < 0.4 and not textual_hit:
            continue

        covered_sigs = h.plan_sig_set & q_sigs
        covered_weight = sum(q_weights.get(s, 0) for s in covered_sigs)
        cost_frac = covered_weight / q_total_weight
        cost_frac = min(cost_frac, 0.95)

        if cost_frac > best_cov:
            best_cov = cost_frac
            best = h

    return best, best_cov

def should_save(row):
    if not row.get('success', True):
        return False
    ms = row.get('execution_ms', 0)
    scans = plan_base_scans(row.get('plan_tree', []))
    if scans >= 3 and ms > 500:
        return True
    if ms > 1500 and scans >= 2:
        return True
    return False

def simulate(session_rows):
    """Simulate the protocol. Returns summary + per_query decision list."""
    handles = []
    per_query = []
    baseline_ms = 0.0
    simulated_ms = 0.0

    for i, row in enumerate(session_rows):
        qseq = row['query_seq']
        ms = row.get('execution_ms', 0)
        baseline_ms += ms

        if not row.get('success', True):
            simulated_ms += ms
            per_query.append({
                'query_seq': qseq, 'action': 'failed',
                'original_ms': ms, 'handle_id': None,
                'predicted_saved_ms': 0.0, 'cov': None,
            })
            continue

        h, cov = match(row, handles)
        if h is not None and cov > 0:
            lookup_cost = max(5.0, h.byte_size / (100 * 1024 * 1024) * 1000)
            saved = max(0.0, min(h.creation_ms, cov * ms) - lookup_cost)
            simulated_ms += (ms - saved)
            per_query.append({
                'query_seq': qseq, 'action': 'reuse',
                'original_ms': ms, 'handle_id': h.name,
                'predicted_saved_ms': saved, 'cov': cov,
            })
            continue

        simulated_ms += ms

        if should_save(row):
            name = f'h{qseq}'
            handles.append(Handle(
                name=name,
                tables=tables_of(row),
                sql_norm=normalize_sql(row['raw_sql']),
                plan_sig_set=frozenset(plan_signatures(row['plan_tree'])),
                sig_weights=sig_weights(row['plan_tree']),
                creation_ms=ms,
                byte_size=estimate_handle_bytes(row['plan_tree']),
                source_query_seq=qseq,
            ))
            per_query.append({
                'query_seq': qseq, 'action': 'save',
                'original_ms': ms, 'handle_id': name,
                'predicted_saved_ms': 0.0, 'cov': None,
            })
        else:
            per_query.append({
                'query_seq': qseq, 'action': 'passthrough',
                'original_ms': ms, 'handle_id': None,
                'predicted_saved_ms': 0.0, 'cov': None,
            })

    savings = baseline_ms - simulated_ms
    return {
        'baseline_ms': baseline_ms,
        'simulated_ms': simulated_ms,
        'savings_ms': savings,
        'savings_pct': 100.0 * savings / baseline_ms if baseline_ms else 0,
        'n_save': sum(1 for p in per_query if p['action'] == 'save'),
        'n_reuse': sum(1 for p in per_query if p['action'] == 'reuse'),
        'n_queries': len(session_rows),
        'per_query': per_query,
    }

if __name__ == '__main__':
    import sys
    for path in sys.argv[1:]:
        rows = [json.loads(l) for l in open(path)]
        res = simulate(rows)
        print(f"{path}: baseline {res['baseline_ms']/1000:.1f}s -> "
              f"simulated {res['simulated_ms']/1000:.1f}s "
              f"({res['savings_pct']:.1f}% saved, "
              f"{res['n_save']} saves, {res['n_reuse']} reuses)")
