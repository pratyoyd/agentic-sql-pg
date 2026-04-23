#!/bin/bash
set -e

cd /scratch/agentic-sql-pg/imdb
PYTHON="../.venv/bin/python3"
export PYTHONPATH=/scratch/agentic-sql-pg

SCENARIO=4

echo "=== Scenario $SCENARIO, 2 reps ==="

for REP in a b; do
    echo "=== Scenario $SCENARIO Rep $REP starting at $(date) ==="
    $PYTHON -u scenario_agent.py --scenario $SCENARIO --rep $REP 2>&1 | tee -a sessions/scenario${SCENARIO}_full.log

    echo "=== Scenario $SCENARIO Rep $REP done. Generating interim report... ==="
    $PYTHON -u scenario_report.py --scenario $SCENARIO 2>&1 | tee -a sessions/scenario${SCENARIO}_full.log
    echo ""
done

echo "=== Scenario $SCENARIO complete at $(date) ==="
