#!/bin/bash
set -e

cd /scratch/agentic-sql-pg/imdb
PYTHON="../.venv/bin/python3"
export PYTHONPATH=/scratch/agentic-sql-pg

echo "=== Scenario 1: Genre Evolution (M1+M2), 3 reps ==="
echo "Model: opus, Temperature: default"
echo ""

for REP in a b c; do
    echo "=== Rep $REP starting at $(date) ==="
    $PYTHON -u scenario1_agent.py --rep $REP 2>&1 | tee sessions/scenario1_rep_${REP}_console.log

    echo "=== Rep $REP done. Generating interim report... ==="
    $PYTHON -u scenario1_report.py
    echo ""
done

echo "=== All 3 reps complete. Final report at reports/scenario1_genre_evolution.md ==="
