#!/bin/bash
set -e

cd /scratch/agentic-sql-pg/imdb
PYTHON="../.venv/bin/python3"
export PYTHONPATH=/scratch/agentic-sql-pg

LOG=sessions/scenario1_full.log

echo "=== Scenario 1: Genre Evolution (M1+M2), 3 reps ===" | tee -a $LOG
echo "Started at $(date)" | tee -a $LOG

for REP in a b c; do
    echo "=== Rep $REP starting at $(date) ===" | tee -a $LOG
    $PYTHON -u scenario1_agent.py --rep $REP 2>&1 | tee -a $LOG

    echo "=== Rep $REP done. Generating interim report... ===" | tee -a $LOG
    $PYTHON -u scenario1_report.py 2>&1 | tee -a $LOG
    echo "" | tee -a $LOG
done

echo "=== All 3 reps complete at $(date) ===" | tee -a $LOG
echo "Report: reports/scenario1_genre_evolution.md" | tee -a $LOG
