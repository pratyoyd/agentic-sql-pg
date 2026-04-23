#!/bin/bash
set -e

cd /scratch/agentic-sql-pg/imdb
PYTHON="../.venv/bin/python3"
export PYTHONPATH=/scratch/agentic-sql-pg

LOG=sessions/all_scenarios_2_to_10.log

echo "=== Scenarios 2-10: 9 scenarios, 2 reps each, 18 sessions ===" | tee -a $LOG
echo "Started at $(date)" | tee -a $LOG
echo "" | tee -a $LOG

for SCENARIO in 2 3 4 5 6 7 8 9 10; do
    echo "========================================" | tee -a $LOG
    echo "=== Starting Scenario $SCENARIO at $(date) ===" | tee -a $LOG
    echo "========================================" | tee -a $LOG

    for REP in a b; do
        echo "=== Scenario $SCENARIO Rep $REP starting at $(date) ===" | tee -a $LOG
        $PYTHON -u scenario_agent.py --scenario $SCENARIO --rep $REP 2>&1 | tee -a $LOG

        echo "=== Scenario $SCENARIO Rep $REP done. Generating interim report... ===" | tee -a $LOG
        $PYTHON -u scenario_report.py --scenario $SCENARIO 2>&1 | tee -a $LOG
        echo "" | tee -a $LOG
    done

    echo "=== Scenario $SCENARIO complete at $(date) ===" | tee -a $LOG
    echo "" | tee -a $LOG
done

echo "=== Generating aggregate report ===" | tee -a $LOG
$PYTHON -u scenario_report.py --scenario 0 2>&1 | tee -a $LOG

echo "=== All 9 scenarios complete at $(date) ===" | tee -a $LOG
echo "Reports: reports/scenario*_*.md" | tee -a $LOG
echo "Aggregate: reports/aggregate_scenarios_2_to_10.md" | tee -a $LOG
