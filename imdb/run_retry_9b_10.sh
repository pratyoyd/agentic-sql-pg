#!/bin/bash
set -e

cd /scratch/agentic-sql-pg/imdb
PYTHON="../.venv/bin/python3"
export PYTHONPATH=/scratch/agentic-sql-pg

LOG=sessions/retry_9b_10.log

echo "=== Retry: scenario9 rep b, scenario10 reps a+b ===" | tee -a $LOG
echo "Started at $(date), 2min sleep between sessions" | tee -a $LOG

echo "=== Scenario 9 Rep b starting at $(date) ===" | tee -a $LOG
$PYTHON -u scenario_agent.py --scenario 9 --rep b 2>&1 | tee -a $LOG
$PYTHON -u scenario_report.py --scenario 9 2>&1 | tee -a $LOG

echo "Sleeping 120s..." | tee -a $LOG
sleep 120

echo "=== Scenario 10 Rep a starting at $(date) ===" | tee -a $LOG
$PYTHON -u scenario_agent.py --scenario 10 --rep a 2>&1 | tee -a $LOG
$PYTHON -u scenario_report.py --scenario 10 2>&1 | tee -a $LOG

echo "Sleeping 120s..." | tee -a $LOG
sleep 120

echo "=== Scenario 10 Rep b starting at $(date) ===" | tee -a $LOG
$PYTHON -u scenario_agent.py --scenario 10 --rep b 2>&1 | tee -a $LOG
$PYTHON -u scenario_report.py --scenario 10 2>&1 | tee -a $LOG

echo "=== Regenerating aggregate report ===" | tee -a $LOG
$PYTHON -u scenario_report.py --scenario 0 2>&1 | tee -a $LOG

echo "=== Retry complete at $(date) ===" | tee -a $LOG
