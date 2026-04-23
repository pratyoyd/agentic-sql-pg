#!/bin/bash
set -e

cd /scratch/agentic-sql-pg/imdb
PYTHON="../.venv/bin/python3"
export PYTHONPATH=/scratch/agentic-sql-pg

LOG=sessions/scenario6_retry.log

echo "=== Scenario 6 retry (tweaked prompt) ===" | tee -a $LOG
echo "Started at $(date)" | tee -a $LOG

echo "=== Rep a starting at $(date) ===" | tee -a $LOG
$PYTHON -u scenario_agent.py --scenario 6 --rep a 2>&1 | tee -a $LOG

$PYTHON -u scenario_report.py --scenario 6 2>&1 | tee -a $LOG

echo "Sleeping 120s..." | tee -a $LOG
sleep 120

echo "=== Rep b starting at $(date) ===" | tee -a $LOG
$PYTHON -u scenario_agent.py --scenario 6 --rep b 2>&1 | tee -a $LOG

$PYTHON -u scenario_report.py --scenario 6 2>&1 | tee -a $LOG

echo "=== Scenario 6 retry complete at $(date) ===" | tee -a $LOG
