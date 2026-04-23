#!/bin/bash
set -e

cd /scratch/agentic-sql-pg/imdb
PYTHON="../.venv/bin/python3"
SCRIPT="e2e_task2_rep_a/run_full_replay.py"
export PYTHONPATH=/scratch/agentic-sql-pg

echo "=== Step 1: Restart Postgres for baseline ==="
pg_ctlcluster 16 poc restart
sleep 3

echo "=== Step 2: Run baseline ==="
$PYTHON -u $SCRIPT --baseline 2>&1 | tee e2e_task2_rep_a/baseline_run.log

echo "=== Step 3: Restart Postgres for hinted ==="
pg_ctlcluster 16 poc restart
sleep 3

echo "=== Step 4: Run hinted + analysis ==="
$PYTHON -u $SCRIPT --hinted 2>&1 | tee e2e_task2_rep_a/hinted_run.log

echo "=== DONE ==="
echo "Report at: e2e_task2_rep_a/task2_rep_a_e2e_report.md"
