#!/bin/bash
set -e
cd /scratch/agentic-sql-pg/imdb
export PYTHONPATH=/scratch/agentic-sql-pg
PYTHON=../.venv/bin/python3

echo "=== Remaining tasks ==="
echo "Started: $(date)"

# Clean and re-run task4 rep c
echo "--- task4 rep c (restart) ---"
rm -f traces/task4_rep_c.jsonl traces/task4_rep_c_summary.json
rm -rf raw_plans/task4/c
$PYTHON -u imdb_agent.py --task task4 --rep c > task4_repc.log 2>&1
SUMMARY="traces/task4_rep_c_summary.json"
if [ -f "$SUMMARY" ]; then
    NQUERIES=$(python3 -c "import json; print(json.load(open('${SUMMARY}'))['num_queries'])")
    echo "  Completed: ${NQUERIES} queries"
else
    echo "  WARNING: no summary"
fi

# Post-process task4
echo "Post-processing task4..."
$PYTHON -u post_process_task.py task4 2>&1

# Run task5
for REP in a b c; do
    echo "--- task5 rep ${REP} ---"
    $PYTHON -u imdb_agent.py --task task5 --rep ${REP} > task5_rep${REP}.log 2>&1
    SUMMARY="traces/task5_rep_${REP}_summary.json"
    if [ -f "$SUMMARY" ]; then
        NQUERIES=$(python3 -c "import json; print(json.load(open('${SUMMARY}'))['num_queries'])")
        echo "  Completed: ${NQUERIES} queries"
    else
        echo "  WARNING: no summary"
    fi
done

# Post-process task5
echo "Post-processing task5..."
$PYTHON -u post_process_task.py task5 2>&1

echo ""
echo "=== All remaining tasks complete at $(date) ==="

# Final summary
echo ""
echo "Full summary:"
for TASK in task1 task2 task3 task4 task5; do
    for REP in a b c; do
        SUMMARY="traces/${TASK}_rep_${REP}_summary.json"
        if [ -f "$SUMMARY" ]; then
            python3 -c "
import json
s = json.load(open('${SUMMARY}'))
print(f\"  {s['session_id']:20s}  queries={s['num_queries']:2d}  wall={s['wall_clock_seconds']:6.1f}s  answer={'yes' if s.get('final_answer') else 'no'}\")
"
        fi
    done
done
