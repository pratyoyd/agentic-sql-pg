#!/bin/bash
# Run IMDB experiment tasks 2-5, each with 3 reps, sequentially.
# Safe to run via nohup — all output goes to per-task log files.
set -e

cd /scratch/agentic-sql-pg/imdb
export PYTHONPATH=/scratch/agentic-sql-pg
PYTHON=../.venv/bin/python3

echo "=== IMDB experiment: tasks 2-5 ==="
echo "Started: $(date)"
echo ""

for TASK in task2 task3 task4 task5; do
    echo "============================================"
    echo "Starting ${TASK} at $(date)"
    echo "============================================"

    for REP in a b c; do
        echo "--- ${TASK} rep ${REP} ---"
        $PYTHON -u imdb_agent.py --task ${TASK} --rep ${REP} \
            > ${TASK}_rep${REP}.log 2>&1

        # Check if summary was produced
        SUMMARY="traces/${TASK}_rep_${REP}_summary.json"
        if [ -f "$SUMMARY" ]; then
            NQUERIES=$(python3 -c "import json; print(json.load(open('${SUMMARY}'))['num_queries'])")
            WALL=$(python3 -c "import json; print(json.load(open('${SUMMARY}'))['wall_clock_seconds'])")
            echo "  Completed: ${NQUERIES} queries in ${WALL}s"
        else
            echo "  WARNING: no summary produced for ${TASK} rep ${REP}"
        fi
    done

    # Post-process this task
    echo "Post-processing ${TASK}..."
    $PYTHON -u post_process_task.py ${TASK} 2>&1

    echo "${TASK} complete at $(date)"
    echo ""
done

# Also post-process task1 (already completed)
echo "Post-processing task1..."
$PYTHON -u post_process_task.py task1 2>&1

echo ""
echo "=== All tasks complete at $(date) ==="
echo ""

# Print summary table
echo "Task summaries:"
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
