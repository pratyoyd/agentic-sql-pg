#!/usr/bin/env python3
"""
Stage 2 sweep: run Postgres-dialect agent on all 31 tasks, 1 rep each.
Resumable via manifest.json.
"""

import json
import time
import traceback
from pathlib import Path

from pg_agent import run_session
from pg_loader import ALL_TASKS

MANIFEST_PATH = Path("stage2/manifest.json")
LOG_DIR = "stage2/traces"


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.load(open(MANIFEST_PATH))
    return {}


def save_manifest(manifest: dict):
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    manifest = load_manifest()

    tasks = ALL_TASKS
    total = len(tasks)

    for idx, task in enumerate(tasks):
        session_id = f"{task}_rep_a"

        # Skip if already done
        if session_id in manifest and manifest[session_id].get("status") == "done":
            nq = manifest[session_id].get("num_queries", 0)
            print(f"[{idx+1}/{total}] {task}: already done ({nq} queries), skipping")
            continue

        print(f"\n[{idx+1}/{total}] {task}: starting...")
        manifest[session_id] = {"task": task, "rep": "a", "status": "running"}
        save_manifest(manifest)

        try:
            summary = run_session(task, log_dir=LOG_DIR, session_id_override=session_id)
            manifest[session_id] = {
                "task": task,
                "rep": "a",
                "status": "done",
                "num_queries": summary["num_queries"],
                "wall_clock_seconds": summary["wall_clock_seconds"],
                "log_path": summary["log_file"],
            }
        except Exception as e:
            print(f"ERROR on {task}: {e}")
            traceback.print_exc()
            manifest[session_id] = {
                "task": task,
                "rep": "a",
                "status": "failed",
                "error": str(e)[:500],
            }

        save_manifest(manifest)

    # Summary
    done = sum(1 for v in manifest.values() if v.get("status") == "done")
    failed = sum(1 for v in manifest.values() if v.get("status") == "failed")
    total_q = sum(v.get("num_queries", 0) for v in manifest.values() if v.get("status") == "done")
    print(f"\n{'='*60}")
    print(f"Sweep complete: {done}/{total} done, {failed} failed, {total_q} total queries")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
