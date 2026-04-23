#!/usr/bin/env python3
"""Generic scenario agent. Usage: python scenario_agent.py --scenario N --rep a"""

import argparse
import psycopg
from scenario_common import SCENARIOS, CONNINFO, get_schema_description, run_session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", type=int, required=True, choices=range(2, 11))
    parser.add_argument("--rep", type=str, required=True)
    args = parser.parse_args()

    sc = SCENARIOS[args.scenario]
    if args.rep not in sc["reps"]:
        raise ValueError(f"Rep {args.rep} not in {sc['reps']} for scenario {args.scenario}")

    print(f"Loading schema description...")
    conn = psycopg.connect(CONNINFO, autocommit=True)
    schema_desc = get_schema_description(conn)
    conn.close()
    print(f"Schema description: {len(schema_desc)} chars")

    run_session(args.scenario, args.rep, schema_desc)


if __name__ == "__main__":
    main()
