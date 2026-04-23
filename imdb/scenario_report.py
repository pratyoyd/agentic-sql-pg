#!/usr/bin/env python3
"""Generic scenario report. Usage: python scenario_report.py --scenario N"""

import argparse
from scenario_common import SCENARIOS, generate_scenario_report, generate_aggregate_report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", type=int, required=True, choices=list(range(2, 11)) + [0])
    args = parser.parse_args()

    if args.scenario == 0:
        generate_aggregate_report()
    else:
        generate_scenario_report(args.scenario)


if __name__ == "__main__":
    main()
