#!/usr/bin/env python
"""Run the NammaMart ELT pipeline outside Airflow.

Usage:
    python scripts/run_elt_local.py              # raw + staging + mart
    python scripts/run_elt_local.py --increment  # also load the 1 Oct increment and MERGE
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.nammamart import elt_pipeline  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the NammaMart ELT pipeline locally")
    parser.add_argument("--run-id", default=None, help="run identifier used in audit tables")
    parser.add_argument("--increment", action="store_true", help="include the next-day orders file")
    parser.add_argument("--data-dir", default=None, help="override the source data folder")
    parser.add_argument("--db", default=None, help="override the DuckDB warehouse path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    summary = elt_pipeline.run_elt(
        run_id=args.run_id,
        data_dir=Path(args.data_dir) if args.data_dir else None,
        db_path=Path(args.db) if args.db else None,
        include_increment=args.increment,
    )
    print("\n=== ELT run summary ===")
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
