"""
include/nammamart_smit/files.py
-------------------------------
Reading source files, used by both DAGs.
"""

import os
from io import StringIO

import pandas as pd
import pendulum

from include.nammamart_smit.config import DATA_DIR, INCREMENT_PATTERN, SOURCES


def check_schema(columns, source, file_name):
    """Fail loudly if a column was renamed, added or dropped (schema change)."""
    expected = SOURCES[source]
    missing = [c for c in expected if c not in columns]
    unexpected = [c for c in columns if c not in expected]
    if missing or unexpected:
        raise ValueError(
            f"[Schema] {file_name} does not match the agreed contract. "
            f"Missing columns: {missing}. Unexpected columns: {unexpected}."
        )


def read_source(file_name, source):
    """Read a CSV exactly as received: every value as text, blanks kept as ''."""
    path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"[Extract] Source file not found: {path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    check_schema(list(df.columns), source, file_name)
    print(f"[Extract] {file_name}: {len(df)} rows, {len(df.columns)} columns")
    return df


def pick_increment_files(param_value, run_after, already_loaded):
    """
    Which next-day order files should this run load?
      - Manual run with the 'increment_file' param: just that file.
      - Scheduled run: every orders_<date>_increment.csv dated before today that
        this pipeline hasn't loaded yet. The DAG runs Monday to Saturday, so
        Monday's run picks up Saturday's and Sunday's files, and a day missed
        because the laptop was off is picked up by the next run.
    """
    if param_value:
        if not os.path.exists(os.path.join(DATA_DIR, param_value)):
            raise FileNotFoundError(f"[Increment] {param_value} not found in {DATA_DIR}")
        return [param_value]

    today = pendulum.instance(run_after).in_timezone("Asia/Kolkata").to_date_string()
    files = []
    for file_name in sorted(os.listdir(DATA_DIR)):
        match = INCREMENT_PATTERN.match(file_name)
        if match and match.group(1) < today and file_name not in already_loaded:
            files.append(file_name)

    print(f"[Increment] files to load this run: {files or 'none'}")
    return files


def to_json(df):
    return df.to_json(orient="records")


def from_json(text):
    # dtype=False / convert_dates=False so values come back exactly as they went in
    return pd.read_json(StringIO(text), orient="records", dtype=False, convert_dates=False)
