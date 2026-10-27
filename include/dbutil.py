"""Small database helpers shared by both NammaMart DAGs (include/dbutil.py).

1. connect(): DuckDB lets only ONE process write to a file at a time. The ETL and ELT DAGs
   are both scheduled for 9:00 AM, so one of them may find the file busy. Instead of failing,
   we wait a few seconds and try again (up to ~3 minutes).
2. split_statements(): cuts a .sql file into single statements. It is aware of quotes and
   comments, so a ';' inside text such as concat_ws('; ', ...) is NOT treated as an end.
3. run_sql_file(): fills in {{placeholders}} and runs every statement in a file.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

log = logging.getLogger("airflow.task")


def connect(path, attempts: int = 60, wait_seconds: float = 3.0):
    import duckdb

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    last = None
    for i in range(1, attempts + 1):
        try:
            return duckdb.connect(str(path))
        except duckdb.IOException as exc:          # "Could not set lock on file ..."
            text = str(exc).lower()
            if "lock" not in text and "conflicting" not in text:
                raise
            last = exc
            if i == 1:
                log.info("warehouse file is busy (another pipeline is writing); waiting to retry...")
            time.sleep(wait_seconds)
    raise RuntimeError(f"warehouse file still locked after {attempts * wait_seconds:.0f}s: {last}")


def split_statements(text: str) -> list[str]:
    out, cur, in_quote, i = [], [], False, 0
    while i < len(text):
        ch = text[i]
        if in_quote:
            cur.append(ch)
            if ch == "'":
                in_quote = False            # a doubled '' closes and re-opens, which is correct
        elif ch == "-" and text[i:i + 2] == "--":
            j = text.find("\n", i)
            i = len(text) if j < 0 else j
            continue
        elif ch == "'":
            in_quote = True
            cur.append(ch)
        elif ch == ";":
            stmt = "".join(cur).strip()
            if stmt:
                out.append(stmt)
            cur = []
        else:
            cur.append(ch)
        i += 1
    tail = "".join(cur).strip()
    if tail:
        out.append(tail)
    return out


def render(text: str, /, **params) -> str:
    for key, value in params.items():
        text = text.replace("{{" + key + "}}", str(value))
    return text


def run_sql_file(con, sql_file, /, **params) -> None:
    text = render(Path(sql_file).read_text(encoding="utf-8"), **params)
    for stmt in split_statements(text):
        con.execute(stmt)
