"""Run the six business questions and print them as markdown.

    python -m nammamart.business_questions            # everything
    python -m nammamart.business_questions q4_elt     # one block

The output is pasted into SUBMISSION.md, so no number in that file is typed by hand.
"""

from __future__ import annotations

import sys

from . import config as cfg
from . import warehouse as wh

QUERY_FILE = cfg.SQL_DIR / "business_questions.sql"


def load_blocks() -> dict[str, str]:
    blocks: dict[str, str] = {}
    name = None
    buffer: list[str] = []
    for line in QUERY_FILE.read_text().splitlines():
        if line.startswith("-- name:"):
            if name:
                blocks[name] = "\n".join(buffer).strip()
            name = line.split(":", 1)[1].strip()
            buffer = []
        elif name:
            buffer.append(line)
    if name:
        blocks[name] = "\n".join(buffer).strip()
    return blocks


def to_markdown(df) -> str:
    if df.empty:
        return "_(no rows)_"
    header = "| " + " | ".join(str(c) for c in df.columns) + " |"
    rule = "| " + " | ".join("---" for _ in df.columns) + " |"
    rows = ["| " + " | ".join("" if v is None else str(v) for v in row) + " |"
            for row in df.itertuples(index=False)]
    return "\n".join([header, rule] + rows)


def main() -> None:
    wanted = sys.argv[1:]
    blocks = load_blocks()
    with wh.connect() as con:
        for name, sql in blocks.items():
            if wanted and name not in wanted:
                continue
            print(f"\n#### `{name}`\n")
            print("```sql")
            print(sql)
            print("```\n")
            try:
                print(to_markdown(con.execute(sql).df()))
            except Exception as exc:                     # noqa: BLE001
                print(f"_query failed: {exc}_")
            print()


if __name__ == "__main__":
    main()
