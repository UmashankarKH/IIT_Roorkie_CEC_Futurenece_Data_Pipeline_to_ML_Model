import pandas as pd
from include.etl_steps import prepare
from include.etl_quarantine import write_quarantine

out = prepare(pd.Timestamp("2026-10-24 09:00"))
written = write_quarantine(out["failed"], "local")

for name, (path, n) in written.items():
    print(f"{name:10} {n:4} rows -> {path.name}")

q = pd.read_csv(written["orders"][0], dtype=str)
print()
print(q.failure_reason.value_counts().to_string())