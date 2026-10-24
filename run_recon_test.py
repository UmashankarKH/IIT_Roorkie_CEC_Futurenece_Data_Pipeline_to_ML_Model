import pandas as pd
from include.etl_steps import prepare
from include.etl_reconcile import reconcile
from include.etl_load import load_dim

out = prepare(pd.Timestamp("2026-10-24 09:00"))
rec = reconcile(out["fact"], out["payments_raw"], out["all_order_ids"])

print("mismatch rows:", len(rec))
print(rec.reason.value_counts().to_string())
print("total amount not reconciling:", round(rec.difference.abs().sum(), 2))
print("loaded to DuckDB:", load_dim(rec, "etl_payment_reconciliation"))
print(rec.head(5).to_string())