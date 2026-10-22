"""Print row counts of the key warehouse tables (used to record the experiments)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))
import nammamart_common as nc  # noqa: E402
c = nc.connect(read_only=True)
out = []
for t in ["etl_fact_orders", "etl_dim_customer", "etl_quarantine_log", "etl_payment_reconciliation", "raw_orders", "raw_payments",
          "raw_customers", "stg_orders", "mart_daily_store_kpis", "mart_payment_reconciliation", "mart_rejected_rows",
          "dq_results", "pipeline_runs"]:
    try:
        out.append(f"{t}={c.execute(f'select count(*) from {t}').fetchone()[0]}")
    except Exception:
        out.append(f"{t}=-")
print(" ".join(out))
for q in ["select round(sum(net_revenue),2) from etl_fact_orders", "select round(sum(net_revenue),2) from mart_payment_mode_revenue"]:
    try:
        print(q.split('from')[1].strip(), c.execute(q).fetchone()[0])
    except Exception:
        pass
