import pandas as pd
from include.dq_checks import CHECKSETS, run_checks
from include.etl_transform import build_dim_customer, build_dim_product, build_dim_store
from include.nammamart_config import DATA_DIR

NOW = pd.Timestamp("2026-10-24 09:00")
rd = lambda f: pd.read_csv(DATA_DIR / f, dtype=str, keep_default_na=False)

for name, builder in [("customers", build_dim_customer), ("products", build_dim_product),
                      ("stores", build_dim_store)]:
    passed, failed, _ = run_checks(rd(f"{name}.csv"), CHECKSETS[name]({"now": NOW}), name, "t", "r1", NOW)
    dim = builder(passed)
    print(f"\n=== {name}: {len(dim)} rows ===")
    print(dim.head(3).to_string())