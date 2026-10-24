import pandas as pd
from include.etl_steps import prepare
from include.etl_reports import daily_zone_revenue, category_margin
from include.etl_load import load_dim

out = prepare(pd.Timestamp("2026-10-24 09:00"))
zone = daily_zone_revenue(out["fact"], out["dim_store"])
cat = category_margin(out["fact"], out["dim_product"])

print("zone table rows:", load_dim(zone, "etl_daily_zone_revenue"))
print("category table rows:", load_dim(cat, "etl_category_margin"))
print()
print("Check: sum of zone revenue =", round(zone.net_revenue.sum(), 2),
      "| fact net revenue =", round(out["fact"].net_revenue.sum(), 2))
print()
print(cat.to_string(index=False))
print()
print(zone.groupby("zone").net_revenue.sum().round(2).to_string())