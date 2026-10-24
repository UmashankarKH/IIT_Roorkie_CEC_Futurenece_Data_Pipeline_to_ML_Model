"""Report tables for the COO. Delivered orders only, net of discount."""
import pandas as pd


def daily_zone_revenue(fact, dim_store):
    d = fact.merge(dim_store[["store_id", "store_name", "zone"]], on="store_id", how="left")
    d["zone"] = d["zone"].fillna("UNKNOWN")           # store row was quarantined
    d["store_name"] = d["store_name"].fillna("UNKNOWN")
    d = d[d.order_status == "DELIVERED"]
    return (d.groupby(["order_date", "zone", "store_id", "store_name"], as_index=False)
              .agg(delivered_orders=("order_id", "count"), net_revenue=("net_revenue", "sum"))
              .round(2))


def category_margin(fact, dim_product):
    d = fact.merge(dim_product[["product_id", "category"]], on="product_id", how="left")
    d["category"] = d["category"].fillna("UNKNOWN")   # product row was quarantined
    d = d[d.order_status == "DELIVERED"]
    out = (d.groupby("category", as_index=False)
             .agg(delivered_orders=("order_id", "count"),
                  net_revenue=("net_revenue", "sum"),
                  gross_margin=("gross_margin", "sum")))
    out["margin_pct"] = (100 * out["gross_margin"] / out["net_revenue"]).round(1)
    out.loc[out.category == "UNKNOWN", "margin_pct"] = None   # cost price unknown
    return out.round(2)