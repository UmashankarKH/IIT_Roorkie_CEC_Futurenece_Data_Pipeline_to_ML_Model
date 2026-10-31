"""Schema contract: the columns every source file must have, in this order."""
EXPECTED = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
               "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
               "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                  "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                 "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date",
               "manager_name", "capacity_orders_per_day"],
}


class SchemaError(ValueError):
    pass


def assert_schema(name, columns):
    got = [str(c).strip() for c in columns]
    want = EXPECTED[name]
    if got == want:
        return
    missing = [c for c in want if c not in got]
    extra = [c for c in got if c not in want]
    if not missing and not extra:
        raise SchemaError(f"{name}: same columns but a different order {got}")
    raise SchemaError(f"{name}: missing columns {missing}, unexpected columns {extra}")
