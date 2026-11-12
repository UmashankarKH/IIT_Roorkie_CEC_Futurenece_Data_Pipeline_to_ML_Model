-- NammaMart ELT: helper macros, created at the start of every SQL task.
-- They mirror standardise() in include/dq_rules.py exactly, so ETL and ELT clean values the same way.

-- Trim all whitespace (DuckDB's trim() only removes spaces; Python's strip() removes tabs/newlines too)
CREATE OR REPLACE TEMP MACRO nm_strip(x) AS regexp_replace(x, '^\s+|\s+$', '', 'g');

-- Original value, or NULL when blank or a placeholder such as 'NULL', 'N/A', '-'
CREATE OR REPLACE TEMP MACRO nm_raw(x) AS
    CASE WHEN x IS NULL OR lower(nm_strip(x)) IN ('', 'null', 'none', 'nan', 'na', 'n/a', '-') THEN NULL ELSE x END;

CREATE OR REPLACE TEMP MACRO nm_text(x) AS nm_strip(nm_raw(x));                  -- trimmed text or NULL
CREATE OR REPLACE TEMP MACRO nm_num(x)  AS TRY_CAST(nm_text(x) AS DOUBLE);       -- number or NULL
CREATE OR REPLACE TEMP MACRO nm_ts(x)   AS TRY_CAST(nm_text(x) AS TIMESTAMP);    -- ISO timestamp or NULL ('31/09/2026' -> NULL)
CREATE OR REPLACE TEMP MACRO nm_key(x)  AS regexp_replace(lower(nm_text(x)), '\s+', ' ', 'g');

-- Canonical category values; NULL means "not in the allowed list"
CREATE OR REPLACE TEMP MACRO nm_payment_mode(x) AS CASE nm_key(x)
    WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cash on delivery' THEN 'Cash on Delivery'
    WHEN 'cod' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' END;      -- 'Cashh' deliberately unmapped (DI-08)
CREATE OR REPLACE TEMP MACRO nm_order_status(x) AS CASE nm_key(x)
    WHEN 'delivered' THEN 'DELIVERED' WHEN 'cancelled' THEN 'CANCELLED' WHEN 'returned' THEN 'RETURNED' END;
CREATE OR REPLACE TEMP MACRO nm_payment_status(x) AS CASE nm_key(x)
    WHEN 'success' THEN 'SUCCESS' WHEN 'failed' THEN 'FAILED' WHEN 'refunded' THEN 'REFUNDED' END;
CREATE OR REPLACE TEMP MACRO nm_tier(x) AS CASE nm_key(x)
    WHEN 'bronze' THEN 'Bronze' WHEN 'silver' THEN 'Silver' WHEN 'gold' THEN 'Gold' WHEN 'platinum' THEN 'Platinum' END;
CREATE OR REPLACE TEMP MACRO nm_zone(x) AS CASE nm_key(x)
    WHEN 'north' THEN 'North' WHEN 'south' THEN 'South' WHEN 'east' THEN 'East' WHEN 'west' THEN 'West' END;
CREATE OR REPLACE TEMP MACRO nm_bool(x) AS CASE lower(nm_strip(x))
    WHEN 'y' THEN TRUE WHEN 'yes' THEN TRUE WHEN 'true' THEN TRUE WHEN 't' THEN TRUE WHEN '1' THEN TRUE
    WHEN 'n' THEN FALSE WHEN 'no' THEN FALSE WHEN 'false' THEN FALSE WHEN 'f' THEN FALSE WHEN '0' THEN FALSE END;

-- Phone: digits only, then drop a leading 91 (12 digits) or 0 (11 digits)
CREATE OR REPLACE TEMP MACRO nm_drop91(d) AS CASE WHEN length(d) = 12 AND d LIKE '91%' THEN substr(d, 3) ELSE d END;
CREATE OR REPLACE TEMP MACRO nm_drop0(d)  AS CASE WHEN length(d) = 11 AND d LIKE '0%'  THEN substr(d, 2) ELSE d END;
CREATE OR REPLACE TEMP MACRO nm_phone_norm(x) AS nm_drop0(nm_drop91(regexp_replace(nm_text(x), '[^0-9]', '', 'g')));

-- Masking (same output as mask_email / mask_phone / mask_name in include/nammamart_etl.py)
CREATE OR REPLACE TEMP MACRO nm_mask_email(e) AS CASE
    WHEN e IS NULL THEN NULL
    WHEN strpos(e, '@') = 0 THEN '***'
    ELSE left(e, least(2, strpos(e, '@') - 1)) || '***@' || substr(e, strpos(e, '@') + 1) END;   -- pr***@example.com
CREATE OR REPLACE TEMP MACRO nm_mask_phone(d) AS CASE
    WHEN d IS NULL OR d = '' THEN NULL
    WHEN length(d) < 6 THEN repeat('*', length(d))
    ELSE left(d, 2) || repeat('*', length(d) - 4) || right(d, 2) END;                          -- 98******21
CREATE OR REPLACE TEMP MACRO nm_mask_name(n) AS CASE
    WHEN n IS NULL THEN NULL
    ELSE regexp_replace(regexp_replace(n, '(\S)\S*', '\1***', 'g'), '\s+', ' ', 'g') END;      -- P*** S***
