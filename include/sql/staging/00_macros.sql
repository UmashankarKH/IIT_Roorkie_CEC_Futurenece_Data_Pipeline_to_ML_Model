-- Reusable SQL macros for the staging layer (same rules as include/nammamart_divya).

-- '  upi ' -> 'UPI', 'cod' / 'Cashh' -> 'Cash on Delivery', 'CARD' -> 'Card'
CREATE OR REPLACE MACRO std_payment_mode(v) AS
    CASE lower(trim(v))
        WHEN 'upi' THEN 'UPI'
        WHEN 'card' THEN 'Card'
        WHEN 'cash on delivery' THEN 'Cash on Delivery'
        WHEN 'cod' THEN 'Cash on Delivery'
        WHEN 'cashh' THEN 'Cash on Delivery'
        WHEN 'wallet' THEN 'Wallet'
        ELSE nullif(trim(v), '')
    END;

-- priya.hegde199@example.com -> pr***@example.com ; malformed -> ***
CREATE OR REPLACE MACRO mask_email(e) AS
    CASE
        WHEN e IS NULL THEN NULL
        WHEN strpos(e, '@') = 0 THEN '***'
        ELSE left(split_part(e, '@', 1), 2) || '***@' || substr(e, strpos(e, '@') + 1)
    END;

-- 9774349021 -> 97******21
CREATE OR REPLACE MACRO mask_phone(p) AS
    CASE
        WHEN p IS NULL THEN NULL
        WHEN length(p) <= 4 THEN repeat('*', length(p))
        ELSE left(p, 2) || repeat('*', length(p) - 4) || right(p, 2)
    END;

-- Priya Hegde -> Priya H.
CREATE OR REPLACE MACRO mask_name(n) AS
    CASE
        WHEN n IS NULL THEN NULL
        WHEN len(string_split(n, ' ')) > 1
            THEN string_split(n, ' ')[1] || ' ' || left(string_split(n, ' ')[-1], 1) || '.'
        ELSE n
    END;
