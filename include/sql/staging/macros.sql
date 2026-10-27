-- Helpers used by the staging models. DuckDB has no initcap() and no masking
-- functions, and I would rather define these once than paste the same CASE
-- expression into five files.
--
-- The three mask_* macros mirror transforms.py exactly, because the ETL and ELT
-- customer dimensions are compared row for row in Part G.

CREATE OR REPLACE MACRO title_case(s) AS
    CASE WHEN s IS NULL THEN NULL
         ELSE array_to_string(
                  list_transform(string_split(s, ' '),
                      lambda w: CASE WHEN length(w) = 0 THEN w
                                ELSE upper(substr(w, 1, 1)) || lower(substr(w, 2)) END),
                  ' ')
    END;

-- Priya Hegde -> Priya H.
CREATE OR REPLACE MACRO mask_name(s) AS
    CASE WHEN s IS NULL OR trim(s) = '' THEN NULL
         WHEN len(regexp_split_to_array(trim(s), '\s+')) = 1 THEN trim(s)
         ELSE regexp_split_to_array(trim(s), '\s+')[1] || ' '
              || substr(list_extract(regexp_split_to_array(trim(s), '\s+'), -1), 1, 1) || '.'
    END;

-- priya.hegde199@example.com -> pr***@example.com. The domain stays so support can
-- still tell a gmail customer from a corporate one.
CREATE OR REPLACE MACRO mask_email(s) AS
    CASE WHEN s IS NULL OR trim(s) = '' THEN NULL
         WHEN position('@' IN s) = 0 THEN '***'
         WHEN length(split_part(s, '@', 1)) > 2
              THEN substr(split_part(s, '@', 1), 1, 2) || '***@' || split_part(s, '@', 2)
         ELSE substr(split_part(s, '@', 1), 1, 1) || '***@' || split_part(s, '@', 2)
    END;

-- 9774349021 -> 97******21. Enough to match a call log, not enough to dial.
CREATE OR REPLACE MACRO mask_phone(s) AS
    CASE WHEN regexp_replace(coalesce(s, ''), '[^0-9]', '', 'g') = '' THEN '***'
         WHEN length(regexp_replace(s, '[^0-9]', '', 'g')) < 4
              THEN repeat('*', length(regexp_replace(s, '[^0-9]', '', 'g')))
         ELSE substr(regexp_replace(s, '[^0-9]', '', 'g'), 1, 2)
              || repeat('*', length(regexp_replace(s, '[^0-9]', '', 'g')) - 4)
              || substr(regexp_replace(s, '[^0-9]', '', 'g'),
                        length(regexp_replace(s, '[^0-9]', '', 'g')) - 1, 2)
    END;

-- The payment mode spellings the profile found, mapped onto the four allowed modes.
CREATE OR REPLACE MACRO standard_payment_mode(s) AS
    CASE lower(trim(s))
        WHEN 'upi'              THEN 'UPI'
        WHEN 'u p i'            THEN 'UPI'
        WHEN 'card'             THEN 'Card'
        WHEN 'credit card'      THEN 'Card'
        WHEN 'debit card'       THEN 'Card'
        WHEN 'cod'              THEN 'Cash on Delivery'
        WHEN 'cash'             THEN 'Cash on Delivery'
        WHEN 'cashh'            THEN 'Cash on Delivery'
        WHEN 'cash on delivery' THEN 'Cash on Delivery'
        WHEN 'wallet'           THEN 'Wallet'
        ELSE NULL
    END;
