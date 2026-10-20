-- include/sql/staging/stg_stores_view.sql
-- ELT layer 2: STAGING (assignment Part B) - stores
CREATE OR REPLACE VIEW v_stg_stores AS
WITH source_rows AS (
    SELECT store_id, store_name, zone, area, pincode, open_date, manager_name,
           capacity_orders_per_day,
           max(_loaded_at) AS _loaded_at, arg_max(_source_file, _loaded_at) AS _source_file,
           arg_max(_run_id, _loaded_at)      AS _run_id, min(_source_row_num) AS _source_row_num,
           count(*) AS _raw_copies
    FROM raw_stores
    GROUP BY store_id, store_name, zone, area, pincode, open_date, manager_name,
             capacity_orders_per_day
),
versioned AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(store_id)
                              ORDER BY _loaded_at DESC, _source_row_num DESC) AS _version_rank
    FROM source_rows
),
typed AS (
    SELECT
        nullif(trim(store_id), '')                                   AS store_id,
        nullif(trim(store_name), '')                                 AS store_name,
        upper(substr(nullif(trim(zone), ''), 1, 1))
            || lower(substr(nullif(trim(zone), ''), 2))              AS zone_raw,
        upper(substr(nullif(trim(area), ''), 1, 1))
            || lower(substr(nullif(trim(area), ''), 2))              AS area,
        nullif(trim(pincode), '')                                    AS pincode,
        try_cast(open_date AS DATE)                                  AS open_date,
        nullif(trim(manager_name), '')                               AS manager_name,
        try_cast(capacity_orders_per_day AS DOUBLE)                  AS capacity_orders_per_day,
        _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
    FROM versioned
),
checked AS (
    SELECT *,
        store_id IS NULL                                             AS chk_rule_046,
        zone_raw IS NULL                                             AS chk_rule_030,
        (zone_raw IS NOT NULL AND zone_raw NOT IN ('North', 'South', 'East', 'West'))
                                                                     AS chk_rule_045,
        (capacity_orders_per_day IS NOT NULL AND capacity_orders_per_day <= 0)
                                                                     AS chk_rule_031,
        (open_date IS NOT NULL AND open_date > current_date)         AS chk_rule_048
    FROM typed
)
SELECT
    store_id, store_name,
    coalesce(zone_raw, 'Unknown')                                    AS zone,
    area, pincode, open_date,
    -- the store manager's name is personal data, so it is masked here too
    CASE WHEN manager_name IS NULL THEN NULL
         WHEN position(' ' IN manager_name) = 0 THEN manager_name
         ELSE split_part(manager_name, ' ', 1) || ' '
              || upper(substr(split_part(manager_name, ' ', 2), 1, 1)) || '.'
    END                                                              AS manager_name_masked,
    capacity_orders_per_day,
    chk_rule_046, chk_rule_030, chk_rule_045, chk_rule_031, chk_rule_048,
    CASE WHEN _version_rank > 1 THEN 'SUPERSEDED'
         WHEN chk_rule_046 THEN 'QUARANTINED'
         ELSE 'PASS' END                                             AS dq_status,
    concat_ws(' | ',
        CASE WHEN _version_rank > 1 THEN 'RULE-029: store_id must be unique (superseded copy) [Uniqueness/Critical]' END,
        CASE WHEN chk_rule_046 THEN 'RULE-046: store_id must be present [Completeness/Critical]' END
    )                                                                AS failure_reason,
    concat_ws(' | ',
        CASE WHEN chk_rule_030 THEN 'RULE-030: zone should not be blank [Completeness/Warning]' END,
        CASE WHEN chk_rule_045 THEN 'RULE-045: zone must be one of North/South/East/West [Validity/Warning]' END,
        CASE WHEN chk_rule_031 THEN 'RULE-031: capacity_orders_per_day must be greater than zero [Accuracy/Warning]' END,
        CASE WHEN chk_rule_048 THEN 'RULE-048: open_date must not be future-dated [Timeliness/Warning]' END
    )                                                                AS dq_warnings,
    _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
FROM checked;
