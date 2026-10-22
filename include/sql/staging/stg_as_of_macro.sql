-- include/sql/staging/stg_as_of_macro.sql
-- ELT layer 2: STAGING - reference date for the "must not be future-dated" rules.
--
-- The staging views call as_of() instead of now() / current_date, so the result
-- of a run does not depend on the day it is executed. The same cut-off is used by
-- the ETL rules (config.AS_OF). RULE-035 (freshness) still uses the real clock.
CREATE OR REPLACE MACRO as_of() AS TIMESTAMP '{as_of}';
