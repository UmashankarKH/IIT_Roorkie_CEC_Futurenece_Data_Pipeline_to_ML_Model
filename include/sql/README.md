# SQL assets for NammaMart pipeline

| File | Purpose | Executed by |
| --- | --- | --- |
| `elt_merge_orders.sql` | Upserts deduplicated, typed order rows into `stg_orders` by `order_id`; only changed rows are updated. | ELT `transform_staging` task |
| `elt_marts.sql` | Builds the five business marts from staging tables. | ELT `build_marts` task |
| `nammamart_business_questions.sql` | Six analytics questions, with ELT and ETL query variants. | Run manually against the DuckDB warehouse |
| `business_question_results.md` | Snapshot of the six query results from the successful 20 October pipeline runs, including known ETL/ELT differences. | Submission evidence |

The customer, product, store and payment staging SQL is currently defined in the
ELT DAG's `transform_staging` task; it has not yet been extracted into separate
SQL files.
