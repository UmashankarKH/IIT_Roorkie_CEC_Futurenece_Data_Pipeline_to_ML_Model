# Recommendation to Sameer, CTO: adopt ELT with a controlled finance contract

## Decision

NammaMart should adopt **ELT as the primary warehouse architecture**, with a validated finance-facing contract that publishes only approved rows and metrics. ELT is the better system of record because it preserves every source row, makes transformations replayable, and keeps rule changes in version-controlled SQL. ETL should remain available as a strict publishing step for finance-critical tables, where rejected records must never enter revenue reporting.

## Evidence from the completed work

On the September files both pipelines publish identical finance numbers: 1,903 clean orders, ₹932,884.15 delivered net revenue, ₹228,083.65 gross margin and 629 order/payment mismatches. After the 1 October increment both show 2,029 orders (150 inserted, 10 corrected to RETURNED, September revenue down ₹3,579.70). ETL rejects 497 orders before loading and quarantines each with a reason; ELT keeps all 2,412 raw order rows (2,572 after the increment) and marks the same 497 as FAIL in staging. Each run records 38 quality checks, 31 of which fail on this data.

The most useful observation came from reconciling the two: the first versions disagreed by 467 orders because the Python and SQL copies of the same rules had drifted (ELT silently dropped duplicates; ETL quarantined `upi` instead of standardising it). Two implementations of one rule set will drift again, which is a strong argument for keeping the rules in one place.

## Comparison

**Auditability.** ELT is stronger. Raw tables retain source values as received together with `_source_file`, `_loaded_at`, and `_run_id`. A reviewer can replay a transformation or compare the original and corrected version of an order. ETL retains quarantine reasons and a clean fact table, which is excellent for proving that rejected rows did not reach finance, but discarded or transformed source context is less convenient to reconstruct. ELT therefore becomes the audit system of record; ETL remains the publishable finance view.

**Data-quality guarantees.** ETL is stronger at the publication boundary. Its Python checks quarantine critical rows before fact loading, so the finance fact table has an explicit acceptance guarantee. ELT keeps bad rows in raw storage and marks them in staging, which is safer for audit but requires every mart query to filter `dq_status` correctly. The recommendation is to enforce tested staging views and a finance contract that rejects any unapproved status, rather than relying on analysts to remember the filter.

**Ease of changing rules.** ELT is easier to change. A new business rule can be implemented in a SQL transformation and replayed over immutable raw data without recollecting source files. ETL changes are also testable, but they require Python code changes and careful handling of quarantine and downstream joins. In either design, quality rules must be version-controlled, reviewed by the business owner, and covered by regression tests because a rule change can change historical totals.

**Performance at 1,000× volume.** ELT is the better long-term scaling choice if the warehouse is sized for the workload. Set-based SQL, partitioning, incremental models, and warehouse parallelism avoid moving all records through one Python worker. ETL can scale with distributed processing, but the current pandas-style implementation would become a bottleneck and would need a larger redesign. At 1,000× volume, ELT should process only changed partitions and rebuild affected marts rather than scan every raw row on every run.

**Cost.** ELT avoids maintaining a separate heavy transformation tier and makes better use of warehouse compute, but it can increase warehouse storage and query costs because raw history is retained. ETL can be cheaper for small local files and predictable batch jobs, but Python workers, temporary storage, and operational maintenance become more expensive as volume and rule complexity grow. Cost controls should include partitioning, incremental SQL, retention policies, and workload monitoring.

**Operational risks.** ELT’s main operational risk is that a poorly written or unfiltered SQL model can expose bad rows to a mart. It also increases the importance of warehouse permissions, lineage, tests, and freshness/volume alerts. ETL’s risks are silent data loss if quarantine is not monitored, bottlenecks at scale, and more difficult replay after a rule change. The proposed contract addresses the ELT risk: raw remains complete, staging records the quality result, and finance reads only certified views.

## Implementation conditions

Before production, require schema checks, freshness and volume thresholds, distribution monitoring, row-level lineage, masked customer fields in every reporting table, and alerts for failed or quarantined runs. Keep raw customer data restricted, store credentials only in Airflow Connections or the deployment secret store, and retain both the original and corrected order versions for returned-order investigations.

## Three biggest risks of this recommendation

1. **Bad data reaches a mart** if a model forgets to filter `dq_status`; mitigate with certified finance views, SQL tests, and restricted write access.
2. **Warehouse cost and latency grow** at high volume; mitigate with partitioning, incremental models, retention controls, and workload monitoring.
3. **Raw PII is overexposed** because ELT retains complete source data; mitigated today by isolating raw customer data in an owner-only DuckDB file and masking every shared table and quarantine file, and in production by role-based access, secret management, and audit logs.
