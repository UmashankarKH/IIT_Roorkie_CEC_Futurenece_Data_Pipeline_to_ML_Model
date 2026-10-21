NammaMart Business Questions — Query Results
Source: include/sql/business_questions.sql
Database: include/warehouse.duckdb

These results were obtained by running the business-question SQL against the current local DuckDB database. They reflect the available dataset and should be interpreted alongside the data-quality findings in SUBMISSION.md.
1. Daily net revenue by zone and store
The query returns delivered-order net revenue grouped by order date, zone, store ID, and store name.

Data-quality observations
- Store S07 has a missing zone, so some results show a null/NaN zone.
- The output includes some future-dated orders. Confirm the intended reporting window before using those rows for business reporting.
The full date/store-level output is available by executing Query 1 in include/sql/business_questions.sql; it is not reproduced here to keep this report concise.
2. Revenue and gross margin by category
Category	Net revenue	Gross margin
Staples	299,565.05	64,419.48
Personal Care	217,476.05	47,414.10
Household	214,647.85	54,911.21
Dairy & Bakery	181,471.60	48,048.02
Snacks & Beverages	176,718.45	41,897.99
Fruits & Vegetables	100,571.40	20,705.90
Missing category	41,911.75	4,943.39


Finding: Staples has the highest net revenue in this query output. Revenue and margin associated with a missing category remain visible rather than being silently assigned to a category.
3. Average delivery time and within-15-minute rate by store
Store ID	Store	Average delivery time (minutes)	Within 15 minutes
S01	Hebbal	20.57	31.79%
S02	Yelahanka	20.98	31.13%
S03	RT Nagar	23.89	28.57%
S04	Jayanagar	27.66	27.14%
S05	JP Nagar	21.10	30.07%
S06	Banashankari	22.61	21.29%
S07	BTM Layout	21.06	32.89%
S08	Whitefield	25.86	35.54%
S09	Indiranagar	31.49	26.28%
S10	Marathahalli	22.09	26.00%
S11	HSR Layout	20.79	28.40%
S12	Rajajinagar	25.48	24.39%
S13	Malleshwaram	24.91	26.80%
S14	Vijayanagar	21.60	30.95%
S15	Basaveshwaranagar	22.06	22.78%


Findings
- Highest average delivery time: S09 (Indiranagar), approximately 31.49 minutes.
- Lowest within-15-minute rate: S06 (Banashankari), approximately 21.29%.
- Highest within-15-minute rate: S08 (Whitefield), approximately 35.54%.
These results identify stores for further operational review; they do not establish the cause of delivery delays.
4. Delivered net revenue by payment mode
Payment mode	Net revenue
UPI	650,999.70
CARD	254,507.05
CASH_ON_DELIVERY	186,287.15
WALLET	139,902.25
CASHH	666.00


Finding: CASHH appears as an unexpected payment mode in the source data and should be reviewed rather than assumed to be an approved category.
5. Total absolute unreconciled amount
Result: 210,031.99

This is the sum of the absolute unreconciled amounts returned by the query. It is not necessarily the net financial exposure, because positive and negative differences are both counted as positive values. Finance/payment operations should investigate the underlying unreconciled records.
6. Top 10 customers by delivered-order spend
The query returns customer ID, masked email, masked phone, loyalty tier, and spend. Contact fields are retained in masked form.
Customer ID	Masked email	Masked phone	Loyalty tier	Spend
C00236	s***@example.com	******8801	Platinum	8,947.80
C00136	s***@example.com	******9688	Bronze	8,738.10
C00115	s***@example.com	******6785	Bronze	8,037.70
C00283	n***@example.com	******1143	Bronze	7,934.60
C00310	k***@example	******9735	Bronze	7,258.95
C00265	v***@example.com	******9969	Gold	7,151.30
C00071	m***@example.com	******8442	Silver	6,933.30
C00338	b***@example.com	******7705	Silver	6,848.55
C00135	d***@example.com	******1672	Bronze	6,667.20
C00357	a***@example.com	*****7612	Bronze	6,637.90


Customer-level results should remain accessible only to authorized reviewers. Do not replace these masked fields with raw contact information in the submission.
Rejected-row evidence
The current rejected-row evidence is produced by aggregating timestamped ETL quarantine CSV files under include/quarantine/indhu/; a dedicated queryable rejected-row audit table has not been implemented.

A representative run recorded 54 rejected rows across these reasons:
Failure reason	Rows
Missing or unknown customer	18
Non-positive or invalid quantity	10
Missing or unknown product	9
Invalid or missing order timestamp	6
Missing or unknown store	5
Invalid discount percentage	3
Unexpected order status	3
Total	54


Separate run files can contain the same repeated counts. Do not sum repeated files and describe the result as unique rejected source records.
Reproducibility
To reproduce these results, execute the corresponding queries in include/sql/business_questions.sql against include/warehouse.duckdb. The figures represent the current local dataset and may change if the source data or pipeline outputs change.