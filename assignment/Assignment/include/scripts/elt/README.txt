NammaMart ELT SQL pack
Copy all .sql files into your project folder: include/elt/

These scripts match the SQL filenames referenced in the current ELT.py.
Assumed source CSV columns follow the NammaMart assignment schema:
orders, payments, customers, products, stores and orders_2026-10-01_increment.csv.
Keep source CSVs in the path configured by ELT.py (currently project-root/data/).
Run the DAG and check each task log; schema differences may require small column-name adjustments.
