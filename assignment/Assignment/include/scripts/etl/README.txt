NammaMart ETL SQL files
Copy all .sql files into your project at: include/scripts/etl/

IMPORTANT:
The current ETL.py shown in this conversation has:
    SQL_DIR = INCLUDE / "etl"
If you place files under include/scripts/etl/, update that line in dags/ETL.py to:
    SQL_DIR = INCLUDE / "scripts" / "etl"

The scripts are based on the SQL filenames referenced by the current ETL.py.
The code's create_empty_report.sql fallback creates a generic empty placeholder table;
if the ETL fact table lacks the expected reporting columns, the report will be empty until
the transformed fact schema is expanded. Review the DAG task logs after running.
