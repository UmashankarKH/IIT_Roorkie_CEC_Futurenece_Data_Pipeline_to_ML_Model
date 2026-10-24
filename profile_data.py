import pandas as pd
import os

data_dir = "assignment/nammamart_dataset"
files = ["customers.csv", "orders.csv", "payments.csv", "products.csv", "stores.csv", "orders_2026-10-01_increment.csv"]

for file in files:
    path = os.path.join(data_dir, file)
    if os.path.exists(path):
        df = pd.read_csv(path)
        print(f"\n--- {file} ---")
        print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
        print("Null counts per column:")
        print(df.isnull().sum()[df.isnull().sum() > 0])
        print("Duplicate rows:", df.duplicated().sum())
    else:
        print(f"File not found: {file}")