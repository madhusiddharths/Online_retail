from snowflake_client import fetch_raw_data
from clean_data import build_analytics_tables
from load_bigquery import load_to_bigquery
import os

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")

def main():
    print("🚀 Starting ETL pipeline")

    # ----------------------------
    # 1️⃣ Fetch raw data
    # ----------------------------
    print("📥 Fetching raw data from Snowflake...")
    raw_df = fetch_raw_data()
    print(f"Raw rows: {raw_df.height}")

    # ----------------------------
    # 2️⃣ Build analytics tables
    # ----------------------------
    print("🧹 Cleaning data and building analytics tables...")
    tables = build_analytics_tables(raw_df)

    for name, df in tables.items():
        print(f"{name}: {df.height} rows")

    # ----------------------------
    # 3️⃣ Load tables into BigQuery
    # ----------------------------
    print("☁️ Loading tables into BigQuery...")

    for table_name, df in tables.items():
        table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
        print(f"➡️ Loading {table_name}...")
        load_to_bigquery(df, table_id)

    print("✅ ETL pipeline completed successfully!")


if __name__ == "__main__":
    main()
