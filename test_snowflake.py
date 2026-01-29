from snowflake_client import get_snowflake_connection
import polars as pl


def test_snowflake_connection():
    print("🔌 Connecting to Snowflake...")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()[0]
    print(f"✅ Connected to Snowflake version: {version}")

    cursor.execute("""
        SELECT COUNT(*) 
        FROM ONLINE_RETAIL.PUBLIC.RAW_ONLINE_RETAIL
    """)
    row_count = cursor.fetchone()[0]
    print(f"📊 Row count in RAW_ONLINE_RETAIL: {row_count}")

    cursor.execute("""
        SELECT * 
        FROM ONLINE_RETAIL.PUBLIC.RAW_ONLINE_RETAIL
        LIMIT 5
    """)
    rows = cursor.fetchall()
    cols = [c[0] for c in cursor.description]

    df = pl.DataFrame(rows, schema=cols)
    print("🔍 Sample data:")
    print(df)

    cursor.close()
    conn.close()

    print("🎉 Snowflake connection test passed!")


if __name__ == "__main__":
    test_snowflake_connection()
