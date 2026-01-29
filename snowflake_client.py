import snowflake.connector
import polars as pl
from dotenv import load_dotenv
import snowflake.connector
import os

load_dotenv()  # loads .env into environment variables

def get_env(var):
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(f"Missing environment variable: {var}")
    return value


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=get_env("SNOWFLAKE_USER"),
        password=get_env("SNOWFLAKE_PASSWORD"),
        account=get_env("SNOWFLAKE_ACCOUNT"),
        warehouse=get_env("SNOWFLAKE_WAREHOUSE"),
        database=get_env("SNOWFLAKE_DATABASE"),
        schema=get_env("SNOWFLAKE_SCHEMA"),
    )

def fetch_raw_data():
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    query = """
    SELECT *
    FROM RAW_ONLINE_RETAIL
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]

    cursor.close()
    conn.close()

    df = pl.DataFrame(rows, schema=columns)
    return df
