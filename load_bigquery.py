from google.cloud import bigquery
import polars as pl

def load_to_bigquery(df: pl.DataFrame, table_id: str):
    """
    Loads a Polars DataFrame into BigQuery.

    Args:
        df (pl.DataFrame): The DataFrame to load
        table_id (str): Full BigQuery table ID in format
                        project.dataset.table
    """

    # Initialize BigQuery client (uses ADC)
    client = bigquery.Client()

    # Convert Polars DataFrame to Pandas (required by BigQuery)
    df_pandas = df.to_pandas()

    # Configure load job: overwrite table if it exists
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )

    # Start the load job
    job = client.load_table_from_dataframe(
        df_pandas,
        table_id,
        job_config=job_config
    )

    # Wait for job to complete
    job.result()

    print(f"✅ Loaded {len(df_pandas)} rows into {table_id}")
