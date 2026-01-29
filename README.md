# Online Retail ETL Pipeline

This project implements a robust ETL (Extract, Transform, Load) pipeline that migrates and models online retail data from Snowflake to Google BigQuery. It transforms raw transaction data into a star schema suitable for analytics.

## 🚀 Overview

The pipeline performs the following steps:
1.  **Extract**: Fetches raw data from Snowflake (`RAW_ONLINE_RETAIL` table).
2.  **Transform**: Cleans and models the data using [Polars](https://pola.rs/).
    -   Standardizes data types.
    -   Separates transactions into Sales and Returns.
    -   Creates Fact tables: `fact_sales`, `fact_returns`.
    -   Creates Dimension tables: `dim_products`, `dim_customers`, `dim_date`.
3.  **Load**: Loads the processed tables into Google BigQuery.

## 🏗 Architecture

```mermaid
graph LR
    A[Snowflake\n(Raw Data)] -->|Extract| B(clean_data.py\nPolars Transformation)
    B -->|Fact & Dim Tables| C[BigQuery\n(Analytics Data)]
    
    subgraph Data Modeling
    B --> D[fact_sales]
    B --> E[fact_returns]
    B --> F[dim_products]
    B --> G[dim_customers]
    B --> H[dim_date]
    end
```

## 🛠️ Prerequisites

-   Python 3.8+
-   Snowflake Account
-   Google Cloud Platform (GCP) Project with BigQuery enabled

## 📦 Installation

1.  **Clone the repository** (if applicable)

2.  **Install dependencies**
    It is recommended to use a virtual environment.
    ```bash
    pip install polars snowflake-connector-python google-cloud-bigquery pandas python-dotenv db-dtypes
    ```

## ⚙️ Configuration

Create a `.env` file in the root directory with the following variables:

```ini
# Snowflake Connection
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Google BigQuery Configuration
PROJECT_ID=your_gcp_project_id
DATASET=your_bigquery_dataset_id
```

## ▶️ Usage

Run the main pipeline script:

```bash
python run_pipeline.py
```

Expected output:
```text
🚀 Starting ETL pipeline
📥 Fetching raw data from Snowflake...
Raw rows: 12345
🧹 Cleaning data and building analytics tables...
fact_sales: ... rows
fact_returns: ... rows
...
☁️ Loading tables into BigQuery...
➡️ Loading fact_sales...
✅ Loaded ... rows into project.dataset.fact_sales
...
✅ ETL pipeline completed successfully!
```

## 📂 Project Structure

-   `run_pipeline.py`: Main entry point. Orchestrates the Extract, Transform, and Load steps.
-   `snowflake_client.py`: Handles connection to Snowflake and raw data extraction.
-   `clean_data.py`: Contains data transformation logic using Polars. Defines the schema for fact and dimension tables.
-   `load_bigquery.py`: Handles loading Polars DataFrames into BigQuery.
-   `test_*.py`: Unit/Integration tests for Snowflake and Google connections.

## 📊 Data Model

The pipeline transforms the flat raw data into the following schema:

| Table Types | Table Name | Description |
| :--- | :--- | :--- |
| **Fact** | `fact_sales` | Individual sales transactions (positive quantities). |
| **Fact** | `fact_returns` | Return transactions (negative quantities or invoices starting with 'C'). |
| **Dimension** | `dim_products` | Unique product catalog (`STOCKCODE`, `DESCRIPTION`). |
| **Dimension** | `dim_customers` | Unique customer list (`CUSTOMERID`, `COUNTRY`). |
| **Dimension** | `dim_date` | Date dimension with year, month, day, week, etc. |
