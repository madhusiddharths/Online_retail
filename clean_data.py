import polars as pl


def build_analytics_tables(df: pl.DataFrame) -> dict:
    """
    Cleans raw online retail data and returns analytics-ready tables:
    - fact_sales
    - fact_returns
    - dim_products
    - dim_customers
    - dim_date
    """

    # ----------------------------
    # 1️⃣ Standardize & type cast
    # ----------------------------
    df = df.with_columns([
        pl.col("INVOICEDATE").str.strptime(pl.Datetime, "%m/%d/%y %H:%M", strict=False),
        pl.col("QUANTITY").cast(pl.Int64),
        pl.col("UNITPRICE").cast(pl.Float64),
        pl.col("CUSTOMERID").cast(pl.Int64),
        pl.col("COUNTRY").str.strip_chars(),
        pl.col("DESCRIPTION").str.strip_chars()
    ])

    print(df.select("INVOICEDATE").head())

    # ----------------------------
    # 2️⃣ Split sales vs returns
    # ----------------------------
    returns_df = df.filter(
        (pl.col("INVOICENO").str.starts_with("C")) |
        (pl.col("QUANTITY") < 0)
    )

    sales_df = df.filter(
        (~pl.col("INVOICENO").str.starts_with("C")) &
        (pl.col("QUANTITY") > 0) &
        (pl.col("UNITPRICE") > 0)
    )

    # ----------------------------
    # 3️⃣ Clean fact_sales
    # ----------------------------
    fact_sales = (
        sales_df
        .drop_nulls(["CUSTOMERID"])
        .unique()
        .with_columns(
            (pl.col("QUANTITY") * pl.col("UNITPRICE")).alias("TOTAL_PRICE")
        )
        .select([
            "INVOICENO",
            "INVOICEDATE",
            "CUSTOMERID",
            "STOCKCODE",
            "QUANTITY",
            "UNITPRICE",
            "TOTAL_PRICE",
            "COUNTRY"
        ])
    )

    # ----------------------------
    # 4️⃣ Clean fact_returns
    # ----------------------------
    fact_returns = (
        returns_df
        .drop_nulls(["CUSTOMERID"])
        .unique()
        .with_columns(
            (pl.col("QUANTITY").abs() * pl.col("UNITPRICE")).alias("RETURN_VALUE")
        )
        .select([
            "INVOICENO",
            "INVOICEDATE",
            "CUSTOMERID",
            "STOCKCODE",
            "QUANTITY",
            "UNITPRICE",
            "RETURN_VALUE",
            "COUNTRY"
        ])
    )

    # ----------------------------
    # 5️⃣ Dimension tables
    # ----------------------------

    dim_products = (
        df
        .select(["STOCKCODE", "DESCRIPTION"])
        .drop_nulls()
        .unique()
    )

    dim_customers = (
        df
        .select(["CUSTOMERID", "COUNTRY"])
        .drop_nulls(["CUSTOMERID"])
        .unique()
    )

    dim_date = (
        df
        .select(pl.col("INVOICEDATE").dt.date().alias("DATE"))
        .unique()
        .with_columns([
            pl.col("DATE").dt.year().alias("YEAR"),
            pl.col("DATE").dt.month().alias("MONTH"),
            pl.col("DATE").dt.day().alias("DAY"),
            pl.col("DATE").dt.weekday().alias("DAY_OF_WEEK"),
            pl.col("DATE").dt.week().alias("WEEK_OF_YEAR")
        ])
    )

    return {
        "fact_sales": fact_sales,
        "fact_returns": fact_returns,
        "dim_products": dim_products,
        "dim_customers": dim_customers,
        "dim_date": dim_date
    }
