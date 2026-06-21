"""
data_transformation.py
------------------------
Stage 3 of the ETL pipeline: derive the analytical columns the business
KPIs depend on, and standardize the two free-text dimensions (category,
city) that arrived with inconsistent casing/spacing or legacy aliases.

Design choices (worth defending in an interview):
  * `revenue` is recomputed from the atomic fields (quantity, unit_price,
    discount) rather than trusted from the raw `total_amount` column.
    This is deliberate: it lets the validation stage compare the source
    system's reported total against an independently derived figure and
    flag the rows where they disagree, instead of silently inheriting
    whatever the source happened to report.
  * The dataset has no cost/COGS column, so `profit` is modeled using
    category-level margin assumptions (documented in CATEGORY_MARGINS
    below). This is a common, defensible approach for a transactional
    export that doesn't expose supplier cost data, and it's explicitly
    called out here and in the README so it's never mistaken for an
    actual reported figure.
  * Category casing/whitespace issues ("ELECTRONICS", "  Clothing") are
    fixed with trim + initcap, since they're pure formatting variants of
    the canonical names -- no manual mapping needed.
  * City names need both whitespace/casing normalization AND a legacy
    alias map (e.g. "Bombay" -> "Mumbai", "Bengaluru" -> "Bangalore")
    applied afterwards, since those are genuinely different strings, not
    formatting variants.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("retail_etl.transformation")

# Assumed category-level profit margins (no cost/COGS column exists in the
# source system, so profit is modeled rather than directly observed).
CATEGORY_MARGINS = {
    "Electronics": 0.12,
    "Clothing": 0.35,
    "Footwear": 0.30,
    "Grocery": 0.08,
    "Furniture": 0.18,
    "Beauty & Personal Care": 0.40,
    "Sports & Fitness": 0.25,
    "Books & Stationery": 0.20,
    "Home Decor": 0.28,
    "Toys & Games": 0.30,
    "Uncategorized": 0.15,  # fallback for rows with no source category
}

# Legacy / colloquial city names mapped to one canonical spelling.
CITY_ALIAS_MAP = {
    "Bombay": "Mumbai",
    "Calcutta": "Kolkata",
    "Bengaluru": "Bangalore",
    "Madras": "Chennai",
    "New Delhi": "Delhi",
}


def standardize_category(df: DataFrame) -> DataFrame:
    """Fixes casing/whitespace inconsistencies in the category column."""
    return df.withColumn("category", F.initcap(F.trim(F.col("category"))))


def normalize_city(df: DataFrame) -> DataFrame:
    """Fixes casing/whitespace, then maps legacy aliases to one canonical city name."""
    df = df.withColumn("city", F.initcap(F.trim(F.col("city"))))

    alias_map_expr = F.create_map([F.lit(x) for pair in CITY_ALIAS_MAP.items() for x in pair])
    df = df.withColumn(
        "city",
        F.coalesce(alias_map_expr[F.col("city")], F.col("city"))
    )
    return df


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Adds revenue, profit, delivery_days, order_month and order_year."""
    df = df.withColumn(
        "revenue",
        F.round(F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount")), 2)
    )

    margin_map_expr = F.create_map([F.lit(x) for pair in CATEGORY_MARGINS.items() for x in pair])
    df = df.withColumn(
        "profit_margin",
        F.coalesce(margin_map_expr[F.col("category")], F.lit(0.15))
    )
    df = df.withColumn("profit", F.round(F.col("revenue") * F.col("profit_margin"), 2))

    df = df.withColumn("delivery_days", F.datediff(F.col("delivery_date"), F.col("order_date")))
    df = df.withColumn("order_month", F.month(F.col("order_date")))
    df = df.withColumn("order_year", F.year(F.col("order_date")))
    df = df.withColumn("order_year_month", F.date_format(F.col("order_date"), "yyyy-MM"))

    return df


def transform_data(df: DataFrame) -> DataFrame:
    df = standardize_category(df)
    df = normalize_city(df)
    df = add_derived_columns(df)
    logger.info("Transformation stage complete. Columns: %s", df.columns)
    return df


if __name__ == "__main__":
    import logging as _logging
    from data_ingestion import get_spark_session, ingest_data
    from data_cleaning import clean_data

    _logging.basicConfig(level=_logging.INFO)
    spark = get_spark_session()
    raw_df = ingest_data(spark, "data/raw/retail_sales_raw.csv")
    cleaned_df, _ = clean_data(raw_df)
    transformed_df = transform_data(cleaned_df)
    transformed_df.printSchema()
    transformed_df.select(
        "transaction_id", "category", "city", "revenue", "profit",
        "delivery_days", "order_month", "order_year"
    ).show(10)
    spark.stop()
