"""
aggregation.py
------------------
Stage 5 of the ETL pipeline: compute the Sales KPIs and Business KPIs that
the rest of the project (Power BI exports, SQL analysis, resume bullets)
is built on top of.

Design choices (worth defending in an interview):
  * KPIs are computed against the *validated* DataFrame and make a
    deliberate per-KPI choice about the `date_integrity_ok` flag: average
    delivery time excludes the rows that failed the date-integrity check
    (a negative delivery time isn't a real number, it's a data error), but
    revenue-based KPIs do NOT exclude those rows -- the sale still
    happened, and dropping it would understate revenue just because the
    delivery date was logged incorrectly.
  * Both the DataFrame API and Spark SQL are used on purpose. The headline
    Sales KPIs are computed with a registered temp view + `spark.sql(...)`
    to demonstrate the "and SQL" half of "using PySpark and SQL" that this
    project is meant to showcase; the categorical breakdowns use the
    DataFrame API where it reads more naturally. Both compile to the same
    Catalyst query plan -- the choice here is about readability and
    demonstrating both skills, not performance.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("retail_etl.aggregation")

VIEW_NAME = "retail_sales"


def compute_sales_kpis(spark: SparkSession, df: DataFrame) -> dict:
    """Total Revenue, Total Orders, Average Order Value, Total Customers -- via Spark SQL."""
    df.createOrReplaceTempView(VIEW_NAME)

    row = spark.sql(f"""
        SELECT
            ROUND(SUM(revenue), 2)            AS total_revenue,
            COUNT(DISTINCT transaction_id)    AS total_orders,
            ROUND(AVG(revenue), 2)            AS average_order_value,
            COUNT(DISTINCT customer_id)       AS total_customers
        FROM {VIEW_NAME}
    """).collect()[0]

    kpis = {
        "total_revenue": float(row["total_revenue"]),
        "total_orders": int(row["total_orders"]),
        "average_order_value": float(row["average_order_value"]),
        "total_customers": int(row["total_customers"]),
    }
    logger.info("Sales KPIs: %s", kpis)
    return kpis


def revenue_by_category(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Business KPI: revenue, profit and order count per category, via Spark SQL."""
    df.createOrReplaceTempView(VIEW_NAME)
    return spark.sql(f"""
        SELECT
            category,
            ROUND(SUM(revenue), 2)            AS total_revenue,
            ROUND(SUM(profit), 2)             AS total_profit,
            COUNT(DISTINCT transaction_id)    AS total_orders
        FROM {VIEW_NAME}
        GROUP BY category
        ORDER BY total_revenue DESC
    """)


def revenue_by_state(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Business KPI: revenue, order count and customer count per state, via Spark SQL."""
    df.createOrReplaceTempView(VIEW_NAME)
    return spark.sql(f"""
        SELECT
            state,
            ROUND(SUM(revenue), 2)            AS total_revenue,
            COUNT(DISTINCT transaction_id)    AS total_orders,
            COUNT(DISTINCT customer_id)       AS total_customers
        FROM {VIEW_NAME}
        GROUP BY state
        ORDER BY total_revenue DESC
    """)


def monthly_revenue_trend(df: DataFrame) -> DataFrame:
    """Business KPI: monthly revenue trend, via the DataFrame API."""
    return (
        df.groupBy("order_year", "order_month", "order_year_month")
          .agg(
              F.round(F.sum("revenue"), 2).alias("total_revenue"),
              F.countDistinct("transaction_id").alias("total_orders"),
          )
          .orderBy("order_year", "order_month")
    )


def top_products(df: DataFrame, n: int = 10) -> DataFrame:
    """Business KPI: top N products by revenue, via the DataFrame API."""
    return (
        df.groupBy("product_id", "category")
          .agg(
              F.round(F.sum("revenue"), 2).alias("total_revenue"),
              F.sum("quantity").alias("total_units_sold"),
          )
          .orderBy(F.desc("total_revenue"))
          .limit(n)
    )


def top_cities_by_revenue(df: DataFrame, n: int = 10) -> DataFrame:
    """Business KPI: top N cities by revenue, via the DataFrame API."""
    return (
        df.groupBy("city", "state")
          .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
          .orderBy(F.desc("total_revenue"))
          .limit(n)
    )


def average_delivery_time(df: DataFrame) -> float:
    """
    Average delivery_days, excluding rows that failed the date-integrity
    check in validation (delivery_date < order_date is a data error, not
    a real delivery time, and would silently drag the average down).
    """
    result = (
        df.filter(F.col("date_integrity_ok"))
          .agg(F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"))
          .collect()[0]["avg_delivery_days"]
    )
    return float(result)


def customer_summary(df: DataFrame) -> DataFrame:
    """Per-customer rollup -- feeds the Power BI customer_summary.csv export."""
    return (
        df.groupBy("customer_id")
          .agg(
              F.countDistinct("transaction_id").alias("total_orders"),
              F.round(F.sum("revenue"), 2).alias("total_revenue"),
              F.round(F.avg("revenue"), 2).alias("avg_order_value"),
              F.max("order_date").alias("last_order_date"),
          )
          .orderBy(F.desc("total_revenue"))
    )


def run_aggregations(spark: SparkSession, df: DataFrame) -> dict:
    """Orchestrates every KPI computation. Returns a dict mixing DataFrames and scalars."""
    results = {}
    results["sales_kpis"] = compute_sales_kpis(spark, df)
    results["sales_kpis"]["average_delivery_days"] = average_delivery_time(df)
    results["revenue_by_category"] = revenue_by_category(spark, df)
    results["revenue_by_state"] = revenue_by_state(spark, df)
    results["monthly_revenue_trend"] = monthly_revenue_trend(df)
    results["top_products"] = top_products(df)
    results["top_cities"] = top_cities_by_revenue(df)
    results["customer_summary"] = customer_summary(df)

    logger.info("Aggregation stage complete.")
    return results


if __name__ == "__main__":
    import logging as _logging
    from data_ingestion import get_spark_session, ingest_data
    from data_cleaning import clean_data
    from data_transformation import transform_data
    from data_validation import validate_data

    _logging.basicConfig(level=_logging.INFO)
    spark = get_spark_session()
    raw_df = ingest_data(spark, "data/raw/retail_sales_raw.csv")
    cleaned_df, _ = clean_data(raw_df)
    transformed_df = transform_data(cleaned_df)
    validated_df, _ = validate_data(transformed_df)

    agg_results = run_aggregations(spark, validated_df)
    print(agg_results["sales_kpis"])
    agg_results["revenue_by_category"].show()
    agg_results["top_products"].show()
    spark.stop()
