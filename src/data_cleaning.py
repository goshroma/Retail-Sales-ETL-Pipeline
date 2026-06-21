"""
data_cleaning.py
------------------
Stage 2 of the ETL pipeline: handle the data-quality problems that are
expected in any real transactional export -- nulls, duplicates, invalid
dates, negative quantities and missing payment methods -- and produce a
cleaning-statistics report so the impact of each fix is auditable rather
than silent.

Design choices (worth defending in an interview):
  * Exact duplicate rows AND duplicate `transaction_id`s (the business key)
    are both removed -- the second catches cases where a duplicate row's
    non-key fields might differ slightly.
  * Missing `customer_id` is treated as a guest checkout rather than
    dropped, since dropping it would understate total revenue.
  * Missing `category` / `city` / `payment_method` are imputed with an
    explicit placeholder so the rows still contribute to revenue totals
    while remaining clearly flagged as incomplete in any group-by.
  * Negative quantities are corrected with abs() rather than dropped --
    these matched generated data-entry sign errors, not genuine refunds,
    and total_amount for these rows was already consistent with the
    corrected (positive) quantity.
  * Rows with an order_date that cannot be parsed as a real calendar date
    are dropped, since every downstream KPI is time-bound and a row that
    can't be placed in a period can't be aggregated correctly. The count
    of dropped rows is reported rather than silently lost.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("retail_etl.cleaning")

DATE_FORMAT = "yyyy-MM-dd"


def clean_data(df: DataFrame) -> tuple[DataFrame, dict]:
    """Returns (cleaned_df, cleaning_stats_dict)."""
    stats = {}
    stats["rows_before_cleaning"] = df.count()

    # ---- 1. Duplicate records -------------------------------------------------
    before = df.count()
    df = df.dropDuplicates()  # exact full-row duplicates
    stats["exact_duplicate_rows_removed"] = before - df.count()

    before = df.count()
    df = df.dropDuplicates(["transaction_id"])  # enforce business-key uniqueness
    stats["duplicate_transaction_ids_removed"] = before - df.count()

    # ---- 2. Missing values ------------------------------------------------
    stats["missing_customer_id_imputed"] = df.filter(F.col("customer_id").isNull()).count()
    df = df.withColumn(
        "customer_id",
        F.when(F.col("customer_id").isNull(), F.lit("GUEST_CUSTOMER")).otherwise(F.col("customer_id"))
    )

    stats["missing_payment_method_imputed"] = df.filter(F.col("payment_method").isNull()).count()
    df = df.withColumn(
        "payment_method",
        F.when(F.col("payment_method").isNull(), F.lit("Not Specified")).otherwise(F.col("payment_method"))
    )

    stats["missing_category_imputed"] = df.filter(F.col("category").isNull()).count()
    df = df.withColumn(
        "category",
        F.when(F.col("category").isNull(), F.lit("Uncategorized")).otherwise(F.col("category"))
    )

    stats["missing_city_imputed"] = df.filter(F.col("city").isNull()).count()
    df = df.withColumn(
        "city",
        F.when(F.col("city").isNull(), F.lit("Unknown City")).otherwise(F.col("city"))
    )

    # ---- 3. Negative quantities ---------------------------------------------
    stats["negative_quantities_corrected"] = df.filter(F.col("quantity") < 0).count()
    df = df.withColumn("quantity", F.abs(F.col("quantity")))

    # ---- 4. Invalid dates ------------------------------------------------------
    df = df.withColumn("order_date_parsed", F.to_date(F.col("order_date"), DATE_FORMAT))
    df = df.withColumn("delivery_date_parsed", F.to_date(F.col("delivery_date"), DATE_FORMAT))

    invalid_order_dates = df.filter(F.col("order_date_parsed").isNull()).count()
    stats["invalid_order_dates_dropped"] = invalid_order_dates
    df = df.filter(F.col("order_date_parsed").isNotNull())

    df = (
        df.drop("order_date", "delivery_date")
          .withColumnRenamed("order_date_parsed", "order_date")
          .withColumnRenamed("delivery_date_parsed", "delivery_date")
    )

    stats["rows_after_cleaning"] = df.count()
    stats["total_rows_removed"] = stats["rows_before_cleaning"] - stats["rows_after_cleaning"]

    logger.info("Cleaning stage complete: %s", stats)
    return df, stats


if __name__ == "__main__":
    import logging as _logging
    from data_ingestion import get_spark_session, ingest_data

    _logging.basicConfig(level=_logging.INFO)
    spark = get_spark_session()
    raw_df = ingest_data(spark, "data/raw/retail_sales_raw.csv")
    cleaned_df, cleaning_stats = clean_data(raw_df)
    cleaned_df.printSchema()
    cleaned_df.show(5)
    print(cleaning_stats)
    spark.stop()
