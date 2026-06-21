"""
data_validation.py
---------------------
Stage 4 of the ETL pipeline: independently verify three data-quality
invariants the rest of the pipeline depends on, rather than assuming the
earlier stages caught everything.

  1. transaction_id uniqueness  -- re-checked even though cleaning already
     deduplicated on this key, because a validation stage that just trusts
     the previous stage isn't actually validating anything.
  2. revenue consistency        -- compares the source system's reported
     `total_amount` against the `revenue` figure independently recomputed
     in the transformation stage. A small tolerance (>= ₹1) is used rather
     than exact float equality, since float rounding alone can produce
     sub-rupee differences that aren't genuine data problems.
  3. date integrity             -- flags any row where delivery_date falls
     before order_date, which is logically impossible and indicates an
     upstream data error.

Rather than dropping flagged rows outright, this stage attaches boolean
flag columns (`revenue_consistent`, `date_integrity_ok`) so the aggregation
stage can make an informed, per-KPI decision about whether to include them
(e.g. excluding date-integrity violations from an "average delivery time"
KPI without discarding their revenue from sales totals).
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("retail_etl.validation")

REVENUE_TOLERANCE = 1.0  # rupees


def validate_data(df: DataFrame) -> tuple[DataFrame, dict]:
    report = {}
    total_records = df.count()
    report["total_records_validated"] = total_records

    # ---- 1. transaction_id uniqueness --------------------------------------
    distinct_ids = df.select("transaction_id").distinct().count()
    duplicate_ids = total_records - distinct_ids
    report["duplicate_transaction_ids_found"] = duplicate_ids
    if duplicate_ids > 0:
        logger.warning(
            "%d duplicate transaction_id(s) survived cleaning -- investigate the dedup logic.",
            duplicate_ids,
        )
    else:
        logger.info("transaction_id uniqueness check passed: 0 duplicates.")

    # ---- 2. revenue consistency ------------------------------------------------
    df = df.withColumn(
        "revenue_consistent",
        F.abs(F.col("total_amount") - F.col("revenue")) < F.lit(REVENUE_TOLERANCE)
    )
    revenue_inconsistent = df.filter(~F.col("revenue_consistent")).count()
    report["revenue_inconsistent_records"] = revenue_inconsistent
    report["pct_revenue_consistent"] = round(
        100.0 * (total_records - revenue_inconsistent) / total_records, 2
    )

    # ---- 3. date integrity -------------------------------------------------
    df = df.withColumn(
        "date_integrity_ok",
        F.col("delivery_date") >= F.col("order_date")
    )
    date_violations = df.filter(~F.col("date_integrity_ok")).count()
    report["date_integrity_violations"] = date_violations
    report["pct_date_integrity_ok"] = round(
        100.0 * (total_records - date_violations) / total_records, 2
    )

    logger.info("Validation stage complete: %s", report)
    return df, report


if __name__ == "__main__":
    import logging as _logging
    from data_ingestion import get_spark_session, ingest_data
    from data_cleaning import clean_data
    from data_transformation import transform_data

    _logging.basicConfig(level=_logging.INFO)
    spark = get_spark_session()
    raw_df = ingest_data(spark, "data/raw/retail_sales_raw.csv")
    cleaned_df, _ = clean_data(raw_df)
    transformed_df = transform_data(cleaned_df)
    validated_df, validation_report = validate_data(transformed_df)
    print(validation_report)
    spark.stop()
