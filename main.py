"""
main.py
------------------
Orchestrates the full Retail Sales ETL pipeline end to end:

    ingestion -> cleaning -> transformation -> validation -> aggregation
    -> CSV export -> (optional) MySQL load

Run with:
    python src/generate_dataset.py     # one-time: build the raw dataset
    python main.py                     # run the full pipeline
"""

import logging
import os
import sys
import time

sys.path.insert(0, "src")

from logger_config import setup_logging                      # noqa: E402
from data_ingestion import get_spark_session, ingest_data     # noqa: E402
from data_cleaning import clean_data                          # noqa: E402
from data_transformation import transform_data                # noqa: E402
from data_validation import validate_data                     # noqa: E402
from aggregation import run_aggregations                      # noqa: E402
from export_data import export_reports                        # noqa: E402

RAW_DATA_PATH = "data/raw/retail_sales_raw.csv"
PROCESSED_DIR = "data/processed"

# Flip to True once a local MySQL server is running and sql/create_tables.sql
# (or just an empty database) is in place -- see README.md "How To Run".
# Also overridable via the LOAD_TO_MYSQL env var (used by docker-compose.yml).
LOAD_TO_MYSQL = os.environ.get("LOAD_TO_MYSQL", "false").strip().lower() == "true"


def main() -> None:
    setup_logging()
    logger = logging.getLogger("retail_etl.main")
    pipeline_start = time.time()

    logger.info("=" * 70)
    logger.info("Retail Sales ETL Pipeline -- starting run")
    logger.info("=" * 70)

    spark = get_spark_session(enable_mysql_jdbc=LOAD_TO_MYSQL)

    try:
        # ---- Stage 1: Ingestion ------------------------------------------------
        logger.info("Stage 1/6: Ingestion")
        raw_df = ingest_data(spark, RAW_DATA_PATH, PROCESSED_DIR)
        logger.info("Ingestion success: %d usable rows.", raw_df.count())

        # ---- Stage 2: Cleaning --------------------------------------------------
        logger.info("Stage 2/6: Cleaning")
        cleaned_df, cleaning_stats = clean_data(raw_df)
        logger.info("Cleaning success: %s", cleaning_stats)

        # ---- Stage 3: Transformation --------------------------------------------
        logger.info("Stage 3/6: Transformation")
        transformed_df = transform_data(cleaned_df)
        logger.info("Transformation success: columns = %s", transformed_df.columns)

        # ---- Stage 4: Validation -------------------------------------------------
        logger.info("Stage 4/6: Validation")
        validated_df, validation_report = validate_data(transformed_df)
        logger.info("Validation success: %s", validation_report)

        # Persist the fully processed table -- useful for the notebook and for
        # ad-hoc inspection, independent of the MySQL load step below.
        validated_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{PROCESSED_DIR}/retail_sales_processed"
        )
        logger.info("Processed dataset written to %s/retail_sales_processed", PROCESSED_DIR)

        # ---- Stage 5: Aggregation -----------------------------------------------
        logger.info("Stage 5/6: Aggregation")
        agg_results = run_aggregations(spark, validated_df)
        logger.info("Aggregation success: sales KPIs = %s", agg_results["sales_kpis"])

        # ---- Stage 6: Export ------------------------------------------------------
        logger.info("Stage 6/6: Export")
        export_reports(agg_results)
        logger.info("Export success: reports/ written.")

        # ---- Optional: MySQL load -------------------------------------------------
        if LOAD_TO_MYSQL:
            from load_to_mysql import load_to_mysql
            logger.info("Loading processed data into MySQL ...")
            load_to_mysql(validated_df)
            logger.info("MySQL load success.")
        else:
            logger.info(
                "Skipping MySQL load (LOAD_TO_MYSQL=False). Set it to True in "
                "main.py once a local MySQL server is running -- see README.md."
            )

        elapsed = round(time.time() - pipeline_start, 1)
        logger.info("=" * 70)
        logger.info("Pipeline run complete in %ss.", elapsed)
        logger.info("=" * 70)

        print("\nPipeline finished successfully.")
        print(f"  Sales KPIs   : {agg_results['sales_kpis']}")
        print(f"  Reports      : reports/")
        print(f"  Full log     : logs/pipeline.log")

    except Exception:
        logger.exception("Pipeline run failed.")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
