"""
data_ingestion.py
------------------
Stage 1 of the ETL pipeline: read the raw retail sales CSV into a Spark
DataFrame.

Design notes (useful talking points for interviews):
  * `infer_schema_preview()` shows what Spark's own type inference produces
    on a sample of the file -- handy during exploration, but not something
    you'd want to build a production job on, since inferred types can
    silently shift if a single new value in the source file changes them.
  * The real ingestion path enforces an explicit `StructType` instead.
  * The schema carries an extra `_corrupt_record` column. Spark's CSV
    reader, in PERMISSIVE mode, treats a value that cannot be cast to its
    declared column type as a genuinely corrupted record: it nulls out the
    rest of that row and stores the original raw line in this column. Rows
    that are merely missing/blank values are NOT corrupt records by this
    definition -- those are real, if messy, data and are handled later in
    `data_cleaning.py`. Corrupt records are split off and written to
    `data/processed/retail_sales_raw_malformed_records.csv` so nothing
    is silently discarded.
"""

import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

logger = logging.getLogger("retail_etl.ingestion")

CORRUPT_RECORD_COL = "_corrupt_record"


def get_spark_session(app_name: str = "RetailSalesETL", enable_mysql_jdbc: bool = False) -> SparkSession:
    """
    Creates (or fetches) a local SparkSession sized for a single machine.

    enable_mysql_jdbc: when True, adds the MySQL Connector/J Maven
    coordinate to `spark.jars.packages` so Spark resolves and loads the
    JDBC driver automatically (via Ivy) when the session starts -- no
    manual JAR download needed. This only has an effect on the call that
    actually creates the JVM/session in this process; because of
    `getOrCreate()`'s reuse semantics, set it to True on main.py's first
    call if the MySQL load stage will run, and every later
    `get_spark_session()` call (with or without the flag) will transparently
    reuse that same already-configured session.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if enable_mysql_jdbc:
        builder = builder.config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0")
    return builder.getOrCreate()


def get_raw_schema() -> StructType:
    """Explicit schema for the raw retail_sales CSV (13 source columns)."""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("delivery_date", StringType(), True),
    ])


def infer_schema_preview(spark: SparkSession, input_path: str) -> None:
    """Logs what Spark's schema inference guesses from a 5% sample of rows."""
    inferred = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("samplingRatio", 0.05)
        .csv(input_path)
    )
    logger.info("Schema inference preview (5%% sample):")
    for field in inferred.schema.fields:
        logger.info("  inferred -> %s: %s", field.name, field.dataType.simpleString())


def ingest_data(spark: SparkSession, input_path: str, processed_dir: str = "data/processed") -> DataFrame:
    """
    Reads the raw CSV using the explicit schema in PERMISSIVE mode.

    Returns only the rows Spark could fully parse against the schema.
    Genuinely corrupt rows (failed type casts) are logged and persisted
    separately rather than dropped silently.
    """
    schema_with_corrupt_col = get_raw_schema().add(CORRUPT_RECORD_COL, StringType(), True)

    raw_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .schema(schema_with_corrupt_col)
        .csv(input_path)
    )
    raw_df.cache()

    total_count = raw_df.count()
    malformed_df = raw_df.filter(raw_df[CORRUPT_RECORD_COL].isNotNull())
    malformed_count = malformed_df.count()
    clean_df = raw_df.filter(raw_df[CORRUPT_RECORD_COL].isNull()).drop(CORRUPT_RECORD_COL)
    clean_count = clean_df.count()

    logger.info(
        "Ingestion complete: %d rows read | %d malformed (failed schema cast) | %d usable",
        total_count, malformed_count, clean_count,
    )

    if malformed_count > 0:
        os.makedirs(processed_dir, exist_ok=True)
        malformed_path = os.path.join(processed_dir, "retail_sales_raw_malformed_records.csv")
        try:
            (malformed_df.select(CORRUPT_RECORD_COL)
             .coalesce(1)
             .write.mode("overwrite")
             .option("header", True)
             .csv(malformed_path))
            logger.warning("Wrote %d malformed records to %s", malformed_count, malformed_path)
        except Exception as exc:
            logger.error("Could not persist malformed records: %s", exc)

    raw_df.unpersist()
    return clean_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = get_spark_session()
    infer_schema_preview(spark, "data/raw/retail_sales_raw.csv")
    df = ingest_data(spark, "data/raw/retail_sales_raw.csv")
    df.printSchema()
    df.show(5, truncate=False)
    spark.stop()
