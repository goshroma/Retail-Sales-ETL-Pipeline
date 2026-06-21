"""
load_to_mysql.py
------------------
Loads the validated, transformed DataFrame into a MySQL table so the
queries in sql/business_queries.sql have something to run against, and
so the project genuinely satisfies "load processed data into MySQL"
rather than only exporting CSVs.

This uses Spark's own JDBC writer (`DataFrame.write.jdbc(...)`/
`.format("jdbc")`) rather than a separate Python MySQL client, so the
same SparkSession that did the ingestion/cleaning/transformation also
owns the load -- one execution engine end to end, which is also the
pattern most real Spark-to-warehouse pipelines use. The MySQL
Connector/J driver itself is fetched automatically via
`spark.jars.packages` (see `get_spark_session(enable_mysql_jdbc=True)`
in data_ingestion.py), so no manual JAR download is required.

NOTE ON SANDBOX EXECUTION: this module could not be run against a live
MySQL server while this project was being built, because that sandbox
had neither a MySQL server nor network egress available. The JDBC logic
below follows the documented Spark JDBC data source API precisely and
is expected to run as-is on a machine with MySQL installed and
reachable -- see README.md's "How To Run" section for the one-time setup
(create the database, optionally run sql/create_tables.sql, then flip
LOAD_TO_MYSQL = True in main.py).
"""

import logging
import os

from pyspark.sql import DataFrame

logger = logging.getLogger("retail_etl.mysql_load")

# Columns that exist for the pipeline's own internal use (QA flags, a
# margin-lookup helper column) and aren't part of the warehouse table the
# SQL queries in sql/business_queries.sql are written against.
COLUMNS_TO_DROP_BEFORE_LOAD = ["profit_margin", "revenue_consistent", "date_integrity_ok"]


def get_mysql_connection_properties() -> dict:
    """
    Reads MySQL connection details from environment variables, falling
    back to sensible local-dev defaults so the project still runs with
    zero configuration against a freshly-installed local MySQL instance
    (default root user, no password, default port).
    """
    return {
        "host": os.environ.get("MYSQL_HOST", "localhost"),
        "port": os.environ.get("MYSQL_PORT", "3306"),
        "database": os.environ.get("MYSQL_DATABASE", "retail_sales_db"),
        "user": os.environ.get("MYSQL_USER", "root"),
        "password": os.environ.get("MYSQL_PASSWORD", ""),
    }


def load_to_mysql(df: DataFrame, table_name: str = "retail_sales_processed") -> None:
    """Overwrites table_name in MySQL with the contents of df."""
    conn = get_mysql_connection_properties()
    jdbc_url = (
        f"jdbc:mysql://{conn['host']}:{conn['port']}/{conn['database']}"
        f"?useSSL=false&allowPublicKeyRetrieval=true"
    )

    load_df = df.drop(*[c for c in COLUMNS_TO_DROP_BEFORE_LOAD if c in df.columns])
    row_count = load_df.count()
    logger.info("Writing %d rows to MySQL table '%s' at %s ...", row_count, table_name, jdbc_url)

    (
        load_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("overwrite")
        .save()
    )

    logger.info("MySQL load complete: table '%s' refreshed with %d rows.", table_name, row_count)


if __name__ == "__main__":
    import logging as _logging
    from data_ingestion import get_spark_session, ingest_data
    from data_cleaning import clean_data
    from data_transformation import transform_data
    from data_validation import validate_data

    _logging.basicConfig(level=_logging.INFO)
    spark = get_spark_session(enable_mysql_jdbc=True)
    raw_df = ingest_data(spark, "data/raw/retail_sales_raw.csv")
    cleaned_df, _ = clean_data(raw_df)
    transformed_df = transform_data(cleaned_df)
    validated_df, _ = validate_data(transformed_df)
    load_to_mysql(validated_df)
    spark.stop()
