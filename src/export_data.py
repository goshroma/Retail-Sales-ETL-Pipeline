"""
export_data.py
------------------
Stage 6 of the ETL pipeline: write the small, pre-aggregated tables a BI
tool like Power BI actually wants to connect to -- one clean, single CSV
file per table, not a Spark `part-00000-...csv` folder.

Spark's native `.write.csv()` is built for large, partitioned output and
produces a directory of part-files even for a 10-row aggregate, which is
exactly the kind of thing that confuses a BI tool's "Get Data > CSV"
dialog. Since every table this module receives from aggregation.py is
small (a handful of categories/states/months, or a few thousand
customers at most), each one is collected to the driver with
`.toPandas()` and written with pandas' `.to_csv()` instead -- the
standard "process the big stuff with Spark, export the small stuff with
pandas" split used in real BI-facing pipelines.
"""

import logging
import os

import pandas as pd

logger = logging.getLogger("retail_etl.export")


def export_reports(agg_results: dict, reports_dir: str = "reports") -> None:
    """Writes category_sales.csv, state_sales.csv, monthly_sales.csv,
    customer_summary.csv and kpi_summary.csv into reports_dir."""
    os.makedirs(reports_dir, exist_ok=True)

    exports = {
        "category_sales.csv": agg_results["revenue_by_category"],
        "state_sales.csv": agg_results["revenue_by_state"],
        "monthly_sales.csv": agg_results["monthly_revenue_trend"],
        "customer_summary.csv": agg_results["customer_summary"],
    }

    for filename, spark_df in exports.items():
        path = os.path.join(reports_dir, filename)
        spark_df.toPandas().to_csv(path, index=False)
        logger.info("Exported %s", path)

    # The KPI summary is a flat key/value table, not a Spark DataFrame --
    # it comes out of aggregation.py as a plain dict of scalars.
    kpis = agg_results["sales_kpis"]
    kpi_df = pd.DataFrame([{"kpi": k, "value": v} for k, v in kpis.items()])
    kpi_path = os.path.join(reports_dir, "kpi_summary.csv")
    kpi_df.to_csv(kpi_path, index=False)
    logger.info("Exported %s", kpi_path)

    logger.info(
        "Export stage complete: %d report file(s) written to %s/",
        len(exports) + 1, reports_dir,
    )


if __name__ == "__main__":
    import logging as _logging
    from data_ingestion import get_spark_session, ingest_data
    from data_cleaning import clean_data
    from data_transformation import transform_data
    from data_validation import validate_data
    from aggregation import run_aggregations

    _logging.basicConfig(level=_logging.INFO)
    spark = get_spark_session()
    raw_df = ingest_data(spark, "data/raw/retail_sales_raw.csv")
    cleaned_df, _ = clean_data(raw_df)
    transformed_df = transform_data(cleaned_df)
    validated_df, _ = validate_data(transformed_df)
    agg_results = run_aggregations(spark, validated_df)
    export_reports(agg_results)
    spark.stop()
