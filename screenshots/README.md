# Screenshots

This folder is a placeholder. Screenshots can't be generated inside the
sandbox this project was built in (no PySpark/MySQL execution available
there -- see the main README's "A note on how this was built" section),
so capture these once you've run the pipeline locally and drop the image
files in here:

1. **Terminal output of `python main.py`** -- shows all 6 stages
   completing with their logged stats (rows cleaned, validation results,
   final KPIs).
2. **`logs/pipeline.log`** open in an editor, scrolled to a representative
   section.
3. **MySQL client** (Workbench/CLI) running one of the queries from
   `sql/business_queries.sql` against `retail_sales_processed`, showing
   results.
4. **`notebooks/exploratory_analysis.ipynb`** rendered in Jupyter, showing
   one of the matplotlib charts.
5. **Power BI / Excel** with `reports/category_sales.csv` or
   `reports/monthly_sales.csv` loaded into a chart, if you build a
   dashboard on top of the exports.

Suggested filenames: `01_pipeline_run.png`, `02_pipeline_log.png`,
`03_mysql_query.png`, `04_notebook_chart.png`, `05_dashboard.png`.
