# Databricks notebook source
# MAGIC %md
# MAGIC # DLT / Lakeflow pipeline walkthrough
# MAGIC
# MAGIC The actual pipeline code lives in `src/dlt/pipeline_global_sales_lakehouse.py` so it can be version-controlled and deployed through Databricks bundles.
# MAGIC
# MAGIC Key tables:
# MAGIC - `bronze_sales_line_items`
# MAGIC - `silver_sales_line_items_deduped`
# MAGIC - `silver_sales_line_items_business`
# MAGIC - `gold_unit_quarter_hour_sales`
# MAGIC - `gold_discount_strategy`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why the pipeline is not notebook-only
# MAGIC
# MAGIC A Lead Data Engineer should be able to package pipelines as source files, validate them in CI/CD, deploy them through a bundle, and promote them across environments. Notebooks are useful for exploration, but the production implementation should be managed as code.
