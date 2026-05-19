# Databricks notebook source
# MAGIC %md
# MAGIC # Local quickstart notebook
# MAGIC This notebook-style file demonstrates the business transformations on synthetic data.

# COMMAND ----------

import pandas as pd

from retail_lakehouse.transformations import (
    add_quarter_hour_bucket,
    apply_enhanced_quantity,
    build_discount_strategy_metrics,
    calculate_net_sales,
    dedupe_reposted_sales_lines,
)

# COMMAND ----------

sales = pd.read_csv("../data/synthetic/sales_line_items.csv")

# COMMAND ----------

silver = (
    sales.pipe(dedupe_reposted_sales_lines)
    .pipe(calculate_net_sales)
    .pipe(apply_enhanced_quantity)
    .pipe(add_quarter_hour_bucket)
)

# COMMAND ----------

silver.head()

# COMMAND ----------

discount_metrics = build_discount_strategy_metrics(silver)
discount_metrics.head(10)
