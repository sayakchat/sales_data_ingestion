# Databricks Delta Live Tables / Lakeflow Declarative Pipeline implementation.
# --------------------------------------------------------------------------------------
# This file is designed for Databricks, not local execution. The pandas module in
# src/retail_lakehouse contains unit-testable versions of the core business rules.
#
# The project deliberately shows multiple ingestion patterns in one governed lakehouse:
# 1. Oracle POS extracts landed by Azure Data Factory through Self-hosted IR.
# 2. JSON files shared by newly onboarded units into ADLS, ingested with Auto Loader.
# 3. SharePoint Excel/CSV master-data files converted to parquet by a metadata-driven ADF
#    pipeline and then loaded into Databricks.
# --------------------------------------------------------------------------------------

import dlt
from pyspark.sql import functions as F

# Pipeline configuration values are provided by databricks.yml / resources/*.yml.
LANDING_PATH = spark.conf.get("landing_path")
NEW_UNIT_JSON_PATH = spark.conf.get("new_unit_json_path")
SHAREPOINT_MASTER_PATH = spark.conf.get("sharepoint_master_path")
CHECKPOINT_PATH = spark.conf.get("checkpoint_path")


# ======================================================================================
# LANDING / BRONZE: all raw source tables are DLT streaming tables.
# ======================================================================================


@dlt.table(
    name="landing_new_unit_sales_json",
    comment="Landing streaming table for JSON sales files shared by newly onboarded units in ADLS.",
    table_properties={"quality": "landing", "source_pattern": "adls_json_autoloader"},
)
@dlt.expect("transaction_id_is_present", "transaction_id IS NOT NULL")
@dlt.expect("business_date_is_present", "business_date IS NOT NULL")
def landing_new_unit_sales_json():
    """Ingest JSON files from new business units using Databricks Auto Loader.

    New units do not always arrive through the established Oracle/ADF route on day one.
    This parallel landing path lets the platform onboard JSON files quickly while still
    applying the same downstream DQ, dedupe and business rules as the core POS feed.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schemas/new_unit_sales_json")
        .load(NEW_UNIT_JSON_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_ingestion_route", F.lit("adls_json_new_unit"))
    )


@dlt.table(
    name="bronze_oracle_sales_line_items",
    comment="Bronze streaming table for Oracle POS extracts landed in ADLS by ADF.",
    table_properties={"quality": "bronze", "source_pattern": "adf_oracle_csv"},
)
@dlt.expect("transaction_id_is_present", "transaction_id IS NOT NULL")
@dlt.expect("business_date_is_present", "business_date IS NOT NULL")
@dlt.expect("gross_sales_is_not_negative_for_completed_lines", "line_status <> 'completed' OR gross_sales >= 0")
def bronze_oracle_sales_line_items():
    """Ingest raw Oracle POS extracts incrementally through Auto Loader.

    ADF handles source extraction and network connectivity; Databricks handles incremental
    file discovery and schema tracking. This keeps the boundary clean between orchestration
    and lakehouse processing.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schemas/oracle_sales_line_items")
        .option("header", "true")
        .load(f"{LANDING_PATH}/sales_line_items/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_ingestion_route", F.lit("adf_oracle_shir"))
    )


@dlt.table(
    name="bronze_new_unit_sales_line_items",
    comment="Bronze streaming table standardising new-unit JSON sales into the POS sales schema.",
    table_properties={"quality": "bronze", "source_pattern": "adls_json_autoloader"},
)
def bronze_new_unit_sales_line_items():
    """Standardise JSON landing records so they can be unioned with Oracle POS rows."""
    return dlt.read_stream("landing_new_unit_sales_json").select(
        "business_date",
        "country_code",
        "location_id",
        "location_name",
        "unit_id",
        "unit_name",
        "transaction_id",
        "transaction_line_id",
        "transaction_completed_ts",
        "product_id",
        "product_name",
        "product_category",
        "is_combo",
        "quantity",
        "gross_sales",
        "discount_amount",
        "tax_amount",
        "discount_type",
        "discount_name",
        "line_status",
        "is_priced_product",
        "is_kitchen_instruction",
        "source_sequence",
        "source_update_ts",
        "extraction_ts",
        "_ingested_at",
        "_source_file",
        "_source_ingestion_route",
    )


@dlt.table(
    name="bronze_sales_line_items_unified",
    comment="Unified bronze streaming table for Oracle POS and new-unit JSON transaction lines.",
    table_properties={"quality": "bronze"},
)
def bronze_sales_line_items_unified():
    """Union all sales ingestion routes before applying business deduplication."""
    oracle_rows = dlt.read_stream("bronze_oracle_sales_line_items")
    json_rows = dlt.read_stream("bronze_new_unit_sales_line_items")
    return oracle_rows.unionByName(json_rows, allowMissingColumns=True)


@dlt.table(
    name="bronze_energy_meter_readings",
    comment="Bronze streaming table for 15-minute energy readings used to estimate unit running cost.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("unit_id_is_present", "unit_id IS NOT NULL")
@dlt.expect("running_cost_is_non_negative", "estimated_running_cost >= 0")
def bronze_energy_meter_readings():
    """Ingest streaming-like energy readings from ADLS landing."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schemas/energy_meter_readings")
        .option("header", "true")
        .load(f"{LANDING_PATH}/energy_meter_readings/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


@dlt.table(
    name="bronze_sharepoint_master_files",
    comment="Bronze streaming table for SharePoint Excel/CSV master-data files converted to parquet by ADF.",
    table_properties={"quality": "bronze", "source_pattern": "adf_sharepoint_to_parquet"},
)
def bronze_sharepoint_master_files():
    """Load business-managed master data from the SharePoint ingestion landing area.

    The ADF pipeline parameterises SharePoint URL, folder path, file type and Key Vault app
    registration secret. It removes spaces from SharePoint folder names before writing the
    resulting parquet files to ADLS, for example `Daily Budget` -> `dailybudget`.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schemas/sharepoint_master_files")
        .load(SHAREPOINT_MASTER_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


# ======================================================================================
# SILVER: dimensions are full-overwrite managed tables; transactions are streaming merge.
# ======================================================================================

# DLT apply_changes performs the managed streaming merge for reposted transactional data.
# It keeps one current version per business sales-line key and prevents reposted data from
# double-counting revenue.
dlt.create_streaming_table(
    name="silver_sales_line_items_deduped",
    comment="Streaming-merge Silver table: latest business version of each sales line.",
    table_properties={"quality": "silver", "merge_pattern": "dlt_apply_changes_scd1"},
)

dlt.apply_changes(
    target="silver_sales_line_items_deduped",
    source="bronze_sales_line_items_unified",
    keys=["business_date", "transaction_id", "transaction_line_id"],
    sequence_by=F.struct(
        F.col("source_sequence").cast("long"),
        F.to_timestamp("source_update_ts"),
        F.to_timestamp("extraction_ts"),
    ),
    stored_as_scd_type=1,
)


@dlt.table(
    name="silver_sales_line_items_business",
    comment="Streaming Silver table with net sales, status handling, enhanced quantity and 15-minute buckets.",
    table_properties={"quality": "silver", "table_type": "transaction_streaming_merge"},
)
@dlt.expect("valid_net_sales", "net_sales IS NOT NULL")
@dlt.expect("valid_enhanced_quantity", "enhanced_quantity >= 0")
def silver_sales_line_items_business():
    """Apply the core sales business rules after dedupe."""
    df = dlt.read_stream("silver_sales_line_items_deduped")

    net_sales_before_status = (
        F.coalesce(F.col("gross_sales").cast("double"), F.lit(0.0))
        - F.coalesce(F.col("discount_amount").cast("double"), F.lit(0.0))
        - F.coalesce(F.col("tax_amount").cast("double"), F.lit(0.0))
    )

    return (
        df.withColumn("net_sales_before_status", F.round(net_sales_before_status, 2))
        .withColumn(
            "net_sales",
            F.when(F.lower(F.col("line_status")) == "cancelled", F.lit(0.0))
            .when(F.lower(F.col("line_status")) == "returned", -F.abs(F.col("net_sales_before_status")))
            .otherwise(F.col("net_sales_before_status")),
        )
        .withColumn(
            "enhanced_quantity",
            F.when(
                (F.col("is_priced_product") == True) & (F.col("is_kitchen_instruction") == False),
                F.col("quantity").cast("double"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("transaction_completed_ts", F.to_timestamp("transaction_completed_ts"))
        .withColumn("quarter_hour_start_ts", F.date_trunc("minute", F.col("transaction_completed_ts")))
        .withColumn(
            "quarter_hour_start_ts",
            F.expr("timestampadd(MINUTE, -minute(quarter_hour_start_ts) % 15, quarter_hour_start_ts)"),
        )
    )


@dlt.table(
    name="silver_dim_unit",
    comment="Full-overwrite managed Silver dimension created from SharePoint unit and mapping override files.",
    table_properties={"quality": "silver", "load_pattern": "managed_full_overwrite_dimension"},
)
def silver_dim_unit():
    """Resolve unit master and unit mapping overrides maintained by the business."""
    master = dlt.read("bronze_sharepoint_master_files").filter("entity_name = 'unit'")
    overrides = dlt.read("bronze_sharepoint_master_files").filter("entity_name = 'unitmappingoverride'")
    return (
        master.alias("m")
        .join(overrides.alias("o"), "unit_id", "left")
        .select(
            F.col("m.country_code"),
            F.col("m.location_id"),
            F.col("m.unit_id"),
            F.coalesce(F.col("o.corrected_unit_name"), F.col("m.unit_name")).alias("unit_name"),
            F.col("m.currency_code"),
            F.current_timestamp().alias("_silver_refreshed_at"),
        )
    )


@dlt.table(
    name="silver_dim_product",
    comment="Full-overwrite managed Silver dimension with hierarchy corrections and product exclusions.",
    table_properties={"quality": "silver", "load_pattern": "managed_full_overwrite_dimension"},
)
def silver_dim_product():
    """Apply product hierarchy corrections and exclusions from SharePoint master files."""
    product = dlt.read("bronze_sharepoint_master_files").filter("entity_name = 'product'")
    corrections = dlt.read("bronze_sharepoint_master_files").filter(
        "entity_name = 'producthierarchycorrections'"
    )
    exclusions = dlt.read("bronze_sharepoint_master_files").filter("entity_name = 'productexclusions'")
    return (
        product.alias("p")
        .join(corrections.alias("c"), "product_id", "left")
        .join(exclusions.alias("e"), "product_id", "left")
        .select(
            F.col("p.product_id"),
            F.col("p.product_name"),
            F.coalesce(F.col("c.corrected_category"), F.col("p.product_category")).alias("product_category"),
            F.col("p.standard_unit_price"),
            F.col("p.is_priced_product"),
            F.col("p.is_kitchen_instruction"),
            F.coalesce(F.col("e.is_active"), F.lit(False)).alias("is_excluded_from_enhanced_quantity"),
            F.current_timestamp().alias("_silver_refreshed_at"),
        )
    )


@dlt.table(
    name="silver_daily_budget_forecast",
    comment="Full-overwrite managed Silver table for daily budget and forecast master data.",
    table_properties={"quality": "silver", "load_pattern": "managed_full_overwrite_dimension"},
)
def silver_daily_budget_forecast():
    """Combine business-managed daily budget and forecast files for variance reporting."""
    budget = dlt.read("bronze_sharepoint_master_files").filter("entity_name = 'dailybudget'")
    forecast = dlt.read("bronze_sharepoint_master_files").filter("entity_name = 'forecast'")
    return (
        budget.alias("b")
        .join(forecast.alias("f"), ["business_date", "unit_id"], "full")
        .select(
            F.coalesce(F.col("b.business_date"), F.col("f.business_date")).alias("business_date"),
            F.coalesce(F.col("b.unit_id"), F.col("f.unit_id")).alias("unit_id"),
            F.col("b.daily_budget_net_sales"),
            F.col("f.forecast_net_sales"),
            F.current_timestamp().alias("_silver_refreshed_at"),
        )
    )


@dlt.table(
    name="silver_energy_15_minute_cost",
    comment="Silver streaming table standardised to the same 15-minute grain as trading analytics.",
    table_properties={"quality": "silver"},
)
def silver_energy_15_minute_cost():
    """Aggregate or standardise energy data to the same time grain as sales analytics."""
    return (
        dlt.read_stream("bronze_energy_meter_readings")
        .withColumn("quarter_hour_start_ts", F.to_timestamp("quarter_hour_start_ts"))
        .groupBy("unit_id", "quarter_hour_start_ts")
        .agg(
            F.sum(F.col("kwh_consumed").cast("double")).alias("kwh_consumed"),
            F.sum(F.col("estimated_running_cost").cast("double")).alias("estimated_running_cost"),
        )
    )


# ======================================================================================
# GOLD: materialized views / aggregate data products built from Silver.
# ======================================================================================


@dlt.table(
    name="gold_unit_quarter_hour_sales",
    comment="Gold materialized view for unit-level opening and closing time analysis.",
    table_properties={"quality": "gold", "table_type": "materialized_view"},
)
def gold_unit_quarter_hour_sales():
    """Aggregate sales by unit and 15-minute bucket, then compare against running cost."""
    sales = dlt.read("silver_sales_line_items_business")
    energy = dlt.read("silver_energy_15_minute_cost")

    sales_agg = (
        sales.groupBy("business_date", "country_code", "location_id", "unit_id", "quarter_hour_start_ts")
        .agg(
            F.sum("net_sales").alias("net_sales"),
            F.sum("enhanced_quantity").alias("enhanced_quantity"),
            F.countDistinct("transaction_id").alias("transaction_count"),
        )
    )

    return (
        sales_agg.join(energy, ["unit_id", "quarter_hour_start_ts"], "left")
        .fillna({"estimated_running_cost": 0.0, "kwh_consumed": 0.0})
        .withColumn("profitability_gap", F.round(F.col("net_sales") - F.col("estimated_running_cost"), 2))
        .withColumn(
            "trading_recommendation",
            F.when(F.col("profitability_gap") < 0, F.lit("review_open_close_time")).otherwise(
                F.lit("keep_current_trading_window")
            ),
        )
    )


@dlt.table(
    name="gold_discount_strategy",
    comment="Gold materialized view for discount performance by geography, unit and product category.",
    table_properties={"quality": "gold", "table_type": "materialized_view"},
)
def gold_discount_strategy():
    """Support commercial discount strategy analysis."""
    return (
        dlt.read("silver_sales_line_items_business")
        .groupBy(
            "country_code",
            "location_id",
            "unit_id",
            "discount_type",
            "discount_name",
            "product_category",
        )
        .agg(
            F.sum("gross_sales").alias("gross_sales"),
            F.sum("discount_amount").alias("discount_amount"),
            F.sum("net_sales").alias("net_sales"),
            F.countDistinct("transaction_id").alias("transaction_count"),
        )
        .withColumn("discount_rate_percent", F.round(F.col("discount_amount") / F.col("gross_sales") * 100, 2))
        .withColumn("net_sales_per_transaction", F.round(F.col("net_sales") / F.col("transaction_count"), 2))
    )


# ======================================================================================
# ANALYTICS: BI-ready transactional layer built directly from Silver, not Gold.
# ======================================================================================


@dlt.view(name="analytics_sales_transactional_source")
def analytics_sales_transactional_source():
    """Denormalised transactional source view for the Analytics-layer merge.

    This intentionally reads from Silver tables, not Gold, so BI users can drill back to
    transaction line detail while still receiving clean dimensions and business logic.
    """
    sales = dlt.read_stream("silver_sales_line_items_business")
    unit = dlt.read("silver_dim_unit")
    product = dlt.read("silver_dim_product")
    budget = dlt.read("silver_daily_budget_forecast")

    return (
        sales.alias("s")
        .join(unit.alias("u"), ["country_code", "location_id", "unit_id"], "left")
        .join(product.alias("p"), "product_id", "left")
        .join(budget.alias("b"), ["business_date", "unit_id"], "left")
        .select(
            "s.business_date",
            "s.country_code",
            "s.location_id",
            "s.unit_id",
            "u.unit_name",
            "s.transaction_id",
            "s.transaction_line_id",
            "s.transaction_completed_ts",
            "s.quarter_hour_start_ts",
            "s.product_id",
            "p.product_name",
            "p.product_category",
            "s.gross_sales",
            "s.discount_amount",
            "s.tax_amount",
            "s.net_sales",
            "s.enhanced_quantity",
            "s.discount_type",
            "s.discount_name",
            "s.line_status",
            "b.daily_budget_net_sales",
            "b.forecast_net_sales",
            "s.source_sequence",
            "s.source_update_ts",
            "s.extraction_ts",
        )
    )


dlt.create_streaming_table(
    name="analytics_sales_transactional",
    comment="Analytics-layer DLT merge table built directly from Silver for BI drill-through.",
    table_properties={"quality": "analytics", "source_layer": "silver", "merge_pattern": "dlt_apply_changes_scd1"},
)

dlt.apply_changes(
    target="analytics_sales_transactional",
    source="analytics_sales_transactional_source",
    keys=["business_date", "transaction_id", "transaction_line_id"],
    sequence_by=F.struct(
        F.col("source_sequence").cast("long"),
        F.to_timestamp("source_update_ts"),
        F.to_timestamp("extraction_ts"),
    ),
    stored_as_scd_type=1,
)
