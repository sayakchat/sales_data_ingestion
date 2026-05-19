"""Business transformation functions for a global travel-retail sales lakehouse.

These functions are deliberately written in pandas so the most critical business logic can
be unit-tested locally. In production, the same logic is implemented in Databricks using
Spark / Delta Live Tables. Keeping business rules testable outside the platform is a useful
Lead Data Engineer pattern because it lowers regression risk during refactoring.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd


MONEY_COLUMNS = ["gross_sales", "discount_amount", "tax_amount"]
DEDUP_KEYS = ["business_date", "transaction_id", "transaction_line_id"]


def _require_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    """Raise a clear error when a transformation receives an incomplete dataframe."""
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def calculate_net_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate commercial net sales and standardise cancelled / returned transactions.

    Business rule:
    - Base net sales = gross sales - discount amount - tax amount.
    - Cancelled lines contribute zero revenue.
    - Returned lines are represented as negative revenue so downstream aggregations can
      naturally net them against original sales.

    The function keeps both `net_sales_before_status` and `net_sales` so analysts can
    audit how status handling changed the raw commercial value.
    """
    _require_columns(df, MONEY_COLUMNS + ["line_status"])

    result = df.copy()

    # Force numeric values because raw ingestion from Oracle/CSV can occasionally produce
    # strings for decimal fields when schema inference is enabled.
    for column in MONEY_COLUMNS:
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)

    result["net_sales_before_status"] = (
        result["gross_sales"] - result["discount_amount"] - result["tax_amount"]
    ).round(2)

    status = result["line_status"].str.lower().fillna("completed")
    result["net_sales"] = result["net_sales_before_status"]
    result.loc[status.eq("cancelled"), "net_sales"] = 0.0
    result.loc[status.eq("returned"), "net_sales"] = -result.loc[
        status.eq("returned"), "net_sales_before_status"
    ].abs()

    result["net_sales"] = result["net_sales"].round(2)
    return result


def dedupe_reposted_sales_lines(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest version of a reposted sales line.

    Many point-of-sale platforms allow corrected or reposted transactions. If the lakehouse
    only appends new extracts, the same business sale can be counted multiple times. The
    dedupe grain here is `(business_date, transaction_id, transaction_line_id)` and the
    latest version is chosen by source sequence, source update timestamp, then extraction
    timestamp.
    """
    _require_columns(df, DEDUP_KEYS + ["source_sequence", "source_update_ts", "extraction_ts"])

    result = df.copy()
    result["source_update_ts"] = pd.to_datetime(result["source_update_ts"], utc=True)
    result["extraction_ts"] = pd.to_datetime(result["extraction_ts"], utc=True)

    result = result.sort_values(
        DEDUP_KEYS + ["source_sequence", "source_update_ts", "extraction_ts"],
        ascending=[True, True, True, True, True, True],
    )

    # `keep="last"` keeps the most recent version after the deterministic sort above.
    return result.drop_duplicates(subset=DEDUP_KEYS, keep="last").reset_index(drop=True)


def apply_enhanced_quantity(df: pd.DataFrame) -> pd.DataFrame:
    """Create enhanced quantity for Average Transaction Value and product-volume analytics.

    Kitchen instructions such as `medium`, `rare`, `hot`, or `with ice` often appear as
    non-priced POS lines. Counting them as products inflates quantity and distorts ATV. This
    rule keeps quantity only when the line is a priced product and not a kitchen instruction.
    """
    _require_columns(df, ["quantity", "is_priced_product", "is_kitchen_instruction"])

    result = df.copy()
    result["quantity"] = pd.to_numeric(result["quantity"], errors="coerce").fillna(0.0)

    valid_product_mask = result["is_priced_product"].astype(bool) & ~result[
        "is_kitchen_instruction"
    ].astype(bool)
    result["enhanced_quantity"] = result["quantity"].where(valid_product_mask, 0.0)
    return result


def add_quarter_hour_bucket(df: pd.DataFrame, timestamp_col: str = "transaction_completed_ts") -> pd.DataFrame:
    """Add a 15-minute trading bucket for opening / closing time analysis."""
    _require_columns(df, [timestamp_col])

    result = df.copy()
    result[timestamp_col] = pd.to_datetime(result[timestamp_col], utc=True)
    result["quarter_hour_start_ts"] = result[timestamp_col].dt.floor("15min")
    result["quarter_hour_key"] = result["quarter_hour_start_ts"].dt.strftime("%H:%M")
    return result


def build_unit_15_min_profitability(
    sales_df: pd.DataFrame,
    energy_df: pd.DataFrame,
    break_even_multiplier: float = 1.0,
) -> pd.DataFrame:
    """Compare 15-minute net sales against estimated running cost.

    The output supports questions such as: "Which units should open later or close earlier
    because early/late trading periods do not cover energy and labour running cost?"
    """
    _require_columns(
        sales_df,
        ["country_code", "location_id", "unit_id", "quarter_hour_start_ts", "net_sales", "enhanced_quantity"],
    )
    _require_columns(energy_df, ["unit_id", "quarter_hour_start_ts", "estimated_running_cost"])

    sales = sales_df.copy()
    energy = energy_df.copy()
    sales["quarter_hour_start_ts"] = pd.to_datetime(sales["quarter_hour_start_ts"], utc=True)
    energy["quarter_hour_start_ts"] = pd.to_datetime(energy["quarter_hour_start_ts"], utc=True)

    sales_agg = (
        sales.groupby(["country_code", "location_id", "unit_id", "quarter_hour_start_ts"], as_index=False)
        .agg(
            net_sales=("net_sales", "sum"),
            enhanced_quantity=("enhanced_quantity", "sum"),
            transaction_count=("transaction_id", "nunique"),
        )
    )

    cost_agg = (
        energy.groupby(["unit_id", "quarter_hour_start_ts"], as_index=False)
        .agg(estimated_running_cost=("estimated_running_cost", "sum"))
    )

    result = sales_agg.merge(cost_agg, on=["unit_id", "quarter_hour_start_ts"], how="left")
    result["estimated_running_cost"] = result["estimated_running_cost"].fillna(0.0)
    result["break_even_threshold"] = result["estimated_running_cost"] * break_even_multiplier
    result["profitability_gap"] = (result["net_sales"] - result["break_even_threshold"]).round(2)
    result["trading_recommendation"] = result["profitability_gap"].apply(
        lambda gap: "review_open_close_time" if gap < 0 else "keep_current_trading_window"
    )
    return result


def build_combo_price_variance(df: pd.DataFrame) -> pd.DataFrame:
    """Compare product unit price inside a combo versus standalone sale price.

    The output is intentionally simple and explainable for commercial teams. Negative price
    variance means the product is cheaper in a combo; positive variance means the product is
    priced higher in a combo than when sold separately.
    """
    _require_columns(df, ["product_id", "product_name", "is_combo", "net_sales", "enhanced_quantity"])

    priced_lines = df[df["enhanced_quantity"] > 0].copy()
    priced_lines["unit_net_price"] = priced_lines["net_sales"] / priced_lines["enhanced_quantity"]

    standalone = (
        priced_lines[~priced_lines["is_combo"].astype(bool)]
        .groupby(["product_id", "product_name"], as_index=False)
        .agg(standalone_avg_unit_net_price=("unit_net_price", "mean"))
    )

    combo = (
        priced_lines[priced_lines["is_combo"].astype(bool)]
        .groupby(["product_id", "product_name"], as_index=False)
        .agg(combo_avg_unit_net_price=("unit_net_price", "mean"), combo_units=("enhanced_quantity", "sum"))
    )

    result = combo.merge(standalone, on=["product_id", "product_name"], how="left")
    result["combo_vs_standalone_delta"] = (
        result["combo_avg_unit_net_price"] - result["standalone_avg_unit_net_price"]
    ).round(2)
    result["combo_discount_percent"] = (
        result["combo_vs_standalone_delta"] / result["standalone_avg_unit_net_price"] * 100
    ).round(2)
    return result.sort_values("combo_units", ascending=False).reset_index(drop=True)


def build_discount_strategy_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise discount performance by geography, unit, discount, and product category."""
    _require_columns(
        df,
        [
            "country_code",
            "location_id",
            "unit_id",
            "discount_type",
            "discount_name",
            "product_category",
            "gross_sales",
            "discount_amount",
            "net_sales",
            "transaction_id",
        ],
    )

    grouped = (
        df.groupby(
            [
                "country_code",
                "location_id",
                "unit_id",
                "discount_type",
                "discount_name",
                "product_category",
            ],
            as_index=False,
        )
        .agg(
            gross_sales=("gross_sales", "sum"),
            discount_amount=("discount_amount", "sum"),
            net_sales=("net_sales", "sum"),
            transaction_count=("transaction_id", "nunique"),
        )
    )

    grouped["discount_rate_percent"] = (
        grouped["discount_amount"] / grouped["gross_sales"].replace({0: pd.NA}) * 100
    ).fillna(0.0).round(2)
    grouped["net_sales_per_transaction"] = (
        grouped["net_sales"] / grouped["transaction_count"].replace({0: pd.NA})
    ).fillna(0.0).round(2)
    return grouped.sort_values(["net_sales", "transaction_count"], ascending=False).reset_index(drop=True)


def normalise_sharepoint_folder_name(folder_name: str) -> str:
    """Normalise a SharePoint folder name before writing to ADLS.

    Real SharePoint business folders often contain spaces and user-friendly names such as
    "Daily Budget" or "Product Hierarchy Corrections". ADLS paths are easier to operate
    when they are deterministic and safe for automation. This helper removes spaces,
    trims leading/trailing separators, lowercases the value, and converts common path
    separators to underscores.

    Examples:
    - "Daily Budget" -> "dailybudget"
    - "Product Hierarchy Corrections" -> "producthierarchycorrections"
    - "Unit Mapping Override" -> "unitmappingoverride"
    """
    if folder_name is None:
        raise ValueError("folder_name must not be None")

    normalised = str(folder_name).strip().lower()
    # Remove spaces exactly as the ADF framework does before writing parquet to ADLS.
    normalised = normalised.replace(" ", "")
    # Convert characters that are awkward in cloud paths into underscores.
    for character in ["/", "\\", "&", "?", "#", "%", ":"]:
        normalised = normalised.replace(character, "_")
    # Collapse duplicate underscores caused by multiple special characters.
    while "__" in normalised:
        normalised = normalised.replace("__", "_")
    return normalised.strip("_")


def build_sharepoint_adls_path(base_path: str, sharepoint_folder: str, entity_name: str) -> str:
    """Build the curated ADLS path used by the metadata-driven SharePoint ingestion.

    The ADF pipeline stores user-friendly SharePoint metadata in the control table, but the
    target ADLS path is normalised so Databricks can read predictable parquet folders.
    This mirrors a common enterprise pattern: SharePoint remains business-owned while ADLS
    becomes platform-owned and engineering-safe.
    """
    base = str(base_path).rstrip("/")
    folder = normalise_sharepoint_folder_name(sharepoint_folder)
    entity = normalise_sharepoint_folder_name(entity_name)
    return f"{base}/{folder}/{entity}"


def apply_product_master_controls(
    sales_df: pd.DataFrame,
    product_exclusions_df: pd.DataFrame,
    combo_price_override_df: pd.DataFrame,
) -> pd.DataFrame:
    """Apply SharePoint-controlled master-data corrections to sales lines.

    Business users often maintain small but high-impact correction files in SharePoint.
    This function demonstrates two examples from the project brief:
    - Product exclusions remove lines that should not contribute to enhanced quantity.
    - Combo average price overrides give commercial teams a governed override for combo
      analytics when the POS structure does not represent combo price cleanly.
    """
    _require_columns(sales_df, ["product_id", "is_combo", "enhanced_quantity"])
    _require_columns(product_exclusions_df, ["product_id", "exclusion_reason", "is_active"])
    _require_columns(combo_price_override_df, ["product_id", "override_unit_price", "is_active"])

    result = sales_df.copy()

    active_exclusions = product_exclusions_df[product_exclusions_df["is_active"].astype(bool)][
        ["product_id", "exclusion_reason"]
    ]
    result = result.merge(active_exclusions, on="product_id", how="left")
    result["is_masterdata_excluded_product"] = result["exclusion_reason"].notna()
    result.loc[result["is_masterdata_excluded_product"], "enhanced_quantity"] = 0.0

    active_overrides = combo_price_override_df[combo_price_override_df["is_active"].astype(bool)][
        ["product_id", "override_unit_price"]
    ]
    result = result.merge(active_overrides, on="product_id", how="left")
    result["combo_override_unit_price"] = pd.to_numeric(
        result["override_unit_price"], errors="coerce"
    )
    result.drop(columns=["override_unit_price"], inplace=True)
    return result

