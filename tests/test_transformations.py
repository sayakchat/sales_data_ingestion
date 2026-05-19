from __future__ import annotations

import pandas as pd

from retail_lakehouse.transformations import (
    add_quarter_hour_bucket,
    apply_enhanced_quantity,
    apply_product_master_controls,
    build_discount_strategy_metrics,
    build_sharepoint_adls_path,
    calculate_net_sales,
    dedupe_reposted_sales_lines,
    normalise_sharepoint_folder_name,
)


def test_net_sales_handles_cancelled_and_returns() -> None:
    df = pd.DataFrame(
        [
            {"gross_sales": 100.0, "discount_amount": 10.0, "tax_amount": 15.0, "line_status": "completed"},
            {"gross_sales": 100.0, "discount_amount": 10.0, "tax_amount": 15.0, "line_status": "cancelled"},
            {"gross_sales": 100.0, "discount_amount": 10.0, "tax_amount": 15.0, "line_status": "returned"},
        ]
    )

    actual = calculate_net_sales(df)["net_sales"].tolist()

    assert actual == [75.0, 0.0, -75.0]


def test_dedupe_keeps_latest_reposted_sales_line() -> None:
    df = pd.DataFrame(
        [
            {
                "business_date": "2026-01-01",
                "transaction_id": "T1",
                "transaction_line_id": "T1-001",
                "source_sequence": 1,
                "source_update_ts": "2026-01-01T10:00:00Z",
                "extraction_ts": "2026-01-01T11:00:00Z",
                "discount_amount": 1.0,
            },
            {
                "business_date": "2026-01-01",
                "transaction_id": "T1",
                "transaction_line_id": "T1-001",
                "source_sequence": 2,
                "source_update_ts": "2026-01-01T12:00:00Z",
                "extraction_ts": "2026-01-01T13:00:00Z",
                "discount_amount": 2.5,
            },
        ]
    )

    deduped = dedupe_reposted_sales_lines(df)

    assert len(deduped) == 1
    assert deduped.loc[0, "discount_amount"] == 2.5


def test_enhanced_quantity_excludes_kitchen_instructions_and_unpriced_lines() -> None:
    df = pd.DataFrame(
        [
            {"quantity": 2, "is_priced_product": True, "is_kitchen_instruction": False},
            {"quantity": 1, "is_priced_product": False, "is_kitchen_instruction": True},
            {"quantity": 3, "is_priced_product": False, "is_kitchen_instruction": False},
        ]
    )

    result = apply_enhanced_quantity(df)

    assert result["enhanced_quantity"].tolist() == [2, 0, 0]


def test_quarter_hour_bucket_floors_timestamp() -> None:
    df = pd.DataFrame([{"transaction_completed_ts": "2026-01-01T10:07:33Z"}])

    result = add_quarter_hour_bucket(df)

    assert result.loc[0, "quarter_hour_key"] == "10:00"


def test_discount_strategy_metrics_aggregates_by_discount_name() -> None:
    df = pd.DataFrame(
        [
            {
                "country_code": "GB",
                "location_id": "LOC001",
                "unit_id": "U1",
                "discount_type": "PROMO",
                "discount_name": "Breakfast Combo Promo",
                "product_category": "Combo",
                "gross_sales": 100.0,
                "discount_amount": 10.0,
                "net_sales": 75.0,
                "transaction_id": "T1",
            },
            {
                "country_code": "GB",
                "location_id": "LOC001",
                "unit_id": "U1",
                "discount_type": "PROMO",
                "discount_name": "Breakfast Combo Promo",
                "product_category": "Combo",
                "gross_sales": 50.0,
                "discount_amount": 5.0,
                "net_sales": 37.5,
                "transaction_id": "T2",
            },
        ]
    )

    result = build_discount_strategy_metrics(df)

    assert result.loc[0, "gross_sales"] == 150.0
    assert result.loc[0, "discount_rate_percent"] == 10.0
    assert result.loc[0, "transaction_count"] == 2


def test_normalise_sharepoint_folder_name_removes_spaces_and_path_noise():
    """SharePoint folder names should become deterministic ADLS path segments."""
    assert normalise_sharepoint_folder_name("Daily Budget") == "dailybudget"
    assert normalise_sharepoint_folder_name("Product Hierarchy Corrections") == "producthierarchycorrections"
    assert normalise_sharepoint_folder_name("Unit Mapping / Override") == "unitmapping_override"


def test_build_sharepoint_adls_path_uses_normalised_folder_and_entity():
    """ADF writes cleaned parquet folder names so Databricks can use stable paths."""
    path = build_sharepoint_adls_path(
        "abfss://landing@account.dfs.core.windows.net/sharepoint",
        "Product Hierarchy Corrections",
        "Product Corrections",
    )
    assert path.endswith("/producthierarchycorrections/productcorrections")


def test_apply_product_master_controls_excludes_quantity_and_adds_combo_override():
    """Master-data controls should be visible in the transaction-level sales model."""
    sales = pd.DataFrame(
        {
            "product_id": ["P001", "P004"],
            "is_combo": [False, True],
            "enhanced_quantity": [2.0, 1.0],
        }
    )
    exclusions = pd.DataFrame(
        {
            "product_id": ["P001"],
            "exclusion_reason": ["Not priced for ATV"],
            "is_active": [True],
        }
    )
    overrides = pd.DataFrame(
        {
            "product_id": ["P004"],
            "override_unit_price": [7.25],
            "is_active": [True],
        }
    )

    result = apply_product_master_controls(sales, exclusions, overrides)

    assert result.loc[result["product_id"].eq("P001"), "enhanced_quantity"].iloc[0] == 0.0
    assert result.loc[result["product_id"].eq("P004"), "combo_override_unit_price"].iloc[0] == 7.25

