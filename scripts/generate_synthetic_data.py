"""Generate synthetic data for the portfolio project.

The data is intentionally small enough for local testing, but it mimics the operational
complexity of a global travel-retail sales platform:
- 38 countries
- multiple airport / station locations
- unit-level transaction lines
- reposted sales lines
- cancelled and returned transactions
- priced products and non-priced kitchen instructions
- energy meter readings for 15-minute profitability analysis
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

COUNTRY_CODES = [
    "GB", "FR", "DE", "ES", "IT", "NL", "BE", "CH", "AT", "SE", "NO", "DK", "FI", "IE",
    "PT", "PL", "CZ", "HU", "RO", "GR", "TR", "AE", "QA", "SA", "IN", "SG", "MY", "TH",
    "AU", "NZ", "US", "CA", "BR", "MX", "ZA", "EG", "JP", "KR",
]

PRODUCTS = [
    ("P001", "Flat White", "Hot Drinks", 3.80, True, False),
    ("P002", "Americano", "Hot Drinks", 3.20, True, False),
    ("P003", "Chicken Sandwich", "Food", 6.50, True, False),
    ("P004", "Breakfast Combo", "Combo", 8.90, True, False),
    ("P005", "Vegan Wrap", "Food", 5.80, True, False),
    ("P006", "Bottled Water", "Cold Drinks", 2.40, True, False),
    ("P007", "Croissant", "Bakery", 2.95, True, False),
    ("K001", "Medium Rare", "Kitchen Instruction", 0.00, False, True),
    ("K002", "With Ice", "Kitchen Instruction", 0.00, False, True),
    ("K003", "Extra Hot", "Kitchen Instruction", 0.00, False, True),
]

DISCOUNTS = [
    ("NONE", "No Discount", 0.00),
    ("STAFF", "Airport Staff 10%", 0.10),
    ("PROMO", "Breakfast Combo Promo", 0.15),
    ("LOYALTY", "Loyalty App Offer", 0.08),
    ("WASTE", "End of Day Markdown", 0.25),
]


def _create_dimensions(rng: np.random.Generator) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create product and location dimensions used by the synthetic fact tables."""
    products = pd.DataFrame(
        PRODUCTS,
        columns=[
            "product_id",
            "product_name",
            "product_category",
            "standard_unit_price",
            "is_priced_product",
            "is_kitchen_instruction",
        ],
    )

    locations = []
    for idx, country_code in enumerate(COUNTRY_CODES, start=1):
        location_id = f"LOC{idx:03d}"
        for unit_number in range(1, 3):
            locations.append(
                {
                    "country_code": country_code,
                    "location_id": location_id,
                    "location_name": f"{country_code} Airport Terminal {unit_number}",
                    "unit_id": f"{location_id}-U{unit_number:02d}",
                    "unit_name": rng.choice(["Coffee Bar", "Food Hall", "Express Kiosk", "Bakery"]),
                    "currency_code": rng.choice(["GBP", "EUR", "USD"]),
                }
            )
    return products, pd.DataFrame(locations)


def _generate_sales(days: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Generate transaction lines and small dimensions."""
    rng = np.random.default_rng(seed)
    products, locations = _create_dimensions(rng)
    product_lookup = products.set_index("product_id").to_dict("index")
    start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)

    rows = []
    transaction_counter = 1

    for day_offset in range(days):
        business_date = (start_date + timedelta(days=day_offset)).date().isoformat()

        # Sample a subset of locations each day so the local dataset remains compact.
        sampled_locations = locations.sample(n=min(24, len(locations)), random_state=seed + day_offset)

        for _, loc in sampled_locations.iterrows():
            transaction_count = int(rng.integers(12, 25))
            for _ in range(transaction_count):
                transaction_id = f"T{transaction_counter:010d}"
                transaction_counter += 1
                base_ts = start_date + timedelta(
                    days=day_offset,
                    hours=int(rng.integers(5, 23)),
                    minutes=int(rng.integers(0, 60)),
                )
                line_count = int(rng.integers(1, 4))
                discount_type, discount_name, discount_rate = DISCOUNTS[int(rng.integers(0, len(DISCOUNTS)))]

                for line_no in range(1, line_count + 1):
                    product_id = rng.choice([p[0] for p in PRODUCTS], p=[0.18, 0.16, 0.14, 0.12, 0.10, 0.14, 0.10, 0.02, 0.02, 0.02])
                    product = product_lookup[product_id]
                    quantity = int(rng.integers(1, 4)) if product["is_priced_product"] else 1
                    is_combo = product_id == "P004" or bool(rng.random() < 0.08)
                    gross_sales = float(product["standard_unit_price"] * quantity)
                    discount_amount = gross_sales * discount_rate
                    tax_amount = max(gross_sales - discount_amount, 0) * 0.2

                    line_status = rng.choice(["completed", "cancelled", "returned"], p=[0.93, 0.04, 0.03])
                    extraction_ts = base_ts + timedelta(hours=2)

                    rows.append(
                        {
                            "business_date": business_date,
                            "country_code": loc["country_code"],
                            "location_id": loc["location_id"],
                            "location_name": loc["location_name"],
                            "unit_id": loc["unit_id"],
                            "unit_name": loc["unit_name"],
                            "transaction_id": transaction_id,
                            "transaction_line_id": f"{transaction_id}-{line_no:03d}",
                            "transaction_completed_ts": base_ts.isoformat(),
                            "product_id": product_id,
                            "product_name": product["product_name"],
                            "product_category": product["product_category"],
                            "is_combo": is_combo,
                            "quantity": quantity,
                            "gross_sales": round(gross_sales, 2),
                            "discount_amount": round(discount_amount, 2),
                            "tax_amount": round(tax_amount, 2),
                            "discount_type": discount_type,
                            "discount_name": discount_name,
                            "line_status": line_status,
                            "is_priced_product": bool(product["is_priced_product"]),
                            "is_kitchen_instruction": bool(product["is_kitchen_instruction"]),
                            "source_sequence": 1,
                            "source_update_ts": (base_ts + timedelta(minutes=5)).isoformat(),
                            "extraction_ts": extraction_ts.isoformat(),
                            "source_file_name": f"oracle_sales_{business_date}_run_01.csv",
                        }
                    )

    sales = pd.DataFrame(rows)

    # Add reposted/corrected rows. They share the same business keys but carry a higher
    # source_sequence and later update timestamp, which tests the dedupe rule.
    repost_sample = sales.sample(frac=0.06, random_state=seed).copy()
    repost_sample["source_sequence"] = 2
    repost_sample["discount_amount"] = (repost_sample["discount_amount"] * 1.1).round(2)
    repost_sample["source_update_ts"] = pd.to_datetime(repost_sample["source_update_ts"], utc=True) + pd.Timedelta(hours=6)
    repost_sample["extraction_ts"] = pd.to_datetime(repost_sample["extraction_ts"], utc=True) + pd.Timedelta(hours=6)
    repost_sample["source_file_name"] = repost_sample["business_date"].apply(lambda d: f"oracle_sales_{d}_run_06_repost.csv")

    sales = pd.concat([sales, repost_sample], ignore_index=True)
    return sales, products, locations


def _generate_inventory(days: int, seed: int, products: pd.DataFrame, locations: pd.DataFrame) -> pd.DataFrame:
    """Generate simple unit/product inventory snapshots."""
    rng = np.random.default_rng(seed + 100)
    start_date = datetime(2026, 1, 1)
    rows = []
    priced_products = products[products["is_priced_product"]]
    for day_offset in range(days):
        snapshot_date = (start_date + timedelta(days=day_offset)).date().isoformat()
        for _, loc in locations.sample(n=min(20, len(locations)), random_state=seed + day_offset).iterrows():
            for _, product in priced_products.sample(n=4, random_state=seed + day_offset).iterrows():
                rows.append(
                    {
                        "snapshot_date": snapshot_date,
                        "country_code": loc["country_code"],
                        "location_id": loc["location_id"],
                        "unit_id": loc["unit_id"],
                        "product_id": product["product_id"],
                        "on_hand_quantity": int(rng.integers(10, 200)),
                        "reserved_quantity": int(rng.integers(0, 20)),
                        "waste_quantity": int(rng.integers(0, 8)),
                    }
                )
    return pd.DataFrame(rows)


def _generate_energy(days: int, seed: int, locations: pd.DataFrame) -> pd.DataFrame:
    """Generate 15-minute energy cost data for opening/closing optimisation."""
    rng = np.random.default_rng(seed + 200)
    start_ts = datetime(2026, 1, 1, 5, 0, tzinfo=timezone.utc)
    rows = []
    sampled_units = locations.sample(n=min(24, len(locations)), random_state=seed)["unit_id"].tolist()
    for day_offset in range(days):
        for unit_id in sampled_units:
            for bucket in range(5 * 4, 23 * 4):
                ts = start_ts + timedelta(days=day_offset, minutes=bucket * 15)
                kwh = max(0.2, rng.normal(2.1, 0.5))
                unit_rate = rng.uniform(0.22, 0.38)
                labour_proxy = rng.uniform(6.0, 11.0)
                rows.append(
                    {
                        "unit_id": unit_id,
                        "quarter_hour_start_ts": ts.isoformat(),
                        "kwh_consumed": round(kwh, 3),
                        "energy_unit_rate": round(unit_rate, 3),
                        "estimated_running_cost": round(kwh * unit_rate + labour_proxy, 2),
                    }
                )
    return pd.DataFrame(rows)


def _generate_control_table() -> pd.DataFrame:
    """Generate ADF control-table rows that drive Oracle-to-ADLS ingestion."""
    return pd.DataFrame(
        [
            {
                "source_system": "oracle_pos",
                "source_schema": "POS",
                "source_table": "SALES_TRANSACTION_LINE",
                "target_entity": "sales_line_items",
                "load_pattern": "incremental",
                "watermark_column": "LAST_UPDATE_TS",
                "primary_key_columns": "BUSINESS_DATE,TRANSACTION_ID,TRANSACTION_LINE_ID",
                "is_active": 1,
                "run_group": "six_daily",
            },
            {
                "source_system": "oracle_pos",
                "source_schema": "POS",
                "source_table": "PAYMENT_LINE",
                "target_entity": "payment_lines",
                "load_pattern": "incremental",
                "watermark_column": "LAST_UPDATE_TS",
                "primary_key_columns": "BUSINESS_DATE,TRANSACTION_ID,PAYMENT_LINE_ID",
                "is_active": 1,
                "run_group": "six_daily",
            },
            {
                "source_system": "oracle_pos",
                "source_schema": "POS",
                "source_table": "DISCOUNT_LINE",
                "target_entity": "discount_lines",
                "load_pattern": "incremental",
                "watermark_column": "LAST_UPDATE_TS",
                "primary_key_columns": "BUSINESS_DATE,TRANSACTION_ID,DISCOUNT_LINE_ID",
                "is_active": 1,
                "run_group": "six_daily",
            },
            {
                "source_system": "oracle_pos",
                "source_schema": "MASTERDATA",
                "source_table": "PRODUCT",
                "target_entity": "products",
                "load_pattern": "full",
                "watermark_column": "",
                "primary_key_columns": "PRODUCT_ID",
                "is_active": 1,
                "run_group": "daily_reference",
            },
        ]
    )


def _write_new_unit_json_files(sales: pd.DataFrame, output_dir: Path) -> None:
    """Write a small ADLS-style JSON landing sample for newly onboarded units.

    The production pattern uses Auto Loader over ADLS-hosted JSON files. Locally we write
    newline-delimited JSON so reviewers can see the expected file shape quickly.
    """
    json_dir = output_dir / "adls_json_new_units" / "sales_line_items"
    json_dir.mkdir(parents=True, exist_ok=True)
    sample = sales.sample(n=min(250, len(sales)), random_state=144).copy()
    sample["source_file_name"] = "new_unit_sales_20260101.json"
    sample.to_json(json_dir / "new_unit_sales_20260101.json", orient="records", lines=True, date_format="iso")


def _generate_sharepoint_master_samples(
    output_dir: Path,
    products: pd.DataFrame,
    locations: pd.DataFrame,
    seed: int,
) -> None:
    """Create local CSV versions of SharePoint master-data files.

    In production ADF converts SharePoint Excel/CSV to parquet in ADLS. CSV is used here so
    the repository remains dependency-light and easy for recruiters to inspect.
    """
    rng = np.random.default_rng(seed + 300)
    master_dir = output_dir / "sharepoint_master_sources"
    master_dir.mkdir(parents=True, exist_ok=True)

    countries = locations[["country_code"]].drop_duplicates().copy()
    countries["country_name"] = countries["country_code"].apply(lambda code: f"Country {code}")
    countries["region"] = rng.choice(["Europe", "APAC", "Americas", "Middle East", "Africa"], size=len(countries))
    countries.to_csv(master_dir / "country_master.csv", index=False)

    locations.to_csv(master_dir / "unit_master.csv", index=False)

    budget_units = locations.sample(n=min(20, len(locations)), random_state=seed).copy()
    budget_rows = []
    for day in pd.date_range("2026-01-01", periods=10, freq="D"):
        for _, unit in budget_units.iterrows():
            budget_rows.append(
                {
                    "business_date": day.date().isoformat(),
                    "unit_id": unit["unit_id"],
                    "daily_budget_net_sales": round(float(rng.uniform(2500, 9000)), 2),
                }
            )
    budget = pd.DataFrame(budget_rows)
    budget.to_csv(master_dir / "daily_budget.csv", index=False)

    forecast = budget.copy()
    forecast["forecast_net_sales"] = (forecast["daily_budget_net_sales"] * rng.uniform(0.85, 1.2, len(forecast))).round(2)
    forecast.drop(columns=["daily_budget_net_sales"], inplace=True)
    forecast.to_csv(master_dir / "forecast.csv", index=False)

    corrections = products[["product_id", "product_category"]].copy()
    corrections["corrected_category"] = corrections["product_category"].replace({"Combo": "Meal Deal"})
    corrections.to_csv(master_dir / "product_hierarchy_corrections.csv", index=False)

    exclusions = pd.DataFrame(
        [
            {"product_id": "K001", "exclusion_reason": "Kitchen instruction", "is_active": True},
            {"product_id": "K002", "exclusion_reason": "Kitchen instruction", "is_active": True},
            {"product_id": "K003", "exclusion_reason": "Kitchen instruction", "is_active": True},
        ]
    )
    exclusions.to_csv(master_dir / "product_exclusions.csv", index=False)

    combo_override = pd.DataFrame(
        [{"product_id": "P004", "override_unit_price": 7.95, "is_active": True}]
    )
    combo_override.to_csv(master_dir / "combo_average_price_override.csv", index=False)

    unit_override = locations.sample(n=min(5, len(locations)), random_state=seed + 1)[
        ["unit_id", "unit_name"]
    ].copy()
    unit_override["corrected_unit_name"] = unit_override["unit_name"] + " - Corrected"
    unit_override.drop(columns=["unit_name"], inplace=True)
    unit_override.to_csv(master_dir / "unit_mapping_override.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/synthetic")
    parser.add_argument("--days", type=int, default=10)
    parser.add_argument("--seed", type=int, default=44)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sales, products, locations = _generate_sales(args.days, args.seed)
    inventory = _generate_inventory(args.days, args.seed, products, locations)
    energy = _generate_energy(args.days, args.seed, locations)
    control = _generate_control_table()

    sales.to_csv(output_dir / "sales_line_items.csv", index=False)
    products.to_csv(output_dir / "dim_products.csv", index=False)
    locations.to_csv(output_dir / "dim_locations.csv", index=False)
    inventory.to_csv(output_dir / "inventory_snapshots.csv", index=False)
    energy.to_csv(output_dir / "energy_meter_readings.csv", index=False)
    control.to_csv(output_dir / "adf_ingestion_control.csv", index=False)
    _write_new_unit_json_files(sales, output_dir)
    _generate_sharepoint_master_samples(output_dir, products, locations, args.seed)

    print(f"Synthetic data written to {output_dir.resolve()}")
    print(f"sales_line_items rows: {len(sales):,}")
    print(f"countries represented: {locations['country_code'].nunique():,}")


if __name__ == "__main__":
    main()
