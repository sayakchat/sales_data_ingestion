# Architecture Notes

## Context

This project models a global travel-retail sales lakehouse where a central platform team ingests on-prem Oracle POS data into Azure and Databricks. The design balances engineering reliability, commercial analytics, and governance.

## Layer responsibilities

### Landing

Landing is the immutable handoff from Azure Data Factory. Files are written by source entity, extraction date, and run identifier. No business rules are applied here.

Example path:

```text
/landing/sales_line_items/extract_date=2026-01-01/run_id=06/*.csv
```

### Bronze

Bronze stores raw records with ingestion metadata. It applies lightweight technical expectations such as required keys and non-negative measures, but it does not remove business evidence.

### Silver

Silver applies the business rules that define trustworthy sales:

- Reposted transaction deduplication by business date and sales line key.
- Cancelled and returned line handling.
- Net sales calculation.
- Enhanced quantity logic.
- Quarter-hour trading buckets.

### Gold

Gold contains curated facts and aggregates:

- Unit sales in 15-minute buckets.
- Discount performance by geography and product category.
- Combo analytics.
- ATV and enhanced quantity metrics.

### Analytics layer

The analytics layer is denormalised enough for BI performance while preserving transaction-level drill-through. This is useful in executive reporting where numbers are frequently challenged by country or unit leaders.

## Why this distinguishes a Lead Data Engineer

A junior portfolio often shows simple file ingestion. This project shows the problems that happen in real enterprise platforms: reposts, late-arriving corrections, cancelled/returned lines, global metadata-driven ingestion, cost-aware analytics, and source-controlled deployments.


## Added ingestion patterns

### ADLS JSON files from new units

New business units can share JSON files directly into an ADLS landing path. Databricks Auto Loader ingests this path as a DLT streaming landing table and standardises the schema before the data joins the unified Bronze sales stream.

### SharePoint Excel/CSV master data

Business-owned master-data files are sourced from SharePoint through a metadata-driven ADF pipeline. The control table stores SharePoint URL, folder path, file name, worksheet, entity name and Key Vault secret references. ADF removes spaces from folder names and writes parquet to ADLS; Databricks then reads the parquet folders as Bronze streaming tables.

### Analytics from Silver

The Analytics layer is intentionally built from Silver. Gold remains a curated aggregate layer, while Analytics is denormalised but transactional for reconciliation and drill-through.
