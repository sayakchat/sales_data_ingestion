# Table Management Strategy

This project uses different table patterns depending on the grain, source behaviour and business use case.

## Landing

- New units provide ADLS-hosted JSON files.
- Databricks Auto Loader reads those files into a DLT streaming landing table.
- The landing table preserves raw source evidence and allows new units to onboard without waiting for the established Oracle feed.

## Bronze

- All Bronze tables are DLT streaming tables.
- Bronze keeps source file name, ingestion timestamp, source route and DQ expectations.
- Oracle POS extracts, new-unit JSON files, energy readings and SharePoint parquet master files all enter the platform through Bronze.

## Silver dimensions

- Silver dimensions are mostly managed full-overwrite tables.
- This is appropriate for relatively small master-data files such as unit, country, daily budget, forecast, product hierarchy corrections, product exclusions, combo average price override and unit mapping override.
- The business can correct these files in SharePoint; the platform refreshes clean governed dimensions in Silver.

## Silver transactions

- Transactional Silver uses DLT streaming merge.
- Keys: `business_date`, `transaction_id`, `transaction_line_id`.
- Sequence: source sequence, source update timestamp and extraction timestamp.
- This solves the reposted-data problem where corrected rows arrive after the original business date.

## Complex Silver and Gold

- Reusable or expensive transformations can be materialized views.
- Gold tables are materialized views for aggregated data products, such as unit quarter-hour profitability and discount strategy.

## Analytics layer

- The Analytics layer is built from Silver, not Gold.
- It is denormalised for BI and SQL Warehouse consumption but remains transactionally traceable.
- DLT merge is used to maintain the latest analytics record for each transaction line.
