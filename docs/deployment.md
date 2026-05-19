# Deployment Guide

## Local validation

```bash
pip install -e .[dev]
make data
make test
```

## Databricks bundle validation

```bash
databricks bundle validate -t dev
```

## Databricks deployment

```bash
databricks bundle deploy -t dev
databricks bundle run retail_sales_lakehouse_job -t dev
```

## Azure Data Factory deployment notes

1. Create the metadata database tables in `adf/metadata/control_table_schema.sql`.
2. Load `adf/metadata/sample_control_rows.csv` into `dbo.IngestionControl`.
3. Configure the Oracle linked service with Self-hosted Integration Runtime.
4. Configure parameterised datasets for Oracle and ADLS.
5. Schedule `pl_metadata_driven_oracle_to_adls` six times daily.
6. Ensure the target folder layout matches the Databricks Auto Loader path in the bundle variables.

## Production hardening checklist

- Store secrets in Azure Key Vault.
- Use Unity Catalog grants and service principals.
- Add row-count and reconciliation checks by business date and country.
- Capture ADF run audit and Databricks pipeline event logs.
- Define SLA alerts for failed or delayed country loads.
- Add cost attribution tags to jobs, clusters and storage.


## Additional deployment variables

The updated project includes additional paths for mixed ingestion patterns:

- `new_unit_json_path`: ADLS path containing JSON sales files shared by newly onboarded units.
- `sharepoint_master_path`: ADLS path containing parquet output from the metadata-driven SharePoint ingestion framework.

In a production workspace, these should point to Unity Catalog external locations or volumes with least-privilege access controls.
