/*
Control table for metadata-driven SharePoint Excel/CSV ingestion.

Design intent:
- Business users can maintain small but important master-data files in SharePoint.
- ADF uses one parameterised pipeline instead of one pipeline per file.
- Secrets such as app registration client secret are resolved from Azure Key Vault.
- Target ADLS folders remove spaces from SharePoint folder names and store files as parquet.
*/

CREATE TABLE dbo.SharePointIngestionControl (
    sharepoint_ingestion_id      INT IDENTITY(1,1) PRIMARY KEY,
    source_system                VARCHAR(100) NOT NULL DEFAULT 'sharepoint',
    sharepoint_site_url          VARCHAR(1000) NOT NULL,
    sharepoint_folder_path       VARCHAR(1000) NOT NULL,
    sharepoint_file_name         VARCHAR(500) NOT NULL,
    file_format                  VARCHAR(20) NOT NULL, -- csv, xlsx
    worksheet_name               VARCHAR(200) NULL,
    entity_name                  VARCHAR(200) NOT NULL,
    keyvault_linked_service      VARCHAR(200) NOT NULL,
    app_id_secret_name           VARCHAR(200) NOT NULL,
    app_secret_secret_name       VARCHAR(200) NOT NULL,
    tenant_id_secret_name        VARCHAR(200) NOT NULL,
    target_container             VARCHAR(100) NOT NULL DEFAULT 'landing',
    target_base_folder           VARCHAR(1000) NOT NULL DEFAULT 'sharepoint_master',
    remove_spaces_from_folder    BIT NOT NULL DEFAULT 1,
    target_file_format           VARCHAR(20) NOT NULL DEFAULT 'parquet',
    load_pattern                 VARCHAR(50) NOT NULL DEFAULT 'full',
    is_active                    BIT NOT NULL DEFAULT 1,
    created_at_utc               DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at_utc               DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
