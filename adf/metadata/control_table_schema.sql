/*
Control table for metadata-driven Azure Data Factory ingestion.

Design intent:
- Add or disable source objects without redeploying the whole pipeline.
- Support full and incremental loads.
- Keep extraction watermarks auditable.
- Drive Oracle-to-ADLS extraction through Self-hosted Integration Runtime.
*/

CREATE TABLE dbo.IngestionControl (
    ingestion_id            INT IDENTITY(1,1) PRIMARY KEY,
    source_system           VARCHAR(100) NOT NULL,
    source_schema           VARCHAR(100) NOT NULL,
    source_table            VARCHAR(200) NOT NULL,
    target_entity           VARCHAR(200) NOT NULL,
    load_pattern            VARCHAR(50)  NOT NULL, -- full, incremental
    watermark_column        VARCHAR(200) NULL,
    last_watermark_value    VARCHAR(100) NULL,
    primary_key_columns     VARCHAR(1000) NOT NULL,
    target_container        VARCHAR(100) NOT NULL DEFAULT 'landing',
    target_folder           VARCHAR(1000) NULL,
    file_format             VARCHAR(20) NOT NULL DEFAULT 'csv',
    run_group               VARCHAR(50) NOT NULL, -- six_daily, daily_reference, etc.
    is_active               BIT NOT NULL DEFAULT 1,
    created_at_utc          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at_utc          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.IngestionRunAudit (
    audit_id                BIGINT IDENTITY(1,1) PRIMARY KEY,
    ingestion_id            INT NOT NULL,
    pipeline_run_id         VARCHAR(100) NOT NULL,
    trigger_time_utc        DATETIME2 NOT NULL,
    rows_read               BIGINT NULL,
    rows_written            BIGINT NULL,
    status                  VARCHAR(50) NOT NULL,
    previous_watermark      VARCHAR(100) NULL,
    new_watermark           VARCHAR(100) NULL,
    error_message           VARCHAR(MAX) NULL,
    created_at_utc          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
