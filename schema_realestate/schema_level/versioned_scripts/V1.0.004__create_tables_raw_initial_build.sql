-- ******************************************************************************************************************************
-- Tables for data loader   
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.stage_data_loader (
  _stage_record_id STRING,
  json_value VARIANT,
  _schema_type STRING,
  _stage_file_name STRING,
  _loader_run_id VARCHAR(36),
  _ingested_at TIMESTAMP_NTZ,
  _staged_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TRANSIENT TABLE raw.custom_customer_stage (
  _stage_record_id STRING,
  json_value VARIANT,
  _schema_type STRING,
  _stage_file_name STRING,
  _loader_run_id VARCHAR(36),
  _ingested_at TIMESTAMP_NTZ,
  _staged_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TRANSIENT TABLE raw.stage_invalid_records (
  _stage_record_id STRING,
  json_value VARIANT,
  _schema_type STRING,
  _stage_file_name STRING,
  _loader_run_id VARCHAR(36),
  _ingested_at TIMESTAMP_NTZ,
  _staged_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TRANSIENT TABLE raw.custom_customer_stage_invalid_records (
  _stage_record_id STRING,
  json_value VARIANT,
  _schema_type STRING,
  _stage_file_name STRING,
  _loader_run_id VARCHAR(36),
  _ingested_at TIMESTAMP_NTZ,
  _staged_at TIMESTAMP_NTZ
);
