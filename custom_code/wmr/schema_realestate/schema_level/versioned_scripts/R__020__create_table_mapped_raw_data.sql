-- ******************************************************************************************************************************
-- Tables for data loader   
-- ******************************************************************************************************************************

CREATE TRANSIENT TABLE IF NOT EXISTS raw.stage_mapped_raw_data (
  _stage_record_id STRING,
  json_value VARIANT,
  _schema_type STRING,
  _stage_file_name STRING,
  _loader_run_id VARCHAR(36),
  _ingested_at TIMESTAMP_NTZ,
  _staged_at TIMESTAMP_NTZ
);