-- ******************************************************************************************************************************
-- Staging tables
-- ******************************************************************************************************************************
CREATE OR REPLACE TRANSIENT TABLE raw.json_total_elec_energy_sensors (
  _stage_record_id STRING,
  json_value VARIANT,
  _stage_file_name STRING,
  _loader_run_id VARCHAR(36),
  _ingested_at TIMESTAMP_NTZ,
  _staged_at TIMESTAMP_NTZ
);