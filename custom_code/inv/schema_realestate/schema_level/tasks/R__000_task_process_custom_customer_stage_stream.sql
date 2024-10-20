
-- ------------------------------------------------------------------------------------------------------------------------------
-- Process custom customer data staging stream (root task)
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK raw.process_custom_customer_stage_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '20 minute'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('custom_customer_stage_str')
AS
  INSERT ALL
    WHEN _schema_type = 'Custom/Willow-Api/DigitalTwin/ActiveElectricalEnergySensor-isCapabilityOf-Building' THEN
      INTO raw.json_total_elec_energy_sensors VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)         
    ELSE
      INTO raw.custom_customer_stage_invalid_records (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at) 
      VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)

  SELECT _schema_type, _stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at
  FROM raw.custom_customer_stage_str;

ALTER TASK raw.process_custom_customer_stage_stream_tk RESUME;

-- ------------------------------------------------------------------------------------------------------------------------------

  CREATE OR REPLACE STREAM raw.json_total_elec_energy_sensors_str
    ON TABLE raw.json_total_elec_energy_sensors
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;  