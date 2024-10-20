-- ------------------------------------------------------------------------------------------------------------------------------
-- Process staging stream (root task)
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK raw.process_stage_data_loader_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '20 minute'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('stage_data_loader_str')
AS
  INSERT ALL
    WHEN _schema_type = 'DirectoryCore/Sites' THEN
      INTO raw.json_directory_core_sites VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'WorkflowCore/Tickets' THEN
      INTO raw.json_workflow_core_tickets VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'InsightDB/Insights' THEN
      INTO raw.json_insights VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'SiteCore/Floors' THEN
      INTO raw.json_site_core_floors VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'Willow/DigitalTwin/Adx/Twins' THEN
      INTO raw.json_twins VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'Willow/DigitalTwin/Adx/TwinsRelationships' THEN
      INTO raw.json_twins_relationships VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'Willow/DigitalTwin/Adx/TwinsValidationResults' THEN
      INTO raw.json_twins_validation_results VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'Willow/DigitalTwin/Adx/TwinsValidationAggregatedScores' THEN
      INTO raw.json_twins_validation_aggregate_scores VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'Willow/DigitalTwin/Adx/TwinsStaticValidationScores' THEN
      INTO raw.json_twins_static_validation_scores VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type = 'Willow/DigitalTwin/Adx/TwinsValidationConnectivityScores' THEN
      INTO raw.json_twins_validation_connectivity_scores VALUES (_stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
    WHEN _schema_type LIKE 'Custom%' THEN
      INTO raw.custom_customer_stage (_stage_record_id, json_value, _schema_type, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
      VALUES (_stage_record_id, json_value, _schema_type, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)            
    ELSE
      INTO raw.stage_invalid_records (_stage_record_id, json_value, _schema_type, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
      VALUES (_stage_record_id, json_value, _schema_type, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)

  SELECT _schema_type, _stage_record_id, json_value, _stage_file_name, _loader_run_id, _ingested_at, _staged_at
  FROM raw.stage_data_loader_str;


-- Task is by default in 'Suspended' state, need to start it:
ALTER TASK raw.process_stage_data_loader_stream_tk RESUME;
