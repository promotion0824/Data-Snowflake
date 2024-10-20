-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer
-- Needs to go into transformed schema because of all the dependent tasks
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK transformed.occupancy_merge_telemetry_stream_trigger_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '45 minute'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('transformed.telemetry_str_occupancy')
AS
  CALL transformed.occupancy_merge_telemetry_stream_sp(SYSTEM$CURRENT_USER_TASK_NAME());  
    
ALTER TASK transformed.occupancy_merge_telemetry_stream_trigger_tk RESUME;