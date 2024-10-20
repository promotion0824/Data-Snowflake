-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_directory_core_sites_stream_tk SUSPEND;

CREATE OR REPLACE TASK transformed.merge_directory_core_sites_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  --  At minute 10 past every 8th hour (starting at midnight UTC)
  SCHEDULE = 'USING CRON 10 */8 * * * UTC'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('raw.json_directory_core_sites_str')
AS
  CALL transformed.merge_directory_core_sites_sp(SYSTEM$CURRENT_USER_TASK_NAME())
;      
    
ALTER TASK transformed.merge_directory_core_sites_stream_tk RESUME;
