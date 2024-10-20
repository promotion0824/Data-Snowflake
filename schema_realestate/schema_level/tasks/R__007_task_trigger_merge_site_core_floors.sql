-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to merge the site_core_floors stream into the site_core_floors table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK raw.merge_site_core_floors_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  --  At minute 10 past every 8th hour (starting at midnight UTC)
  SCHEDULE = 'USING CRON 10 */8 * * * UTC'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('json_site_core_floors_str')
AS
  CALL transformed.merge_site_core_floors_sp(SYSTEM$CURRENT_USER_TASK_NAME())
;      
    
ALTER TASK raw.merge_site_core_floors_stream_tk RESUME;
