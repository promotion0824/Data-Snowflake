-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer
-- ------------------------------------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = (SELECT CASE current_region()
    WHEN 'AZURE_AUSTRALIAEAST' THEN 'Australia/Sydney'
    WHEN 'AZURE_EASTUS2' THEN 'America/New_York'
    WHEN 'AZURE_WESTEUROPE' THEN 'Europe/Paris'
    ELSE NULL
    END);
SET task_schedule = 'USING CRON 2 */3 * * * ' || COALESCE($time_zone,$time_zone_default,'Etc/UTC');
CREATE OR REPLACE TASK raw.merge_insight_occurrences_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  --  At minute 10 past every 8th hour (starting at midnight UTC)
  SCHEDULE = $task_schedule
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('json_insight_occurrences_str')
AS
  CALL transformed.merge_insight_occurrences_sp(SYSTEM$CURRENT_USER_TASK_NAME())
;      
    
ALTER TASK raw.merge_insight_occurrences_stream_tk RESUME;