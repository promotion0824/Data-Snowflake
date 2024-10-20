-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to merge workflow_core_tickets_str stream into workflow_core_tickets table
-- ------------------------------------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = (SELECT CASE current_region()
    WHEN 'AZURE_AUSTRALIAEAST' THEN 'Australia/Sydney'
    WHEN 'AZURE_EASTUS2' THEN 'America/New_York'
    WHEN 'AZURE_WESTEUROPE' THEN 'Europe/Paris'
    ELSE NULL
    END);
SET task_schedule = 'USING CRON 0 */3 * * * ' || COALESCE($time_zone,$time_zone_default,'Etc/UTC','Etc/UTC');

CREATE OR REPLACE TASK raw.merge_workflow_core_tickets_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  --  At minute 10 past every 8th hour (starting at midnight UTC)
  SCHEDULE = $task_schedule
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('json_workflow_core_tickets_str')
AS
  CALL transformed.merge_workflow_core_tickets_sp(SYSTEM$CURRENT_USER_TASK_NAME())
;      
    
ALTER TASK raw.merge_workflow_core_tickets_stream_tk RESUME;
