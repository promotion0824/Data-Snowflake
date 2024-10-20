-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer
-- Needs to go into transformed schema because of all the dependent tasks
-- ------------------------------------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = (SELECT CASE current_region()
    WHEN 'AZURE_AUSTRALIAEAST' THEN 'Australia/Sydney'
    WHEN 'AZURE_EASTUS2' THEN 'America/New_York'
    WHEN 'AZURE_WESTEUROPE' THEN 'Europe/Paris'
    ELSE NULL
    END);
SET task_schedule = 'USING CRON 7 */3 * * * ' || COALESCE($time_zone,$time_zone_default,'Etc/UTC');
CREATE OR REPLACE TASK transformed.merge_twins_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = $task_schedule
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
     SYSTEM$STREAM_HAS_DATA('raw.json_twins_str')
  OR SYSTEM$STREAM_HAS_DATA('raw.json_twins_relationships_str')
AS
  CALL raw.merge_twins_stream_sp();  
    
ALTER TASK transformed.merge_twins_stream_tk RESUME;