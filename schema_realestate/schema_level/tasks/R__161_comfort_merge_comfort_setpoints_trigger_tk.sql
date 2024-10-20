-----------------------------------------------------------------------------------------------------
-- Task that triggers comfort_setpoints data refresh
-- --------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.buildings GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = (SELECT CASE current_region()
    WHEN 'AZURE_AUSTRALIAEAST' THEN 'Australia/Sydney'
    WHEN 'AZURE_EASTUS2' THEN 'America/New_York'
    WHEN 'AZURE_WESTEUROPE' THEN 'Europe/Paris'
    ELSE NULL
    END);
SET task_schedule = 'USING CRON 0 */3 * * * ' || COALESCE($time_zone,$time_zone_default,'Etc/UTC');

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
ALTER TASK IF EXISTS transformed.comfort_merge_comfort_setpoints_trigger_tk SUSPEND;

CREATE OR REPLACE TASK transformed.comfort_merge_comfort_setpoints_trigger_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = $task_schedule
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
WHEN
  SYSTEM$STREAM_HAS_DATA('transformed.comfort_telemetry_str')
AS
  CALL transformed.comfort_merge_setpoints_sp(SYSTEM$CURRENT_USER_TASK_NAME()); 

{% if environment|lower == 'prd' %}

    ALTER TASK IF EXISTS transformed.comfort_merge_comfort_setpoints_trigger_tk SET ERROR_INTEGRATION = error_{{ environment }}_nin;
    ALTER TASK IF EXISTS transformed.comfort_merge_comfort_setpoints_trigger_tk RESUME;

{% endif %}