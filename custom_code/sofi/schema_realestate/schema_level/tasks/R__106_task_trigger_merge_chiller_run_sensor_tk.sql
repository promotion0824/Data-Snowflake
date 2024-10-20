-----------------------------------------------------------------------------------------------------
-- Task that triggers data refresh
-- Executed after task transformed.trigger_merge_zone_air_temp_assets_tk
-- --------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = (SELECT TOP 1 default_value:DefaultTimeZone::STRING FROM transformed.site_defaults WHERE type = 'DefaultTimeZone');
SET task_schedule = 'USING CRON 0 */3 * * * ' || COALESCE($time_zone,$time_zone_default,'Etc/UTC');

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
-- As this is an R script the root task has to be suspended and started again in each script

ALTER TASK IF EXISTS transformed.trigger_merge_chiller_run_sensor_tk SUSPEND;

CREATE OR REPLACE TASK transformed.trigger_merge_chiller_run_sensor_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = $task_schedule
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CALL transformed.merge_chiller_run_sensor_sp(SYSTEM$CURRENT_USER_TASK_NAME()); 

ALTER TASK IF EXISTS transformed.trigger_merge_chiller_run_sensor_tk RESUME;