-----------------------------------------------------------------------------------------------------
-- Task that triggers insert_fan_coil_unit_measurements_sp data refresh
-- --------------------------------------------------------------------------------------------------

SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = (SELECT TOP 1 default_value:DefaultTimeZone::STRING FROM transformed.site_defaults WHERE type = 'DefaultTimeZone');
SET task_schedule = 'USING CRON 0 */3 * * * ' || COALESCE($time_zone,$time_zone_default,'Etc/UTC');

CREATE OR REPLACE TASK transformed.trigger_merge_fan_coil_unit_measurements_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = $task_schedule
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CALL transformed.insert_fan_coil_unit_measurements_sp(SYSTEM$CURRENT_USER_TASK_NAME());   
  
-- Start root task 
ALTER TASK IF EXISTS transformed.trigger_merge_fan_coil_unit_measurements_tk RESUME;

