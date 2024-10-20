-----------------------------------------------------------------------------------------------------
-- Task that triggers comfort_merge_comfort_hourly_metrics_sp data refresh
-- Executed after task transformed.comfort_insert_comfort_measurements_tk
-- --------------------------------------------------------------------------------------------------

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
ALTER TASK IF EXISTS transformed.comfort_merge_comfort_setpoints_trigger_tk SUSPEND;

CREATE OR REPLACE TASK transformed.comfort_merge_comfort_hourly_metrics_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.comfort_insert_comfort_measurements_tk
AS
  CALL transformed.comfort_merge_comfort_hourly_metrics_sp(SYSTEM$CURRENT_USER_TASK_NAME());   
  
-- Start root task 
ALTER TASK IF EXISTS transformed.comfort_merge_comfort_hourly_metrics_tk RESUME;
ALTER TASK IF EXISTS transformed.comfort_merge_comfort_setpoints_trigger_tk RESUME;
