-----------------------------------------------------------------------------------------------------
-- Task that triggers zone_air_temp_measurements data refresh
-- Executed after task transformed.trigger_merge_zone_air_temp_setpoints_tk
-- --------------------------------------------------------------------------------------------------

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
-- As this is an R script the root task has to be suspended and started again in each script
ALTER TASK IF EXISTS transformed.trigger_merge_chiller_run_sensor_tk suspend;

CREATE OR REPLACE TASK transformed.trigger_insert_chiller_delta_measurements_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.trigger_merge_chiller_run_sensor_tk
AS
  CALL transformed.insert_chiller_delta_measurements_sp(SYSTEM$CURRENT_USER_TASK_NAME());   
  
ALTER TASK transformed.trigger_insert_chiller_delta_measurements_tk RESUME;
ALTER TASK IF EXISTS transformed.trigger_merge_chiller_run_sensor_tk RESUME;