-----------------------------------------------------------------------------------------------------
-- Task that triggers chiller_15min data refresh
-- Executed after task transformed.trigger_insert_zone_air_temp_measurements_tk
-- --------------------------------------------------------------------------------------------------

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
-- As this is an R script the root task has to be suspended and started again in each script

ALTER TASK IF EXISTS transformed.trigger_insert_chiller_delta_measurements_tk SUSPEND;

CREATE OR REPLACE TASK transformed.trigger_merge_chiller_15min_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.trigger_insert_chiller_delta_measurements_tk
AS
  CALL transformed.merge_chiller_15min_sp(SYSTEM$CURRENT_USER_TASK_NAME());   
  
-- Start root task 
ALTER TASK IF EXISTS transformed.trigger_merge_chiller_15min_tk RESUME;
ALTER TASK IF EXISTS transformed.trigger_insert_chiller_delta_measurements_tk RESUME;

