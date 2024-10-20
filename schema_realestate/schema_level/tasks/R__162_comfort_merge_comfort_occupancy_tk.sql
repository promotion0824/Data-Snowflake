-----------------------------------------------------------------------------------------------------
-- Task that triggers merge_zocomfort_merge_occupancy_spne_air_occupancy_sp data refresh
-- Executed after task transformed.comfort_trigger_merge_comfort_setpoints_tk
-- --------------------------------------------------------------------------------------------------

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
ALTER TASK IF EXISTS TRANSFORMED.comfort_merge_comfort_setpoints_trigger_tk suspend;

CREATE OR REPLACE TASK transformed.comfort_merge_comfort_occupancy_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 2400000
  AFTER transformed.comfort_merge_comfort_setpoints_trigger_tk
AS
  CALL transformed.comfort_merge_occupancy_sp(SYSTEM$CURRENT_USER_TASK_NAME());   
  
ALTER TASK transformed.comfort_merge_comfort_occupancy_tk RESUME;
ALTER TASK IF EXISTS transformed.comfort_merge_comfort_setpoints_trigger_tk RESUME;