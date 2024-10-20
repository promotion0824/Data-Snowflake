-----------------------------------------------------------------------------------------------------
-- Task that triggers zone_air_temp_measurements data refresh
-- Executed after task transformed.comfort_trigger_merge_comfort_setpoints_tk
-- --------------------------------------------------------------------------------------------------

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
ALTER TASK IF EXISTS transformed.comfort_merge_comfort_setpoints_trigger_tk suspend;

CREATE OR REPLACE TASK transformed.comfort_merge_setpoint_offsets_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.comfort_merge_comfort_occupancy_tk
AS
  CALL transformed.comfort_merge_setpoint_offset_sp(SYSTEM$CURRENT_USER_TASK_NAME());   
  
ALTER TASK transformed.comfort_merge_setpoint_offsets_tk RESUME;
ALTER TASK IF EXISTS transformed.comfort_merge_comfort_setpoints_trigger_tk RESUME;