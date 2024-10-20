-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed to improve query performance

-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.occupancy_merge_telemetry_stream_trigger_tk SUSPEND;


CREATE OR REPLACE TASK transformed.occupancy_merge_space_hourly_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.occupancy_merge_telemetry_stream_trigger_tk
AS

    CALL transformed.occupancy_merge_space_hourly_sp()
;    

ALTER TASK IF EXISTS transformed.occupancy_merge_space_hourly_tk RESUME;
ALTER TASK IF EXISTS transformed.occupancy_merge_telemetry_stream_trigger_tk RESUME;
