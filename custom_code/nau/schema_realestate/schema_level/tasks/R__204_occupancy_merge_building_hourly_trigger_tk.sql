-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed to improve query performance

-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.occupancy_merge_building_hourly_tk SUSPEND;


CREATE OR REPLACE TASK transformed.occupancy_merge_building_hourly_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '40 minute'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS

    CALL transformed.occupancy_merge_building_hourly_sp()
;    

ALTER TASK IF EXISTS transformed.occupancy_merge_building_hourly_tk RESUME;
