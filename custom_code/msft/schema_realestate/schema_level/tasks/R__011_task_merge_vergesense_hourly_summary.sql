----------------------------------------------------------------------------------
-- Tasks that aggregate to the hourly level
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.insert_vergesense_time_series_tk SUSPEND;

CREATE OR REPLACE TASK transformed.merge_vergesense_hourly_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000  
  AFTER transformed.insert_vergesense_time_series_tk
AS
  CALL transformed.merge_vergesense_hourly_sp()
;

ALTER TASK IF EXISTS transformed.merge_vergesense_hourly_tk RESUME;
ALTER TASK IF EXISTS transformed.insert_vergesense_time_series_tk RESUME;
