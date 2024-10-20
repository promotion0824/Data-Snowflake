----------------------------------------------------------------------------------
-- Tasks that aggregate to the hourly level
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK transformed.insert_vergesense_time_series_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '180 minute' --  Every 3 hours
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CALL transformed.insert_vergesense_time_series_sp()
;

ALTER TASK transformed.insert_vergesense_time_series_tk RESUME;