-- ******************************************************************************************************************************
-- Task to trigger stored procedure
-- ******************************************************************************************************************************

CREATE OR REPLACE TASK transformed.merge_sites_hourly_temperature_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- Every hour; 15 minutes after the hour
  SCHEDULE = 'USING CRON 15 * * * * UTC'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CALL transformed.merge_sites_hourly_temperature_sp();

ALTER TASK IF EXISTS transformed.merge_sites_hourly_temperature_tk RESUME
;