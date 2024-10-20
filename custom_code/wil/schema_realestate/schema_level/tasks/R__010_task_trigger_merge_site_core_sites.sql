-- ******************************************************************************************************************************
-- Task to trigger stored procedure
-- ******************************************************************************************************************************

CREATE OR REPLACE TASK transformed.merge_site_core_sites_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- Once per day at 10 p.m.
  SCHEDULE = 'USING CRON 4 22 * * * UTC'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CALL transformed.merge_site_core_sites_sp('');

ALTER TASK IF EXISTS transformed.merge_site_core_sites_tk RESUME
;