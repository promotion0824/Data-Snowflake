-- ------------------------------------------------------------------------------------------------------------------------------
-- Scheduled Task
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK prd_db.transformed.merge_site_volume_by_month_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = 'USING CRON 05 01 * * * UTC'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 2
  USER_TASK_TIMEOUT_MS = 1800000
  ERROR_INTEGRATION = error_prd_nin
AS
  CALL prd_db.transformed.merge_site_volume_by_month_sp();

ALTER TASK prd_db.transformed.merge_site_volume_by_month_tk RESUME;