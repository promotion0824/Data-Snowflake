-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that schedule stored procedure execution
-- 8 minutes after every hour
-- ------------------------------------------------------------------------------------------------------------------------------
--SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = 'Etc/UTC';
SET task_schedule = 'USING CRON 8 * * * * ' || COALESCE($time_zone,$time_zone_default);

CREATE OR REPLACE TASK transformed.merge_tou_hhng_spot_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
  SCHEDULE = $task_schedule
AS
  CALL transformed.merge_tou_hhng_spot_sp()
;      

ALTER TASK transformed.merge_tou_hhng_spot_tk RESUME;
