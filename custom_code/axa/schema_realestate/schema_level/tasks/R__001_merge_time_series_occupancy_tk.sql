-----------------------------------------------------------------------------------------------------
-- Task that triggers every 3 hours
-- --------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone = (SELECT COALESCE($time_zone,'UTC'));
SET task_schedule = 'USING CRON 0 */3 * * * ' || $time_zone;

CREATE OR REPLACE TASK transformed.merge_time_series_occupancy_tk
   USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
   SCHEDULE = $task_schedule
   USER_TASK_TIMEOUT_MS = 1200000
   SUSPEND_TASK_AFTER_NUM_FAILURES = 5
   ERROR_INTEGRATION = error_{{ environment }}_nin
AS
	CALL transformed.merge_time_series_occupancy_sp(SYSTEM$CURRENT_USER_TASK_NAME());


ALTER TASK transformed.merge_time_series_occupancy_tk resume;

