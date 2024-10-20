-- ------------------------------------------------------------------------------------------------------------------------------
-- Task that consumes stage stream and moves data into 'transformed' layer
-- temporary workaround until we get the Auto Ingest working on the pipe.
-- once that is working, change the schedule back and suspend, and REMOVE the root task.
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_telemetry_late_arriving_twins_tk SUSPEND;
CREATE OR REPLACE TASK transformed.merge_telemetry_late_arriving_twins_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  --  At 10 minutes past 2 am UTC every day
  SCHEDULE = 'USING CRON 10 2 * * * UTC'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 2
  USER_TASK_TIMEOUT_MS = 3600000
  ERROR_INTEGRATION = error_{{ environment }}_nin
  AS 
	CALL raw.merge_telemetry_late_arriving_twins_sp();


ALTER TASK IF EXISTS transformed.merge_telemetry_late_arriving_twins_tk SUSPEND;