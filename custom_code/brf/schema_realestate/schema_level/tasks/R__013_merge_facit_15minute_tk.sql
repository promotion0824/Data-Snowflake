-----------------------------------------------------------------------------------------------------
-- Task that triggers merge_facit_15minute_tk
-- --------------------------------------------------------------------------------------------------

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
ALTER TASK transformed.merge_telemetry_stream_tk SUSPEND;
ALTER TASK transformed.merge_facit_trend_id_15minute_tk SUSPEND;

CREATE OR REPLACE TASK transformed.merge_facit_15minute_tk
   USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
   USER_TASK_TIMEOUT_MS = 1200000
   AFTER transformed.merge_facit_trend_id_15minute_tk
AS
	CALL transformed.merge_facit_15minute_sp(SYSTEM$CURRENT_USER_TASK_NAME());


ALTER TASK transformed.merge_facit_15minute_tk RESUME;
ALTER TASK transformed.merge_telemetry_stream_tk RESUME;