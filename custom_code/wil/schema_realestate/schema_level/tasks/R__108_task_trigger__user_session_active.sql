-- ******************************************************************************************************************************
-- Task to trigger stored procedure to persist the tenant engagement data
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE TASK transformed.trigger_user_session_active_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- Twice per day 10 a.m. and 10 p.m.
  SCHEDULE = 'USING CRON 4 10,22 * * * UTC'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CALL transformed.insert_user_session_active_sp();

--ALTER TASK IF EXISTS transformed.trigger_user_session_active_tk RESUME
ALTER TASK IF EXISTS transformed.trigger_user_session_active_tk SUSPEND
;