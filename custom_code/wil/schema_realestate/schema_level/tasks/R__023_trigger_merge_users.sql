-- ******************************************************************************************************************************
-- Task to trigger sproc
-- ******************************************************************************************************************************

CREATE OR REPLACE TASK transformed.merge_users_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- Once per day at 00:33 AM UTC
  SCHEDULE = 'USING CRON 28 0 * * * UTC'
WHEN
  SYSTEM$STREAM_HAS_DATA('raw.json_users_str')
AS
  CALL transformed.merge_users_sp(SYSTEM$CURRENT_USER_TASK_NAME());

ALTER TASK IF EXISTS transformed.merge_users_tk RESUME
;