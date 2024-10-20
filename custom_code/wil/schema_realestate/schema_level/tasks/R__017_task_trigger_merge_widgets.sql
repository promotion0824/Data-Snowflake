-- ******************************************************************************************************************************
-- Task to trigger sproc
-- ******************************************************************************************************************************

CREATE OR REPLACE TASK transformed.merge_widgets_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- Once per day at 00:33 AM UTC
  SCHEDULE = 'USING CRON 33 0 * * * UTC'
AS
  CALL transformed.merge_widgets_sp(SYSTEM$CURRENT_USER_TASK_NAME());

ALTER TASK IF EXISTS transformed.merge_widgets_tk RESUME
;