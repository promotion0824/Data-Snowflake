-- ******************************************************************************************************************************
-- Task to trigger sproc
-- ******************************************************************************************************************************

CREATE OR REPLACE TASK transformed.merge_connectors_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- Once per day at 1:30 AM UTC
  SCHEDULE = 'USING CRON 30 1 * * * UTC'
AS
  CALL transformed.merge_connectors_sp();

ALTER TASK IF EXISTS transformed.merge_connectors_tk RESUME
;