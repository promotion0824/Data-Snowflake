-- ******************************************************************************************************************************
-- Task to trigger sproc
-- ******************************************************************************************************************************

CREATE OR REPLACE TASK transformed.merge_customers_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- Once per day at 1:35 AM UTC
  SCHEDULE = 'USING CRON 35 1 * * * UTC'
AS
  CALL transformed.merge_customers_sp();

ALTER TASK IF EXISTS transformed.merge_customers_tk RESUME
;