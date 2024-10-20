-----------------------------------------------------------------------------------------------------
-- Task that triggers sproc
-- --------------------------------------------------------------------------------------------------

ALTER TASK IF EXISTS transformed.create_table_billed_electricity_tk SUSPEND;

CREATE OR REPLACE TASK transformed.create_table_billed_electricity_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  --  At 10 minutes past 1 am UTC every Monday
  SCHEDULE = 'USING CRON 10 1 * * Mon UTC'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 2
  USER_TASK_TIMEOUT_MS = 3600000
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
	CALL transformed.create_table_billed_electricity_sp(SYSTEM$CURRENT_USER_TASK_NAME());

ALTER TASK transformed.create_table_billed_electricity_tk RESUME;