-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to merge the site_core_floors stream into the site_core_floors table
-- ------------------------------------------------------------------------------------------------------------------------------

-- drop old misnamed tasks;
DROP TASK IF EXISTS transformed.SUSTAINABILITY_TRIGGER_CREATE_TABLE_SUSTAINABILITY_TWINS_TK;
DROP TASK IF EXISTS transformed.SUSTAINABILITY_TRIGGER_CREATE_TABLE_UTILITY_BILLS_TK;

CREATE OR REPLACE TASK transformed.sustainability_create_table_sustainability_twins_trigger_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- Once per day 55 minutes after 12am
  SCHEDULE = 'USING CRON 55 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CREATE OR REPLACE TABLE transformed.sustainability_twins AS SELECT * FROM transformed.sustainability_twins_v
;      
    
ALTER TASK transformed.sustainability_create_table_sustainability_twins_trigger_tk RESUME;
