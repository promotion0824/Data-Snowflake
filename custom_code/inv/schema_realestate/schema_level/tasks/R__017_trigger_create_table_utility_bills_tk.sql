-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to merge the site_core_floors stream into the site_core_floors table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK transformed.create_table_utility_bills_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'MEDIUM'
  -- Once per day 8 minutes after 1am
  SCHEDULE = 'USING CRON 8 1 * * * UTC'
  USER_TASK_TIMEOUT_MS = 2400000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CALL transformed.create_table_utility_bills_sp()
;      
-- we don't need this one. It doesn't look like it is being used; if it is needed; sustainability_utlity_bills From standard code should  be used instead.
-- also we have billed_electricity (in use) from custom_code for this customer 
ALTER TASK transformed.create_table_utility_bills_tk SUSPEND;
