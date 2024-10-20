-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to merge the site_core_floors stream into the site_core_floors table
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK transformed.sustainability_create_table_resources_comparison_trigger_tk
  WAREHOUSE = prd_elt_medium_wh
  -- Once per day 8 minutes after 1am
  SCHEDULE = 'USING CRON 8 1 * * * UTC'
  USER_TASK_TIMEOUT_MS = 1800000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  CALL transformed.sustainability_create_table_resources_comparison_sp()
;      
    
ALTER TASK transformed.sustainability_create_table_resources_comparison_trigger_tk RESUME;