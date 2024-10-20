-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that schedule stored procedure execution
--  12 minutes after each hour
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK transformed.sustainability_create_table_tou_electrical_emissions_trigger_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
  SCHEDULE = 'USING CRON 15 * * * * America/New_York'
AS
  CREATE OR REPLACE TABLE transformed.sustainability_tou_electrical_emissions AS 
      SELECT * FROM transformed.sustainability_tou_electrical_emissions_v
;      

ALTER TASK transformed.sustainability_create_table_tou_electrical_emissions_trigger_tk RESUME;
