-- ------------------------------------------------------------------------------------------------------------------------------
-- Trigger daily site energy usage calculation
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK utils.trigger_site_daily_electrical_energy_usage_calculation_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- This task is scheduled to run at 0:30am every day Australia/Sydney time zone)
  SCHEDULE = 'USING CRON 30 0 * * * Australia/Sydney' 
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  -- Calculate comfort scores for previous day
  CALL utils.calculate_site_daily_electrical_energy_usage_sp(
    TO_DATE(DATEADD(DAY, -1, CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP()))), 
    TO_DATE(DATEADD(DAY, -1, CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP()))),
    SYSTEM$CURRENT_USER_TASK_NAME()
  ); 
  
-- Task is by default in 'Suspended' state, need to start it:
ALTER TASK utils.trigger_site_daily_electrical_energy_usage_calculation_tk SUSPEND;