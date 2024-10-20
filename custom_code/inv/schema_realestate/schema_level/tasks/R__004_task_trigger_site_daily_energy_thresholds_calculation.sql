-- ------------------------------------------------------------------------------------------------------------------------------
-- Trigger daily site energy threshold calculation
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK utils.trigger_site_daily_energy_thresholds_calculation_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- This task is scheduled to run at 1:00am first day of every month (Australia/Sydney time zone)
  SCHEDULE = 'USING CRON 0 1 1 * * Australia/Sydney'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  -- Calculate energy thresholds for current month
  CALL utils.calculate_site_daily_energy_thresholds_sp(
    TO_DATE(DATE_TRUNC('MONTH', CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP()))), 
    LAST_DAY(CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP()), 'MONTH'),
    SYSTEM$CURRENT_USER_TASK_NAME()
  ); 
  
-- Task is by default in 'Suspended' state, need to start it:
ALTER TASK utils.trigger_site_daily_energy_thresholds_calculation_tk SUSPEND;