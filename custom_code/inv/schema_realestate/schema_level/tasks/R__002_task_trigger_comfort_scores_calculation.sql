-- ------------------------------------------------------------------------------------------------------------------------------
-- Trigger comfort score daily calculation
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK utils.trigger_comfort_scores_calculation_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  -- This task is scheduled to run at 0:30am every day 
  -- from Tuesday through Saturday (Australia/Sydney time zone)
  -- That will ensure that ComfortScore is calculated for every weekday (Mon-Fri)
  SCHEDULE = 'USING CRON 30 0 * * 2-6 Australia/Sydney'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
AS
  -- Calculate comfort scores for previous day
  CALL utils.calculate_comfort_scores_sp(
    TO_DATE(DATEADD(DAY, -1, CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP()))), 
    TO_DATE(DATEADD(DAY, -1, CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP()))),
    SYSTEM$CURRENT_USER_TASK_NAME()
  );   
  
-- Task is by default in 'Suspended' state, need to start it:
ALTER TASK utils.trigger_comfort_scores_calculation_tk SUSPEND;