-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer
-- every day 2 am local time
-- ------------------------------------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = 'Etc/UTC';
SET task_schedule = 'USING CRON 2 0 * * * ' || COALESCE($time_zone,$time_zone_default);

CREATE OR REPLACE TASK transformed.load_ontology_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  --ERROR_INTEGRATION = error_{{ environment }}_nin
  SCHEDULE = $task_schedule
AS
  CALL transformed.load_ontology_sp()
;      
    
ALTER TASK transformed.load_ontology_tk RESUME;
