-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks to persist views as tables in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
SET time_zone = (SELECT TOP 1 time_zone FROM transformed.sites GROUP BY time_zone ORDER BY count(*) DESC);
SET time_zone_default = (SELECT CASE current_region()
    WHEN 'AZURE_AUSTRALIAEAST' THEN 'Australia/Sydney'
    WHEN 'AZURE_EASTUS2' THEN 'America/New_York'
    WHEN 'AZURE_WESTEUROPE' THEN 'Europe/Paris'
    ELSE NULL
    END);
SET task_schedule = 'USING CRON 0 */3 * * * ' || COALESCE($time_zone,$time_zone_default,'Etc/UTC');
CREATE OR REPLACE TASK transformed.create_table_transformed_occupancy_tk
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	SCHEDULE = $task_schedule
	USER_TASK_TIMEOUT_MS = 1200000
	SUSPEND_TASK_AFTER_NUM_FAILURES = 5
	ERROR_INTEGRATION = error_{{ environment }}_nin
AS  
	CREATE OR REPLACE TABLE transformed.occupancy 
	AS 
		SELECT * 
		FROM transformed.occupancy_v
;  
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK transformed.create_table_transformed_occupancy_tk RESUME;
