-- ------------------------------------------------------------------------------------------------------------------------------
-- Clone from PRD to UAT
-- ------------------------------------------------------------------------------------------------------------------------------

EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='UAT_DB') THEN
      CREATE OR REPLACE TASK raw.clone_from_prd_raw_tk
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
        --  At 10 minutes past every 8th hour
        SCHEDULE = 'USING CRON 10 */3 * * * UTC'
        SUSPEND_TASK_AFTER_NUM_FAILURES = 3
        USER_TASK_TIMEOUT_MS = 3600000
        ERROR_INTEGRATION = error_{{ environment }}_nin
        AS 
        CALL raw.clone_from_prd_raw_sp();
        
      ALTER TASK IF EXISTS raw.clone_from_prd_raw_tk RESUME;
	END IF;
END;
$$