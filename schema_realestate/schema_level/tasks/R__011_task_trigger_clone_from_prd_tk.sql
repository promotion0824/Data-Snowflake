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
      CREATE OR REPLACE TASK transformed.clone_from_prd_tk
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
        --  At 10 minutes past every 8th hour
        SCHEDULE = 'USING CRON 10 */3 * * * UTC'
        SUSPEND_TASK_AFTER_NUM_FAILURES = 3
        USER_TASK_TIMEOUT_MS = 3600000
        ERROR_INTEGRATION = error_{{ environment }}_nin
        AS 
        CALL transformed.clone_from_prd_sp();
        
      ALTER TASK IF EXISTS transformed.clone_from_prd_tk RESUME;
	END IF;
END;
$$