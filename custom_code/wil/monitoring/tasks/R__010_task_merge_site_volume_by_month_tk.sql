-- ------------------------------------------------------------------------------------------------------------------------------
-- Scheduled Task
-- ------------------------------------------------------------------------------------------------------------------------------

EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='PRD_DB') THEN
      CREATE OR REPLACE TASK transformed.merge_site_volume_by_month_tk
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
        SCHEDULE = 'USING CRON 05 19 1 1-12 * UTC'
        SUSPEND_TASK_AFTER_NUM_FAILURES = 2
        USER_TASK_TIMEOUT_MS = 1800000
      ERROR_INTEGRATION = error_{{ environment }}_nin
  WHEN
    SYSTEM$STREAM_HAS_DATA('central_monitoring_db.raw.site_volume_by_month_str')
      AS
        CALL monitoring_db.transformed.merge_site_volume_by_month_sp();
          
      ALTER TASK transformed.merge_site_volume_by_month_tk RESUME;
	END IF;
END;
$$