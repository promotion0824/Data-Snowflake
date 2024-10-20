-- ------------------------------------------------------------------------------------------------------------------------------
-- Scheduled Task
-- ------------------------------------------------------------------------------------------------------------------------------

      CREATE OR REPLACE TASK transformed.merge_connectors_tk
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
        SCHEDULE = 'USING CRON 05 19 1 1-12 * UTC'
        SUSPEND_TASK_AFTER_NUM_FAILURES = 2
        USER_TASK_TIMEOUT_MS = 1800000
        ERROR_INTEGRATION = error_{{ environment }}_nin
      WHEN
        SYSTEM$STREAM_HAS_DATA('raw.json_connectors_str')
      AS
        CALL transformed.merge_connectors_sp();
          
      ALTER TASK transformed.merge_connectors_tk RESUME;
