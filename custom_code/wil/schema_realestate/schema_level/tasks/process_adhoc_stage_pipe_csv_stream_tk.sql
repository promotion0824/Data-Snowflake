-- ------------------------------------------------------------------------------------------------------------------------------
-- Create task

-- ------------------------------------------------------------------------------------------------------------------------------
USE wil_automation_db;

CREATE OR REPLACE TASK data_compliance.process_adhoc_stage_pipe_csv_stream_tk
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '15 minute'
    USER_TASK_TIMEOUT_MS = 1200000
    SUSPEND_TASK_AFTER_NUM_FAILURES = 5
    ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
    SYSTEM$STREAM_HAS_DATA('utils.ADHOC_STAGE_PIPE_CSV_STR')
AS
    CALL utils.load_from_stage_data_compliance();

-- Task is by default in 'Suspended' state, need to start it:
ALTER TASK data_compliance.process_adhoc_stage_pipe_csv_stream_tk RESUME;