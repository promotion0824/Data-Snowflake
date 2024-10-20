-- ------------------------------------------------------------------------------------------------------------------------------
-- Task that consumes stage stream and moves data into 'transformed' layer
-- temporary workaround until we get the Auto Ingest working on the pipe.
-- once that is working, change the schedule back and suspend, and REMOVE the root task.
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK transformed.merge_telemetry_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'SMALL'
  SCHEDULE = '20 minute'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  USER_TASK_TIMEOUT_MS = 2400000
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('raw.stage_telemetry_str')
  AS 
	CALL raw.merge_telemetry_sp();
	
{% if environment|lower == 'prd' %}
ALTER TASK IF EXISTS transformed.merge_telemetry_stream_tk RESUME;
{% endif %}
