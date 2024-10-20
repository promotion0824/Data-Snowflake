-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_telemetry_stream_tk SUSPEND;

CREATE OR REPLACE TASK transformed.create_table_transformed_occupancy_divided_openings_hourly_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_telemetry_stream_tk
AS
	CALL transformed.merge_occupancy_divided_openings_hourly_sp()
;    

ALTER TASK IF EXISTS transformed.create_table_transformed_occupancy_divided_openings_hourly_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_telemetry_stream_tk RESUME;