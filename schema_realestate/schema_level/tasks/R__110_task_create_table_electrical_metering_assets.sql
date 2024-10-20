-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed

-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;


CREATE OR REPLACE TASK transformed.electrical_metering_assets_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.create_table_transformed_capabilities_assets_tk
AS

CALL transformed.transformed_create_table_electrical_metering_assets_sp()
;    

ALTER TASK IF EXISTS transformed.electrical_metering_assets_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;