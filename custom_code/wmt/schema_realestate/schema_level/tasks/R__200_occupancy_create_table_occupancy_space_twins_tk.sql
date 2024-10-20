-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed to improve query performance

-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;


CREATE OR REPLACE TASK transformed.occupancy_create_table_occupancy_space_twins_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.create_table_transformed_capabilities_assets_tk
AS

CREATE OR REPLACE TABLE transformed.occupancy_space_twins AS SELECT * FROM transformed.occupancy_space_twins_v
;    

ALTER TASK IF EXISTS transformed.occupancy_create_table_occupancy_space_twins_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;
