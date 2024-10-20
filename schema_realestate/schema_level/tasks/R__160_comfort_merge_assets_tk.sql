-----------------------------------------------------------------------------------------------------
-- Task that triggers zone_air_temp_assets data refresh
-- --------------------------------------------------------------------------------------------------

-- Suspend task first if it exists.
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;

CREATE OR REPLACE TASK transformed.comfort_merge_assets_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.create_table_transformed_capabilities_assets_tk
AS
    CREATE OR REPLACE TABLE transformed.comfort_assets AS 
       SELECT * FROM transformed.comfort_assets_v;


-- Start task
ALTER TASK transformed.comfort_merge_assets_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;