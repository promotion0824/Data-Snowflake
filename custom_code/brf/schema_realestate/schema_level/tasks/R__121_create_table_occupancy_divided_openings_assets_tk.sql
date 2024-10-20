-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;

CREATE OR REPLACE TASK transformed.create_table_transformed_occupancy_divided_openings_assets_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.create_table_transformed_capabilities_assets_tk
AS
	CREATE OR REPLACE TABLE transformed.occupancy_divided_openings_assets 
	AS 
		SELECT * 
		FROM transformed.occupancy_divided_openings_assets_v
;    

ALTER TASK IF EXISTS transformed.create_table_transformed_occupancy_divided_openings_assets_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;