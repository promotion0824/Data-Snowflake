-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;

CREATE OR REPLACE TASK transformed.create_table_transformed_levels_buildings_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_twins_stream_tk
AS
	CREATE OR REPLACE TABLE transformed.levels_buildings 
	AS 
		SELECT * 
		FROM transformed.levels_buildings_v
;    
    
ALTER TASK transformed.create_table_transformed_levels_buildings_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;