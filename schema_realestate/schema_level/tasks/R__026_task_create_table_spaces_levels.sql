-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;

CREATE OR REPLACE TASK transformed.create_table_transformed_spaces_levels_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_twins_stream_tk
AS
	CREATE OR REPLACE TABLE transformed.spaces_levels 
	AS 
		SELECT * 
		FROM transformed.spaces_levels_v
;    
    
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK transformed.create_table_transformed_spaces_levels_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;