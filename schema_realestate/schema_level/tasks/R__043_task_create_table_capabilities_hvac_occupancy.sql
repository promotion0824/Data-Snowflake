-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks to persist views as tables in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;
CREATE OR REPLACE TASK transformed.create_table_transformed_capabilities_hvac_occupancy_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_twins_stream_tk
AS
	CREATE OR REPLACE TABLE transformed.capabilities_hvac_occupancy 
	AS 
		SELECT * 
		FROM transformed.capabilities_hvac_occupancy_v
;    
    
ALTER TASK transformed.create_table_transformed_capabilities_hvac_occupancy_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;