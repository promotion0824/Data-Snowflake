-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;
ALTER TASK IF EXISTS transformed.create_table_building_scopes_tk SUSPEND;

CREATE OR REPLACE TASK transformed.create_table_building_scopes_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.create_table_transformed_capabilities_assets_tk
AS
	CREATE OR REPLACE TABLE transformed.building_scopes 
	AS 
		SELECT * 
		FROM transformed.building_scopes_v
;    

ALTER TASK IF EXISTS transformed.create_table_building_scopes_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;
