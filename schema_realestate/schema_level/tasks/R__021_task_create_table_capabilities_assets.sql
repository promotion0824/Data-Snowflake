-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;
ALTER TASK IF EXISTS transformed.create_table_transformed_capabilities_assets_tk SUSPEND;

CREATE OR REPLACE TASK transformed.create_table_transformed_capabilities_assets_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_twins_relationships_stream_tk
AS
	CREATE OR REPLACE TABLE transformed.capabilities_assets 
	AS 
		SELECT * 
		FROM transformed.capabilities_assets_v
;    

ALTER TASK IF EXISTS transformed.create_table_transformed_capabilities_assets_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;
