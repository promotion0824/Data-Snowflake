-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to persist view as table in transformed
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_directory_core_sites_stream_tk SUSPEND;

CREATE OR REPLACE TASK transformed.create_table_transformed_sites_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_directory_core_sites_stream_tk
AS
	CREATE OR REPLACE TABLE transformed.sites 
	AS 
		SELECT * 
		FROM transformed.sites_v
;    
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.create_table_transformed_sites_tk RESUME;
ALTER TASK transformed.merge_directory_core_sites_stream_tk RESUME;
