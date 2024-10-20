-----------------------------------------------------------------------------------------------------
-- Task that triggers chiller_assets data refresh
-- This is also a root task that kicks-off all subsequent comfort score related tables refresh
-- Executed on schedule
-- As this is a root task it needs to stay in 'Suspended' state until all child tasks are created.
-- --------------------------------------------------------------------------------------------------

-- Suspend task first if it exists.
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;
ALTER TASK IF EXISTS transformed.create_table_transformed_capabilities_assets_tk SUSPEND;
ALTER TASK IF EXISTS transformed.trigger_merge_chiller_assets_tk SUSPEND;

CREATE OR REPLACE TASK transformed.trigger_merge_chiller_assets_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000  
  AFTER transformed.create_table_transformed_capabilities_assets_tk
AS
  CREATE OR REPLACE TRANSIENT TABLE transformed.chiller_assets AS 
	SELECT * FROM transformed.chiller_assets_v;

-- Start task
ALTER TASK transformed.trigger_merge_chiller_assets_tk RESUME;
ALTER TASK IF EXISTS transformed.create_table_transformed_capabilities_assets_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;