-----------------------------------------------------------------------------------------------------
-- Task that triggers populating of site_defaults table
-- This should run after task raw.merge_directory_core_sites_stream_tk
-- --------------------------------------------------------------------------------------------------

-- Suspend root task if it exists otherwise (re)deployment of subsequent tasks fails.
ALTER TASK transformed.merge_directory_core_sites_stream_tk SUSPEND;

CREATE OR REPLACE TASK transformed.trigger_insert_site_defaults_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_directory_core_sites_stream_tk
AS
  CALL transformed.insert_site_defaults_sp(SYSTEM$CURRENT_USER_TASK_NAME());   
  
ALTER TASK transformed.trigger_insert_site_defaults_tk RESUME;
ALTER TASK transformed.merge_directory_core_sites_stream_tk RESUME;
