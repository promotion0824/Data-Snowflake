-- ------------------------------------------------------------------------------------------------------------------------------
-- Tasks that consume stage streams and move data into 'transformed' layer
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk SUSPEND;
CREATE OR REPLACE TASK transformed.merge_twins_relationships_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER transformed.merge_twins_stream_tk
AS
  CALL raw.merge_twins_relationships_stream_sp();    
ALTER TASK IF EXISTS transformed.merge_twins_relationships_stream_tk RESUME;
ALTER TASK IF EXISTS transformed.merge_twins_stream_tk RESUME;