-- ------------------------------------------------------------------------------------------------------------------------------
-- Task to call the stored procedure to merge the insights stream into the insights table
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TASK raw.merge_insights_stream_tk SUSPEND;

CREATE OR REPLACE TASK raw.merge_impact_scores_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  USER_TASK_TIMEOUT_MS = 1200000
  AFTER raw.merge_insights_stream_tk
WHEN
  SYSTEM$STREAM_HAS_DATA('json_impact_scores_str')
AS
  CALL transformed.merge_impact_scores_sp(SYSTEM$CURRENT_USER_TASK_NAME())
;      
    
ALTER TASK raw.merge_impact_scores_stream_tk RESUME;
ALTER TASK raw.merge_insights_stream_tk RESUME;
