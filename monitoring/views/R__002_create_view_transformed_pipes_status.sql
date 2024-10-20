-- ------------------------------------------------------------------------------------------------------------------------------
-- Create pipes status view
-- ------------------------------------------------------------------------------------------------------------------------------
-- We could get these dynamically by iterating over snowflake.information_schema.databases and then running SYSTEM$PIPE_STATUS
-- for every pipe_name in <db_name>.information_schema.pipes but we would have to run a stored procedure.
-- Since we have fixed database names and only two pipes per database/environment, it's probably not necessary.

-- Deploy only for customer accounts
-- {% if accountType == 'customer' -%}

-- CREATE OR REPLACE VIEW monitoring_db.transformed.pipes_status AS 
--   SELECT 
--     CURRENT_ACCOUNT() AS account_name,
--     'ingest_time_series_pp' AS name,
--     'prd_db' AS database_name,
--     'raw' AS schema_name,
--     SYSDATE() AS _captured_at, 
--     SYSTEM$PIPE_STATUS('prd_db.raw.ingest_time_series_pp') AS pipe_status
    
--   UNION ALL
  
--   SELECT 
--     CURRENT_ACCOUNT() AS account_name,
--     'ingest_time_series_pp' AS name,
--     'uat_db' AS database_name,
--     'raw' AS schema_name,
--     SYSDATE() AS _captured_at, 
--     SYSTEM$PIPE_STATUS('uat_db.raw.ingest_time_series_pp') AS pipe_status
    
--   UNION ALL
  
--   SELECT 
--     CURRENT_ACCOUNT() AS account_name,
--     'ingest_time_series_pp' AS name,
--     'dev_db' AS database_name,
--     'raw' AS schema_name,
--     SYSDATE() AS _captured_at, 
--     SYSTEM$PIPE_STATUS('dev_db.raw.ingest_time_series_pp') AS pipe_status  
  
--   UNION ALL
  
--   SELECT 
--     CURRENT_ACCOUNT() AS account_name,
--     'ingest_raw_from_ext_stage_pp' AS name,
--     'prd_db' AS database_name,
--     'raw' AS schema_name,
--     SYSDATE() AS _captured_at, 
--     SYSTEM$PIPE_STATUS('prd_db.raw.ingest_raw_from_ext_stage_pp') AS pipe_status
    
--   UNION ALL
  
--   SELECT 
--     CURRENT_ACCOUNT() AS account_name,
--     'ingest_raw_from_ext_stage_pp' AS name,
--     'uat_db' AS database_name,
--     'raw' AS schema_name,
--     SYSDATE() AS _captured_at, 
--     SYSTEM$PIPE_STATUS('uat_db.raw.ingest_raw_from_ext_stage_pp') AS pipe_status
    
--   UNION ALL
  
--   SELECT 
--     CURRENT_ACCOUNT() AS account_name,
--     'ingest_raw_from_ext_stage_pp' AS name,
--     'dev_db' AS database_name,
--     'raw' AS schema_name,
--     SYSDATE() AS _captured_at, 
--     SYSTEM$PIPE_STATUS('dev_db.raw.ingest_raw_from_ext_stage_pp') AS pipe_status   
-- ;

-- {%- endif %}

USE ROLE {{ defaultRole }};