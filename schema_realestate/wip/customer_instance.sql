-- USE ROLE {{ defaultRole }};
-- USE WAREHOUSE dev_wh;

-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Create database
-- -- ------------------------------------------------------------------------------------------------------------------------------
-- CREATE DATABASE IF NOT EXISTS monitoring_db;

-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Create schema
-- -- ------------------------------------------------------------------------------------------------------------------------------
-- CREATE SCHEMA IF NOT EXISTS transformed;

-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Create Users and roles
-- -- ------------------------------------------------------------------------------------------------------------------------------
-- USE ROLE SECURITYADMIN;

-- CREATE USER IF NOT EXISTS monitoring_pipeline_usr
--   LOGIN_NAME   = 'monitoring_pipeline_usr'
--   DEFAULT_ROLE = monitoring
--   DEFAULT_WAREHOUSE = monitoring_pipeline_wh
--   PASSWORD = '';   -- deploy manually with real password (stored in lastpass and key vault); 
  
-- -- ALTER USER monitoring_pipeline_usr SET PASSWORD = '';
  
-- -- DROP ROLE IF EXISTS monitoring_pipeline;

-- CREATE ROLE IF NOT EXISTS monitoring_pipeline;
-- GRANT ROLE monitoring_pipeline TO USER monitoring_pipeline_usr;
-- GRANT ROLE monitoring_pipeline TO ROLE SYSADMIN;

-- USE ROLE ACCOUNTADMIN;
-- GRANT MONITOR USAGE ON ACCOUNT TO ROLE monitoring_pipeline;
-- GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE monitoring_pipeline;

-- USE ROLE SECURITYADMIN;
-- GRANT USAGE ON DATABASE dev_db TO ROLE monitoring_pipeline;
-- GRANT USAGE ON DATABASE uat_db TO ROLE monitoring_pipeline;
-- GRANT USAGE ON DATABASE prd_db TO ROLE monitoring_pipeline;
-- GRANT USAGE ON DATABASE monitoring_db TO ROLE monitoring_pipeline;
-- GRANT USAGE ON DATABASE _{{ customerName }} TO ROLE monitoring_pipeline;
-- GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE monitoring_pipeline;
-- GRANT USAGE ON SCHEMA monitoring_db.transformed TO ROLE monitoring_pipeline;

-- GRANT SELECT ON ALL tables IN schema monitoring_db.transformed TO ROLE monitoring_pipeline;
-- GRANT SELECT ON ALL views  IN schema monitoring_db.transformed TO ROLE monitoring_pipeline;
-- GRANT SELECT ON future tables IN schema monitoring_db.transformed TO ROLE monitoring_pipeline;
-- GRANT SELECT ON future views  IN schema monitoring_db.transformed TO ROLE monitoring_pipeline;

-- -- Permissions to monitor pipes
-- GRANT USAGE ON SCHEMA dev_db.raw  TO ROLE monitoring_pipeline;
-- GRANT MONITOR ON PIPE dev_db.raw.ingest_time_series_pp TO ROLE monitoring_pipeline;
-- GRANT MONITOR ON PIPE dev_db.raw.ingest_raw_from_ext_stage_pp TO ROLE monitoring_pipeline;

-- GRANT USAGE ON SCHEMA uat_db.raw  TO ROLE monitoring_pipeline;
-- GRANT MONITOR ON PIPE uat_db.raw.ingest_time_series_pp TO ROLE monitoring_pipeline;
-- GRANT MONITOR ON PIPE uat_db.raw.ingest_raw_from_ext_stage_pp TO ROLE monitoring_pipeline;

-- GRANT USAGE ON SCHEMA prd_db.raw  TO ROLE monitoring_pipeline;
-- GRANT MONITOR ON PIPE prd_db.raw.ingest_time_series_pp TO ROLE monitoring_pipeline;
-- GRANT MONITOR ON PIPE prd_db.raw.ingest_raw_from_ext_stage_pp TO ROLE monitoring_pipeline;

-- USE ROLE {{ defaultRole }};

-- -- ------------------------------------------------------------------------------------------------------------------------------
-- -- Create Warehouse
-- -- ------------------------------------------------------------------------------------------------------------------------------
-- USE ROLE {{ defaultRole }};

-- CREATE WAREHOUSE IF NOT EXISTS  monitoring_pipeline_wh WITH 
--     WAREHOUSE_SIZE = 'XSMALL' 
--     AUTO_SUSPEND = 60 
--     AUTO_RESUME = TRUE 
--     MIN_CLUSTER_COUNT = 1 
--     MAX_CLUSTER_COUNT = 1
--     SCALING_POLICY = 'ECONOMY' 
--     INITIALLY_SUSPENDED = TRUE
--     COMMENT = 'Warehouse used for monitoring and logs pipeline.'
-- ;

-- USE ROLE ACCOUNTADMIN;

-- -- We don't want to suspend the monitoring warehouse automatically
-- CREATE RESOURCE MONITOR IF NOT EXISTS  monitoring_pipeline_rm WITH  CREDIT_QUOTA = 50
--     FREQUENCY = MONTHLY
--     START_TIMESTAMP = IMMEDIATELY
--     TRIGGERS ON 50 PERCENT DO NOTIFY
--     		 ON 75 PERCENT DO NOTIFY
--              ON 90 PERCENT DO NOTIFY
--              ON 100 PERCENT DO NOTIFY;

-- ALTER WAREHOUSE monitoring_pipeline_wh 
-- SET RESOURCE_MONITOR = monitoring_pipeline_rm;

-- GRANT MONITOR ON WAREHOUSE monitoring_pipeline_wh TO ROLE sysadmin;

-- USE ROLE {{ defaultRole }};

-- GRANT USAGE ON WAREHOUSE monitoring_pipeline_wh TO ROLE monitoring_pipeline;
-- GRANT OPERATE ON WAREHOUSE monitoring_pipeline_wh TO ROLE monitoring_pipeline;

-- ------------------------------------------------------------------------------------------------------------------------------
-- Create monitoring views
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE OR REPLACE VIEW monitoring_db.transformed.account_details AS 
    WITH cte_customer AS (

    SELECT TOP 1 RIGHT(database_name, LEN(database_name) -1) AS identifier
    FROM snowflake.information_schema.databases
    WHERE STARTSWITH(database_name, '_')

    ), cte_prd_schemachange AS (

    SELECT MAX(version) AS deployment_version, MAX(installed_on) AS last_deployed_at
    FROM prd_db.schemachange.change_history

    ), cte_uat_schemachange AS (

    SELECT MAX(version) AS deployment_version, MAX(installed_on) AS last_deployed_at
    FROM uat_db.schemachange.change_history
    

    ), cte_dev_schemachange AS (

    SELECT MAX(version) AS deployment_version, MAX(installed_on) AS last_deployed_at
    FROM dev_db.schemachange.change_history
    
    )
    SELECT 
    CURRENT_ACCOUNT() AS account_name,
    CURRENT_REGION() AS region,
    identifier AS customer_identifier,
    CURRENT_VERSION() AS snowflake_version,
    -- to_object(parse_json('{"prd": {"version": deployment_version }}'))
    PARSE_JSON('{"prd":{"version":"' || prd.deployment_version || '", "lastDeployedAt":"' || prd.last_deployed_at || '"}, 
                "uat":{"version":"' || uat.deployment_version || '", "lastDeployedAt":"' || uat.last_deployed_at || '"},
                "dev":{"version":"' || dev.deployment_version || '", "lastDeployedAt":"' || dev.last_deployed_at || '"}
                }') AS deployment_details
    FROM cte_customer
    CROSS JOIN cte_prd_schemachange prd
    CROSS JOIN cte_uat_schemachange uat
    CROSS JOIN cte_dev_schemachange dev
;

CREATE or replace VIEW monitoring_db.transformed.tasks_history AS 
  SELECT
    CURRENT_ACCOUNT() AS _account_name, 
    query_id,
    name,
    database_name,
    schema_name,
    query_text,
    condition_text,
    state,
    error_code,
    error_message,
    CONVERT_TIMEZONE('UTC', scheduled_time) AS scheduled_time,
    CONVERT_TIMEZONE('UTC', query_start_time) AS query_start_time,
    CONVERT_TIMEZONE('UTC', completed_time) AS completed_time,
    root_task_id,
    graph_version,
    run_id,
    return_value,
    CONVERT_TIMEZONE('UTC', SYSDATE()) AS _exported_at
  FROM snowflake.account_usage.task_history
;

CREATE OR REPLACE VIEW monitoring_db.transformed.pipes_status AS 
  SELECT 
    CURRENT_ACCOUNT() AS _account_name,
    'ingest_time_series_pp' AS _name,
    'prd_db' AS _database_name,
    'raw' AS _schema_name,
    SYSDATE() AS _captured_at, 
    SYSTEM$PIPE_STATUS('prd_db.raw.ingest_time_series_pp') AS pipe_status
//    
  UNION ALL
  
  SELECT 
    CURRENT_ACCOUNT() AS _account_name,
    'ingest_time_series_pp' AS _name,
    'uat_db' AS _database_name,
    'raw' AS _schema_name,
    SYSDATE() AS _captured_at, 
    SYSTEM$PIPE_STATUS('uat_db.raw.ingest_time_series_pp') AS pipe_status
    
  UNION ALL
  
  SELECT 
    CURRENT_ACCOUNT() AS _account_name,
    'ingest_time_series_pp' AS _name,
    'dev_db' AS _database_name,
    'raw' AS _schema_name,
    SYSDATE() AS _captured_at, 
    SYSTEM$PIPE_STATUS('dev_db.raw.ingest_time_series_pp') AS pipe_status  
  
  UNION ALL
  
  SELECT 
    CURRENT_ACCOUNT() AS _account_name,
    'ingest_raw_from_ext_stage_pp' AS _name,
    'prd_db' AS _database_name,
    'raw' AS _schema_name,
    SYSDATE() AS _captured_at, 
    SYSTEM$PIPE_STATUS('prd_db.raw.ingest_raw_from_ext_stage_pp') AS pipe_status
    
  UNION ALL
  
  SELECT 
    CURRENT_ACCOUNT() AS _account_name,
    'ingest_raw_from_ext_stage_pp' AS _name,
    'uat_db' AS _database_name,
    'raw' AS _schema_name,
    SYSDATE() AS _captured_at, 
    SYSTEM$PIPE_STATUS('uat_db.raw.ingest_raw_from_ext_stage_pp') AS pipe_status
    
  UNION ALL
  
  SELECT 
    CURRENT_ACCOUNT() AS _account_name,
    'ingest_raw_from_ext_stage_pp' AS _name,
    'dev_db' AS _database_name,
    'raw' AS _schema_name,
    SYSDATE() AS _captured_at, 
    SYSTEM$PIPE_STATUS('dev_db.raw.ingest_raw_from_ext_stage_pp') AS pipe_status   
;


-- Pipe copy history
//CREATE VIEW monitoring_db.transformed.pipe_copy_history AS 
//SELECT 
//  pipe_catalog_name, 
//  pipe_schema_name, 
//  pipe_name, 
//  table_catalog_name, 
//  table_schema_name, 
//  table_name, 
//  CONVERT_TIMEZONE('UTC', 'Australia/Sydney', last_load_time) as last_load_time,
//  pipe_received_time, 
//  row_count, 
//  row_parsed, 
//  file_size, 
//  error_count, 
//  first_error_message, 
//  status, 
//  file_name, 
//  stage_location
//FROM TABLE(information_schema.copy_history(table_name=>'raw.time_series', start_time=> DATEADD(HOURS, -24, CURRENT_TIMESTAMP())))
//UNION ALL
//SELECT 
//  pipe_catalog_name, 
//  pipe_schema_name, 
//  pipe_name, 
//  table_catalog_name, 
//  table_schema_name, 
//  table_name, 
//  CONVERT_TIMEZONE('UTC', 'Australia/Sydney', last_load_time) as last_load_time, 
//  pipe_received_time, 
//  row_count, 
//  row_parsed, 
//  file_size, 
//  error_count, 
//  first_error_message, 
//  status, 
//  file_name, 
//  stage_location
//FROM TABLE(information_schema.copy_history(table_name=>'raw.stage_data_loader', start_time=> DATEADD(HOURS, -24, CURRENT_TIMESTAMP())));

-- We can run this every 24 hours rather than hourly
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE VIEW monitoring_db.transformed.serverless_tasks_history AS
  SELECT 
    CURRENT_ACCOUNT() AS _account_name,
    database_name,
    schema_name,
    task_name, 
    task_id,
    start_time,
    end_time,
    credits_used,  
    CONVERT_TIMEZONE('UTC', SYSDATE()) AS _exported_at
  FROM snowflake.account_usage.serverless_task_history
;  

USE ROLE {{ defaultRole }};