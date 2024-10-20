-- ------------------------------------------------------------------------------------------------------------------------------
-- Create serverless task history view
-- ------------------------------------------------------------------------------------------------------------------------------
-- This view enables serverless task cost tracking

-- Deploy only for customer accounts
-- {% if accountType == 'customer' -%}

-- USE ROLE ACCOUNTADMIN;

-- CREATE OR REPLACE VIEW monitoring_db.transformed.serverless_tasks_history AS
--   SELECT 
--     CURRENT_ACCOUNT() AS account_name,
--     database_name,
--     schema_name,
--     task_name, 
--     task_id,
--     start_time,
--     end_time,
--     credits_used,  
--     CONVERT_TIMEZONE('UTC', SYSDATE()) AS _exported_at
--   FROM snowflake.account_usage.serverless_task_history
-- ;  

-- {%- endif %}

USE ROLE {{ defaultRole }};