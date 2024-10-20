-- ------------------------------------------------------------------------------------------------------------------------------
-- Create task history view
-- ------------------------------------------------------------------------------------------------------------------------------

-- Deploy only for customer accounts
-- {% if accountType == 'customer' -%}

-- CREATE OR REPLACE VIEW monitoring_db.transformed.tasks_history AS 
--   SELECT
--     CURRENT_ACCOUNT() AS account_name, 
--     query_id,
--     name,
--     database_name,
--     schema_name,
--     query_text,
--     condition_text,
--     state,
--     error_code,
--     error_message,
--     CONVERT_TIMEZONE('UTC', scheduled_time) AS scheduled_time,
--     CONVERT_TIMEZONE('UTC', query_start_time) AS query_start_time,
--     CONVERT_TIMEZONE('UTC', completed_time) AS completed_time,
--     root_task_id,
--     graph_version,
--     run_id,
--     return_value,
--     CONVERT_TIMEZONE('UTC', SYSDATE()) AS _exported_at
--   FROM snowflake.account_usage.task_history
-- ;

{%- endif %}

USE ROLE {{ defaultRole }};