-- ******************************************************************************************************************************
-- Create Serverless Tasks History view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW central_monitoring_db.published.serverless_tasks_history AS 
  SELECT 
    UPPER(tasks_history.account_name) AS account_name,
    IFNULL(account_details.customer_identifier, 'UNKNOWN') AS customer_identifier,
    IFNULL(account_details.region, 'UNKNOWN') AS region,
    REPLACE(database_name, '_DB') AS environment_name,
    task_name,
    database_name,
    schema_name,
    start_time,
    end_time,
    credits_used,
    tasks_history._exported_at AS exported_at
  FROM raw.serverless_tasks_history tasks_history
    LEFT JOIN published.account_details account_details ON (UPPER(tasks_history.account_name) = account_details.account_name)
  -- TODO: Remove this condition once we get rid of the old databases
  WHERE environment_name IN ('DEV','UAT', 'PRD')
;
