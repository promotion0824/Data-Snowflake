-- ******************************************************************************************************************************
-- Create Tasks History view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW central_monitoring_db.published.tasks_history AS 
  SELECT 
    UPPER(tasks_history.account_name) AS account_name,
    IFNULL(account_details.customer_identifier, 'UNKNOWN') AS customer_identifier,
    IFNULL(account_details.region, 'UNKNOWN') AS region,
    REPLACE(database_name, '_DB') AS environment_name,
    query_id,
    name,
    database_name,
    schema_name,
    query_text,
    condition_text,
    state,
    error_code,
    error_message,
    scheduled_time,
    query_start_time,
    completed_time,
    root_task_id,
    graph_version,
    run_id,
    return_value,
    account_url,
    tasks_history._exported_at AS exported_at
  FROM raw.tasks_history tasks_history
    LEFT JOIN published.account_details account_details ON (UPPER(tasks_history.account_name) = account_details.account_name)
  -- TODO: Remove this condition once we get rid of the old databases
  WHERE environment_name IN ('DEV','UAT', 'PRD')
    AND tasks_history.name NOT IN ('LOAD_TELEMETRY_FILES_TK','CREATE_TABLE_TRANSFORMED_PERSONS_TK', 'TRIGGER_SITE_DAILY_ENERGY_THRESHOLDS_CALCULATION_TK')
    AND tasks_history.name NOT LIKE '%_AIR_TEMPERATURE_%'
;