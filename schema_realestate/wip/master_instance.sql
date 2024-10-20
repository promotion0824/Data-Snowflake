use database monitoring_db;
USE ROLE {{ defaultRole }};
USE WAREHOUSE dev_wh;

CREATE SCHEMA raw;
CREATE SCHEMA transformed;
CREATE SCHEMA published;

----------------------------------------------------------------------------------------------------------------------------
-- Raw tables
----------------------------------------------------------------------------------------------------------------------------

CREATE or replace TABLE raw.tasks_history (
    _account_name TEXT,
    query_id	TEXT,
    name	TEXT,
    database_name	TEXT,
    schema_name	TEXT,
    query_text	TEXT,
    condition_text	TEXT,
    state	TEXT,
    error_code	NUMBER,
    error_message	TEXT,
    scheduled_time	TIMESTAMP_TZ,
    query_start_time	TIMESTAMP_TZ,
    completed_time	TIMESTAMP_TZ,
    root_task_id	TEXT,
    graph_version	NUMBER,
    run_id	NUMBER,
    return_value	TEXT,
    _exported_at TIMESTAMP_LTZ
);

CREATE or replace TABLE raw.pipes_status (
  _account_name TEXT,
  _name TEXT,
  _database_name TEXT,
  _schema_name TEXT,
  _captured_at TIMESTAMP_LTZ,
  pipe_status	VARIANT
);

CREATE or replace TABLE raw.account_details (
  account_name TEXT,
  region TEXT,
  customer_identifier TEXT,
  snowflake_version TEXT,
  pipe_status	VARIANT,
  _captured_at TIMESTAMP_LTZ,
  _loader_run_id VARCHAR(36)
);

CREATE or replace TABLE raw.serverless_tasks_history (
  _account_name TEXT,
  task_name TEXT,
  start_time TIMESTAMP_LTZ,
  end_time TIMESTAMP_LTZ,
  total_credits_used NUMBER(38,9),
  _exported_at TIMESTAMP_LTZ,
  _loader_run_id VARCHAR(36),
  pipe_status	VARIANT
);

----------------------------------------------------------------------------------------------------------------------------
-- Published views
----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.account_details AS 
  SELECT 
    account_name,
    region,
    customer_identifier,
    snowflake_version,
    deployment_details,
    CONCAT(
      'https://', 
      LOWER(account_name), '.', 
      CASE REGION
        WHEN 'AZURE_AUSTRALIAEAST'
          THEN 'australia-east.azure'
        WHEN 'AZURE_EASTUS2'
          THEN 'east-us-2.azure'
        WHEN 'AZURE_WESTEUROPE'
          THEN 'west-europe.azure'
        ELSE 'UNKNOWN'
      END, 
      '.snowflakecomputing.com'
    ) AS account_url,
    _captured_at AS last_updated_at
  FROM raw.account_details
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_name ORDER BY _captured_at DESC) = 1 
;


CREATE OR REPLACE VIEW published.pipes_status_latest AS 
  SELECT 
    UPPER(_account_name) AS account_name,
    IFNULL(account_details.customer_identifier, 'UNKNOWN') AS customer_identifier,
    IFNULL(account_details.region, 'UNKNOWN') AS region,    
    UPPER(REPLACE(_database_name, '_db')) AS environment_name,
    UPPER(_name) AS name,
    UPPER(_schema_name) AS schema_name,
    UPPER(_database_name) AS database_name,
    pipes_status._captured_at AS last_updated_at,
    pipe_status:executionState::STRING AS execution_state,
    pipe_status:lastForwardedMessageTimestamp::TIMESTAMP AS last_forwarded_message_at,
    pipe_status:lastPulledFromChannelTimestamp::TIMESTAMP AS last_pulled_from_channel_at,
    pipe_status:lastReceivedMessageTimestamp::TIMESTAMP AS last_received_message_at,
    pipe_status:oldestFileTimestamp::TIMESTAMP AS oldest_file_processed_at,
    pipe_status:lastPipeErrorTimestamp::TIMESTAMP AS last_pipe_error_at,
    pipe_status:lastPipeFaultTimestamp::TIMESTAMP AS last_pipe_fault_timestamp,
    pipe_status:lastIngestedTimestamp::TIMESTAMP AS last_ingested_at,
    pipe_status:numOutstandingMessagesOnChannel::INT AS num_outstanding_messages_on_channel,
    pipe_status:pendingFileCount::INT AS pending_file_count,
    pipe_status:oldestPendingFilePath::STRING AS oldest_pending_file_path,
      pipe_status:channelErrorMessage::STRING AS channel_error_message,
    pipe_status:lastForwardedFilePath::STRING AS last_forwarded_file_path,
    pipe_status AS raw_pipe_status,
    account_url
  FROM raw.pipes_status pipes_status
    LEFT JOIN published.account_details account_details ON (UPPER(pipes_status._account_name) = account_details.account_name)
  -- TODO: Remove this condition once we get rid of the old databases
  WHERE environment_name IN ('DEV','UAT', 'PRD')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _account_name, _name, _schema_name, _database_name ORDER BY pipes_status._captured_at DESC) = 1
;

CREATE OR REPLACE VIEW published.pipes_status_history AS 
  SELECT 
    UPPER(_account_name) AS account_name,
    IFNULL(account_details.customer_identifier, 'UNKNOWN') AS customer_identifier,
    IFNULL(account_details.region, 'UNKNOWN') AS region, 
    UPPER(REPLACE(_database_name, '_db')) AS environment_name,
    UPPER(_name) AS name,
    UPPER(_schema_name) AS schema_name,
    UPPER(_database_name) AS database_name,
    pipes_status._captured_at AS captured_at,
    pipe_status:executionState::STRING AS execution_state,
    pipe_status:lastForwardedMessageTimestamp::TIMESTAMP AS last_forwarded_message_at,
    pipe_status:lastPulledFromChannelTimestamp::TIMESTAMP AS last_pulled_from_channel_at,
    pipe_status:lastReceivedMessageTimestamp::TIMESTAMP AS last_received_message_at,
    pipe_status:oldestFileTimestamp::TIMESTAMP AS oldest_file_processed_at,
    pipe_status:lastPipeErrorTimestamp::TIMESTAMP AS last_pipe_error_at,
    pipe_status:lastPipeFaultTimestamp::TIMESTAMP AS last_pipe_fault_timestamp,
    pipe_status:lastIngestedTimestamp::TIMESTAMP AS last_ingested_at,
    pipe_status:numOutstandingMessagesOnChannel::INT AS num_outstanding_messages_on_channel,
    pipe_status:pendingFileCount::INT AS pending_file_count,
    pipe_status:oldestPendingFilePath::STRING AS oldest_pending_file_path,
    pipe_status:channelErrorMessage::STRING AS channel_error_message,
    pipe_status:lastForwardedFilePath::STRING AS last_forwarded_file_path,
    pipe_status AS raw_pipe_status,
    account_url
  FROM raw.pipes_status pipes_status
    LEFT JOIN published.account_details account_details ON (UPPER(pipes_status._account_name) = account_details.account_name)
  -- TODO: Remove this condition once we get rid of the old databases
  WHERE environment_name IN ('DEV','UAT', 'PRD')
;

CREATE OR REPLACE VIEW published.tasks_history AS 
  SELECT 
    UPPER(_account_name) AS account_name,
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
    LEFT JOIN published.account_details account_details ON (UPPER(tasks_history._account_name) = account_details.account_name)
  -- TODO: Remove this condition once we get rid of the old databases
  WHERE environment_name IN ('DEV','UAT', 'PRD')
;

CREATE OR REPLACE VIEW published.serverless_tasks_history AS 
  SELECT 
    UPPER(_account_name) AS account_name,
    IFNULL(account_details.customer_identifier, 'UNKNOWN') AS customer_identifier,
    IFNULL(account_details.region, 'UNKNOWN') AS region,
    task_name,
    start_time,
    end_time,
    total_credits_used,
    tasks_history._exported_at AS exported_at
  FROM raw.serverless_tasks_history tasks_history
    LEFT JOIN published.account_details account_details ON (UPPER(tasks_history._account_name) = account_details.account_name)
;

----------------------------------------------------------------------------------------------------------------------------
-- App Insights export views
----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.tasks_history_test AS 
SELECT 
    PARSE_JSON(
        '{"ServiceName": "Snowflake", "ServiceInstance": "' || 
        _account_name || 
        '", "ActivityName":"' || 
        CONCAT(database_name, '.', schema_name, '.', name) || 
        '", "Properties": ' || 
        TO_JSON(OBJECT_CONSTRUCT(*)) || 
        '}'
    ) AS json_payload
FROM published.tasks_history;