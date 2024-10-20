-- ******************************************************************************************************************************
-- Create Pipes Status History view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW central_monitoring_db.published.pipes_status_history AS 
  SELECT 
    UPPER(pipes_status.account_name) AS account_name,
    IFNULL(account_details.customer_identifier, 'UNKNOWN') AS customer_identifier,
    IFNULL(account_details.region, 'UNKNOWN') AS region,    
    UPPER(REPLACE(database_name, '_db')) AS environment_name,
    UPPER(name) AS name,
    UPPER(schema_name) AS schema_name,
    UPPER(database_name) AS database_name,
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
  FROM raw.pipes_status_history pipes_status
    LEFT JOIN published.account_details account_details ON (UPPER(pipes_status.account_name) = account_details.account_name)
  -- TODO: Remove this condition once we get rid of the old databases
  WHERE environment_name IN ('DEV','UAT', 'PRD')
;