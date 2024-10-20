-- ******************************************************************************************************************************
-- Stored procedure that merges from raw to transformed
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_dataloader_copyactivity_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
MERGE INTO transformed.dataloader_copyactivity AS tgt 
  USING (
    SELECT
		json_value:UniqueName::STRING AS unique_name,
		json_value:DataSourceName::STRING AS data_source_name,
		json_value:DataSourceType::STRING AS data_source_type,
		json_value:Region::STRING AS region,
		json_value:TriggerName::STRING AS trigger_name,
		json_value:LoaderType::STRING AS loader_type,
		json_value:Status::STRING AS status,
		json_value:StartTime::TIMESTAMP_NTZ AS start_time,
		json_value:DurationSeconds::INTEGER AS duration_seconds,
		json_value:BytesRead::INTEGER AS bytes_read,
		json_value:BytesWritten::INTEGER AS bytes_written,
		json_value:RowsRead::INTEGER AS rows_read,
		json_value:RowsCopied::INTEGER AS rows_copied,
		json_value:RowsSkipped::INTEGER AS rows_skipped,
		json_value:FilesRead::INTEGER AS files_read,
		json_value:FilesWritten::INTEGER AS files_written,
		json_value:FilesSkipped::INTEGER AS files_skipped,
		json_value:ThroughputKBps::FLOAT AS throughput_kbps,
		TRY_TO_DATE(json_value:LastWatermark::STRING) AS last_watermark,
		json_value:SourceQuery::STRING AS source_query,
		json_value:DestinationPath::STRING AS destination_path,
		json_value:FullOutput::STRING AS full_output,
		json_value:DataSourceId::STRING AS data_source_id,
		json_value:SourceEntityId::STRING AS source_entity_id,	  
		json_value:LoaderRunId::STRING AS loader_run_id,
		json_value:PipelineRunId::STRING AS pipeline_run_id,
		_stage_record_id,
		json_value:_loader_run_Id::STRING AS _loader_run_id,
		json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
		_staged_at
    FROM raw.json_dataloader_copyactivity_str
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loader_run_id, pipeline_run_id ORDER BY _ingested_at DESC) = 1
  ) AS src
    ON (tgt.loader_run_id = src.loader_run_id
   AND  tgt.pipeline_run_id = src.pipeline_run_id)
  WHEN MATCHED THEN
    UPDATE 
    SET 
		tgt.unique_name = src.unique_name,
		tgt.data_source_name = src.data_source_name,
		tgt.data_source_type = src.data_source_type,
		tgt.region = src.region,
		tgt.trigger_name = src.trigger_name,
		tgt.loader_type = src.loader_type,
		tgt.status = src.status,
		tgt.start_time = src.start_time,
		tgt.duration_seconds = src.duration_seconds,
		tgt.bytes_read = src.bytes_read,
		tgt.bytes_written = src.bytes_written,
		tgt.rows_read = src.rows_read,
		tgt.rows_copied = src.rows_copied,
		tgt.rows_skipped = src.rows_skipped,
		tgt.files_read = src.files_read,
		tgt.files_written = src.files_written,
		tgt.files_skipped = src.files_skipped,
		tgt.throughput_kbps = src.throughput_kbps,
		tgt.last_watermark = src.last_watermark,
		tgt.source_query = src.source_query,
		tgt.destination_path = src.destination_path,
		tgt.full_output = src.full_output,
		tgt.data_source_id = src.data_source_id,
		tgt.source_entity_id = src.source_entity_id,
		tgt.loader_run_id = src.loader_run_id,
		tgt.pipeline_run_id = src.pipeline_run_id,
		tgt._last_updated_at = SYSDATE(),
		tgt._stage_record_id  = src._stage_record_id,
		tgt._loader_run_id  = src._loader_run_id,
		tgt._ingested_at  = src._ingested_at,
		tgt._staged_at  = src._staged_at
  WHEN NOT MATCHED THEN
    INSERT (
		unique_name,
		data_source_name,
		data_source_type,
		region,
		trigger_name,
		loader_type,
		status,
		start_time,
		duration_seconds,
		bytes_read,
		bytes_written,
		rows_read,
		rows_copied,
		rows_skipped,
		files_read,
		files_written,
		files_skipped,
		throughput_kbps,
		last_watermark,
		source_query,
		destination_path,
		full_output,
		data_source_id,
		source_entity_id,
		loader_run_id,
		pipeline_run_id,
		_created_at,
		_last_updated_at,
		_stage_record_id,
		_loader_run_id,
		_ingested_at,
		_staged_at
	  ) 
    VALUES (
		src.unique_name,
		src.data_source_name,
		src.data_source_type,
		src.region,
		src.trigger_name,
		src.loader_type,
		src.status,
		src.start_time,
		src.duration_seconds,
		src.bytes_read,
		src.bytes_written,
		src.rows_read,
		src.rows_copied,
		src.rows_skipped,
		src.files_read,
		src.files_written,
		src.files_skipped,
		src.throughput_kbps,
		src.last_watermark,
		src.source_query,
		src.destination_path,
		src.full_output,
		src.data_source_id,
		src.source_entity_id,
		src.loader_run_id,
		src.pipeline_run_id,
		SYSDATE(),
		SYSDATE(),
		src._stage_record_id,
		src._loader_run_id,
		src._ingested_at,
		src._staged_at
    );
    $$
;
