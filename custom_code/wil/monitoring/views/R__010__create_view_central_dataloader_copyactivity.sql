-- ------------------------------------------------------------------------------------------------------------------------------
-- monitoring view
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW central_monitoring_db.published.dataloader_copyactivity AS
SELECT
		unique_name,
		data_source_name,
		data_source_type,
		region,
		trigger_name,
		loader_type,
        'UAT' AS environment,
        UPPER(COALESCE(NULLIF(stage.customer,''), NULLIF(stage.customer_parsed,''), NULLIF(stage.new_cust,'')) ) AS customer,
		status,
		start_time,
		duration_seconds,
		bytes_read/1048576.00 AS mb_read,
		bytes_written/1048576.00 AS mb_written,
		rows_read,
		rows_copied,
		rows_skipped,
		files_read,
		files_written,
		files_skipped,
		throughput_kbps,
		last_watermark,
		loader_run_id,
		pipeline_run_id,
		data_source_id,
		source_entity_id,
		source_query,
		destination_path,
		full_output
FROM uat_db.transformed.dataloader_copyactivity_stage stage
WHERE customer <> 'TPS://WILDSECORE'

UNION ALL

SELECT
		unique_name,
		data_source_name,
		data_source_type,
		region,
		trigger_name,
		loader_type,
        'PRD' AS environment,
        UPPER(COALESCE(NULLIF(stage.customer,''), NULLIF(stage.customer_parsed,''), NULLIF(stage.new_cust,'')) ) AS customer,
		status,
		start_time,
		duration_seconds,
		bytes_read/1048576.00 AS mb_read,
		bytes_written/1048576.00 AS mb_written,
		rows_read,
		rows_copied,
		rows_skipped,
		files_read,
		files_written,
		files_skipped,
		throughput_kbps,
		last_watermark,
		loader_run_id,
		pipeline_run_id,
		data_source_id,
		source_entity_id,
		source_query,
		destination_path,
		full_output
FROM prd_db.transformed.dataloader_copyactivity_stage stage
;