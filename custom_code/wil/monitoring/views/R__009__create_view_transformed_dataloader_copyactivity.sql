-- ------------------------------------------------------------------------------------------------------------------------------
-- monitoring view
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW transformed.dataloader_copyactivity_stage AS
SELECT
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
		position('stg',destination_path) + 3 AS startpos,
		position('dls',destination_path) AS endpos,
		substr(destination_path,startpos,endpos-startpos) AS custenv,
		UPPER(right(custenv,3)) AS env,
		UPPER(substring(custenv,0,len(custenv)-3)) AS customer,
		POSITION('_',unique_name) AS first_underscore,
		CASE WHEN unique_name LIKE '%hourly_temperature%' THEN Left(unique_name,first_underscore-1)  
			 WHEN unique_name LIKE 'engagement%' THEN 'inv'
			 WHEN unique_name LIKE '%weatherdata%' THEN Left(unique_name,first_underscore-1)
			 WHEN unique_name LIKE '%ontology%' THEN Left(unique_name,first_underscore-1)
		ELSE NULL
		END AS customer_parsed,
		full_output,
		data_source_id,
		source_entity_id,
		loader_run_id,
		pipeline_run_id,
        CASE WHEN customer = '' THEN POSITION('-lda-',destination_path)+5 ELSE NULL END AS lda_start,
        CASE WHEN customer = '' THEN POSITION('-',destination_path,lda_start) ELSE NULL END AS lda_end,
        CASE WHEN customer = '' THEN SUBSTR(destination_path,lda_start,lda_end-lda_start) ELSE NULL END AS new_cust
FROM transformed.dataloader_copyactivity;