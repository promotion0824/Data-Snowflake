-- ******************************************************************************************************************************
-- Switch database
-- ******************************************************************************************************************************
USE {{ environment }}_db;


-- Ingest Telemetry

DROP PIPE IF EXISTS raw.ingest_mapped_raw_data;

CREATE OR REPLACE PIPE raw.ingest_mapped_raw_data
	AUTO_INGEST = TRUE
	INTEGRATION = 'EXT_STAGE_{{ uppercaseEnvironment }}_NIN'
  ERROR_INTEGRATION = 'ERROR_{{ uppercaseEnvironment }}_NIN'
  AS
  COPY INTO raw.stage_mapped_raw_data(_stage_record_id, json_value, _schema_type, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
     FROM (
     	SELECT 
          UUID_STRING() AS _stage_record_id, 
          $1 AS json_value,
          $1:_schema_type::STRING AS _schema_type,
          metadata$filename AS _stage_file_name,
          $1:_loader_run_id::STRING AS _loader_run_id,
          $1:_ingested_at::TIMESTAMP_NTZ AS _ingested_at, 
          SYSDATE() AS _staged_at
      FROM @raw.data_loader_eh_capture_container/evhns-prd-eus2-15-wmr-in1-60537353/evh-mapped-rawdata/
     ) FILE_FORMAT = (TYPE = 'AVRO' COMPRESSION = 'AUTO' NULL_IF='') ON_ERROR = CONTINUE
;

ALTER PIPE IF EXISTS raw.ingest_mapped_raw_data 
  SET PIPE_EXECUTION_PAUSED = false;

-- Allow pipe monitoring
GRANT MONITOR ON PIPE raw.ingest_mapped_raw_data TO ROLE monitoring_pipeline_reader;