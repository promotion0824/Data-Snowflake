-- ******************************************************************************************************************************
-- Switch database
-- ******************************************************************************************************************************
USE {{ environment }}_db;


-- Ingest Telemetry

DROP PIPE IF EXISTS raw.ingest_telemetry_pp;

CREATE OR REPLACE PIPE raw.ingest_telemetry_pp
	AUTO_INGEST = TRUE
	INTEGRATION = 'EXT_TELEMETRY_STAGE_{{ uppercaseEnvironment }}_NIN'
  ERROR_INTEGRATION = 'ERROR_{{ uppercaseEnvironment }}_NIN'
  AS
  COPY INTO raw.stage_telemetry(
      connector_id, dt_id, external_id, trend_id, captured_at, enqueued_at, scalar_value, latitude, longitude, altitude, properties, exported_time, _ingested_at, stage_file_name
  )
     FROM (
     	SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, SYSDATE(), METADATA$FILENAME
        FROM @raw.data_loader_telemetry_esg
     ) FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = 'GZIP' RECORD_DELIMITER='\n' ESCAPE_UNENCLOSED_FIELD = None ESCAPE = '\\' NULL_IF='' SKIP_HEADER=1) ON_ERROR = CONTINUE
;

ALTER PIPE IF EXISTS raw.ingest_telemetry_pp 
  SET PIPE_EXECUTION_PAUSED = false;

-- Allow pipe monitoring
GRANT MONITOR ON PIPE raw.ingest_telemetry_pp TO ROLE monitoring_pipeline_reader;

-- ******************************************************************************************************************************
-- Ingest pipe for all other data
-- ******************************************************************************************************************************
DROP PIPE IF EXISTS raw.ingest_raw_from_ext_stage_pp;

CREATE PIPE raw.ingest_raw_from_ext_stage_pp
	AUTO_INGEST = TRUE
	INTEGRATION = 'EXT_STAGE_{{ uppercaseEnvironment }}_NIN'
  ERROR_INTEGRATION = 'ERROR_{{ uppercaseEnvironment }}_NIN'
  AS
  COPY INTO raw.stage_data_loader(_stage_record_id, json_value, _schema_type, _stage_file_name, _loader_run_id, _ingested_at, _staged_at)
     FROM (
      SELECT 
          UUID_STRING() AS _stage_record_id, 
          $1 AS json_value,
          $1:_schema_type::STRING AS _schema_type,
          metadata$filename AS _stage_file_name,
          $1:_loader_run_id::STRING AS _loader_run_id,
          $1:_ingested_at::TIMESTAMP_NTZ AS _ingested_at, 
          SYSDATE() AS _staged_at
      FROM @raw.data_loader_esg
     )
;  

ALTER PIPE IF EXISTS raw.ingest_raw_from_ext_stage_pp 
  SET PIPE_EXECUTION_PAUSED = false;

-- Allow pipe monitoring
GRANT MONITOR ON PIPE raw.ingest_raw_from_ext_stage_pp TO ROLE monitoring_pipeline_reader;
  	