-- ******************************************************************************************************************************
-- Stored procedure that merges from raw to transformed
-- USAGE:  CALL transformed.merge_telemetry_sp();
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE raw.merge_twins_relationships_stream_sp()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$
      BEGIN
        MERGE INTO transformed.twins_relationships AS tgt 
        USING (
            SELECT
            json_value:SourceId::STRING AS source_twin_id,
            json_value:Name::STRING AS relationship_name,
            json_value:TargetId::STRING AS target_twin_id,
            json_value:Id::STRING AS relationship_id,
            json_value:Deleted::BOOLEAN AS is_deleted,
            json_value:ExportTime::TIMESTAMP_NTZ AS export_time,
            TRY_PARSE_JSON(json_value:Raw::VARIANT)::VARIANT AS raw_json_value,
            true AS is_active,
            _stage_record_id,
            json_value:_loader_run_id::STRING AS _loader_run_id,
            json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
            _staged_at
          FROM raw.json_twins_relationships_str
          -- Make sure that the joining key is unique (take just the latest batch if there is more)
          QUALIFY ROW_NUMBER() OVER (PARTITION BY relationship_id ORDER BY _ingested_at DESC) = 1
        ) AS src
          ON (tgt.relationship_id = src.relationship_id)
        WHEN MATCHED THEN
          UPDATE 
          SET 
            tgt.source_twin_id = src.source_twin_id,
            tgt.relationship_name = src.relationship_name,
            tgt.target_twin_id = src.target_twin_id,
            tgt.relationship_id = src.relationship_id,
            tgt.is_deleted = src.is_deleted,
            tgt.export_time = src.export_time,
            tgt.raw_json_value = src.raw_json_value,
            tgt._last_updated_at = SYSDATE(),
            tgt._is_active = true,
            tgt._stage_record_id = src._stage_record_id,
            tgt._loader_run_id = src._loader_run_id,
            tgt._ingested_at = src._ingested_at,
            tgt._staged_at = src._staged_at
        WHEN NOT MATCHED THEN
          INSERT (
            source_twin_id, 
            relationship_name,
            target_twin_id, 
            relationship_id,
            is_deleted, 
            export_time, 
            raw_json_value, 
            _is_active,
            _created_at,
            _last_updated_at,
            _stage_record_id, 
            _loader_run_id, 
            _ingested_at, 
            _staged_at) 
          VALUES (
            src.source_twin_id, 
            src.relationship_name,
            src.target_twin_id, 
            src.relationship_id,
            src.is_deleted, 
            src.export_time, 
            src.raw_json_value, 
            true,
            SYSDATE(), 
            SYSDATE(),
            src._stage_record_id, 
            src._loader_run_id, 
            src._ingested_at, 
            src._staged_at 
          );    
		  EXCEPTION
          WHEN statement_error THEN
            RETURN OBJECT_CONSTRUCT('Error type', 'STATEMENT_ERROR',
                                    'SQLCODE', sqlcode,
                                    'SQLERRM', sqlerrm,
                                    'SQLSTATE', sqlstate);
      END;		
    $$