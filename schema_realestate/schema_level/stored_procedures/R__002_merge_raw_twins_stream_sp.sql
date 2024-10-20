-- ******************************************************************************************************************************
-- Stored procedure that merges from raw to transformed
-- USAGE:  CALL transformed.merge_telemetry_sp();
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE raw.merge_twins_stream_sp()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$
      BEGIN
        MERGE INTO transformed.twins AS tgt 
        USING (
            SELECT
            json_value:Id::STRING AS twin_id,
            json_value:Name::STRING AS name,
            json_value:ModelId::STRING AS model_id,
            json_value:TrendId::STRING AS trend_id,
            json_value:UniqueId::STRING AS unique_id,
            json_value:ExternalId::STRING AS external_id,
            json_value:FloorId::STRING AS floor_id,
            json_value:Location.FloorDtId::STRING AS floor_dtid,
            json_value:SiteId::STRING AS site_id,
            json_value:Location.SiteDtId::STRING AS site_dtid,
            json_value:ConnectorId::STRING AS connector_id,
            json_value:GeometryViewerId::STRING AS geometry_viewer_id,
            json_value:Tags::STRING AS tags,
            json_value:Deleted::BOOLEAN AS is_deleted,
            json_value:ExportTime::TIMESTAMP_NTZ AS export_time,
            TRY_PARSE_JSON(json_value:Raw::VARIANT)::variant AS raw_json_value,
            true AS is_active,
            _stage_record_id,
            json_value:_loader_run_id::STRING AS _loader_run_id,
            json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
            _staged_at
          FROM raw.json_twins_str
          -- Make sure that the joining key is unique (take just the latest batch if there is more)
          QUALIFY ROW_NUMBER() OVER (PARTITION BY twin_id ORDER BY _ingested_at DESC) = 1
        ) AS src
          ON (tgt.twin_id = src.twin_id)
        WHEN MATCHED THEN
          UPDATE 
          SET 
            tgt.twin_id = src.twin_id,
            tgt.name = src.name,
            tgt.model_id = src.model_id,
            tgt.trend_id = src.trend_id,
            tgt.unique_id = src.unique_id,
            tgt.external_id = src.external_id,
            tgt.floor_id = src.floor_id,
            tgt.floor_dtid = src.floor_dtid,
            tgt.site_id = src.site_id,
            tgt.site_dtid = src.site_dtid,
            tgt.connector_id = src.connector_id,
            tgt.geometry_viewer_id = src.geometry_viewer_id,
            tgt.tags = src.tags,	  
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
            twin_id,
            name,
            model_id,
            trend_id,
            unique_id,
            external_id,
            floor_id,
            floor_dtid,
            site_id,
            site_dtid,
            connector_id,
            geometry_viewer_id,
            tags,
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
            src.twin_id,
            src.name,
            src.model_id,
            src.trend_id,
            src.unique_id,
            src.external_id,
            src.floor_id,
            src.floor_dtid,
            src.site_id,
            src.site_dtid,
            src.connector_id,
            src.geometry_viewer_id,
            src.tags,
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