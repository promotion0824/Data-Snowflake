-- ******************************************************************************************************************************
-- Stored procedure that merges from raw to transformed
-- USAGE:  CALL transformed.merge_connectors_sp();
-- ******************************************************************************************************************************
	
CREATE OR REPLACE PROCEDURE transformed.merge_connectors_sp()
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
		   MERGE INTO transformed.connectors AS tgt 
		   USING (
			SELECT DISTINCT
				json_value:Id::string as id,
				json_value:Name::string as name,
				json_value:ClientId::string as client_id,
				json_value:connectorType::string as connector_type,
				json_value:SiteId::string as site_id,
				json_value:IsEnabled::string as is_enabled,
				json_value:IsArchived::string as is_archived,
				json_value:LastUpdatedAt::string as source_last_updated_at,
				json_value:_loader_run_id::STRING AS _loader_run_id,
				json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at
			 FROM raw.json_connectors_str c
			QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) = 1
		  ) AS src
			ON (tgt.id = src.id)
		  WHEN MATCHED AND src.source_last_updated_at != tgt.source_last_updated_at THEN
			UPDATE 
			SET 
			  tgt.name = src.name,
			  tgt.connector_type = src.connector_type,
			  tgt.site_id = src.site_id,
			  tgt.is_enabled = src.is_enabled,
			  tgt.is_archived = src.is_archived,
			  tgt.source_last_updated_at = src.source_last_updated_at,
      		  tgt._loader_run_id = src._loader_run_id,
			  tgt._last_updated_at = SYSDATE()
		  WHEN NOT MATCHED THEN
			INSERT (
				id,
				client_id,
				name,
				connector_type,
				site_id,
				is_enabled,
				is_archived,
				source_last_updated_at,
				_loader_run_id,
				_last_updated_at,
				_ingested_at
				)		
			VALUES (
				src.id,
				src.client_id,
				src.name, 
				src.connector_type,
				src.site_id,
				src.is_enabled,
				src.is_archived,
				src.source_last_updated_at,
				src._loader_run_id,
				SYSDATE(),
				src._ingested_at
			);
    $$
;