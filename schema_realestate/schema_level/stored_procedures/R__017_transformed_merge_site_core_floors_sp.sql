-- ******************************************************************************************************************************
-- Stored procedure that merges into insights
-- This is called via transformed.merge_insights_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_insights_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_site_core_floors_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
  MERGE INTO transformed.site_core_floors AS tgt 
  USING (
    SELECT
      json_value:Id::STRING AS id,
      json_value:SiteId::STRING AS site_id,
      json_value:Name::STRING AS name,	  
      json_value:Code::STRING AS floor_code,
      json_value:SortOrder::INTEGER AS sort_order,
      json_value:Geometry::STRING AS geometry,
      json_value:IsDecommissioned::BOOLEAN AS is_decommissioned,
      json_value:ModelReference::STRING AS model_reference,
      json_value:IsSiteWide::BOOLEAN AS is_site_wide,
      TRY_PARSE_JSON(json_value::VARIANT)::variant AS raw_json_value,
      true AS is_active,
      _stage_record_id,
      json_value:_loader_run_Id::STRING AS _loader_run_id,
      json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
      _staged_at
    FROM raw.json_site_core_floors_str
    -- Make sure that the joining key is unique (take just the latest batch if there is more)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) = 1
  ) AS src
    ON (tgt.id = src.id)
  WHEN MATCHED THEN
    UPDATE 
    SET 
      tgt.site_id=src.site_id,
      tgt.name=src.name,
      tgt.floor_code=src.floor_code,
      tgt.sort_order=src.sort_order,
      tgt.geometry=src.geometry,
      tgt.is_decommissioned=src.is_decommissioned,
      tgt.model_reference=src.model_reference,
      tgt.is_site_wide=src.is_site_wide,
      tgt.raw_json_value=src.raw_json_value,
      tgt._is_active=true,
      tgt._last_updated_at= SYSDATE(),
      tgt._stage_record_id=src._stage_record_id,
      tgt._loader_run_id=src._loader_run_id,
      tgt._ingested_at=src._ingested_at,
      tgt._staged_at=src._staged_at
  WHEN NOT MATCHED THEN
    INSERT (
		id,
		site_id,
		name,
		floor_code,
		sort_order,
		geometry,
		is_decommissioned,
		model_reference,
		is_site_wide,
		raw_json_value,
		_is_active,
		_created_at,
		_last_updated_at,
		_stage_record_id,
		_loader_run_id,
		_ingested_at,
		_staged_at
	  ) 
    VALUES (
		src.id,
		src.site_id,
		src.name,
		src.floor_code,
		src.sort_order,
		src.geometry,
		src.is_decommissioned,
		src.model_reference,
		src.is_site_wide,
		src.raw_json_value,
		true,
		SYSDATE(),
		SYSDATE(),
		src._stage_record_id,
		src._loader_run_id,
		src._ingested_at,
		src._staged_at
    );
    $$
;
