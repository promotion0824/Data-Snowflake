-- ******************************************************************************************************************************
-- Stored procedure that merges into inspections
-- This is called via transformed.merge_inspections_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_inspections_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_inspections_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
  AS
    $$
      MERGE INTO transformed.inspections AS tgt 
      USING (
        SELECT
          json_value:Id::STRING AS id,
          json_value:SiteId::STRING AS site_id,
          json_value:Name::STRING AS name,
          json_value:FloorCode::STRING AS floor_code,
          json_value:ZoneId::STRING AS zone_id,
          json_value:AssetId::STRING AS asset_id,
          json_value:AssignedWorkgroupId::STRING AS assigned_workgroup_id,
          json_value:FrequencyInHours::DECIMAL(12,0) AS frequency_in_hours,
          json_value:StartDate::TIMESTAMP_NTZ AS start_date,
          json_value:EndDate::TIMESTAMP_NTZ AS end_date,
          json_value:LastRecordId::STRING AS last_record_id,
          json_value:IsArchived::BOOLEAN AS is_archived,
          json_value:SortOrder::DECIMAL(12,0) AS sort_order,
          json_value:Frequency::DECIMAL(12,0) AS frequency,
          json_value:FrequencyUnit::STRING AS frequency_unit,
          json_value:TwinId::STRING AS twin_id,
          json_value raw_json_value,
          _stage_record_id,
          json_value:_loader_run_Id::STRING AS _loader_run_id,
          json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
          _staged_at
        FROM raw.json_inspections_str
        -- Make sure that the joining key is unique (take just the latest batch if there is more)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC)  =  1
      ) AS src
        ON (tgt.id  =  src.id)
      WHEN MATCHED THEN
        UPDATE 
        SET 
            tgt.site_id = src.site_id,
            tgt.name = src.name,
            tgt.floor_code = src.floor_code,
            tgt.zone_id = src.zone_id,
            tgt.asset_id = src.asset_id,
            tgt.assigned_workgroup_id = src.assigned_workgroup_id,
            tgt.frequency_in_hours = src.frequency_in_hours,
            tgt.start_date = src.start_date,
            tgt.end_date = src.end_date,
            tgt.last_record_id = src.last_record_id,
            tgt.is_archived = src.is_archived,
            tgt.sort_order = src.sort_order,
            tgt.frequency = src.frequency,
            tgt.frequency_unit = src.frequency_unit,
            tgt.twin_id = src.twin_id,
            tgt.raw_json_value = src.raw_json_value,
            tgt._created_at = SYSDATE(),
            tgt._last_updated_at = SYSDATE(),
            tgt._stage_record_id = src._stage_record_id,
            tgt._loader_run_id = src._loader_run_id,
            tgt._ingested_at = src._ingested_at,
            tgt._staged_at = src._staged_at
      WHEN NOT MATCHED THEN
        INSERT (
            id,
            site_id,
            name,
            floor_code,
            zone_id,
            asset_id,
            assigned_workgroup_id,
            frequency_in_hours,
            start_date,
            end_date,
            last_record_id,
            is_archived,
            sort_order,
            frequency,
            frequency_unit,
            twin_id,
            raw_json_value,
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
            src.zone_id,
            src.asset_id,
            src.assigned_workgroup_id,
            src.frequency_in_hours,
            src.start_date,
            src.end_date,
            src.last_record_id,
            src.is_archived,
            src.sort_order,
            src.frequency,
            src.frequency_unit,
            src.twin_id,
            src.raw_json_value,
            SYSDATE(),
            SYSDATE(),
            src._stage_record_id,
            src._loader_run_id,
            src._ingested_at,
            src._staged_at
        );
    $$
;
