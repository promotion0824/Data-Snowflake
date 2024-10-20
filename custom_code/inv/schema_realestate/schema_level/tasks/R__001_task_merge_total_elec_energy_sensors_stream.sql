
-- ------------------------------------------------------------------------------------------------------------------------------
-- Process Total Electrical Energy Sensors Stream Task
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK raw.merge_total_elec_energy_sensors_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '20 minute'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('json_total_elec_energy_sensors_str')
AS
  MERGE INTO transformed.total_elec_energy_sensors AS tgt 
  USING (
    SELECT
      json_value:uniqueID::STRING AS unique_id,
      json_value:id::STRING AS id,
      json_value:trendID::STRING AS trend_id,
      json_value:siteID::STRING AS site_id,
      json_value:externalID::STRING AS external_id,
      json_value:name::STRING AS name,
      json_value:description::STRING AS description,      
      json_value:type::STRING AS type,
      json_value:unit::STRING AS unit,
      json_value:trendInterval::INT AS trend_interval,
      json_value:enabled::BOOLEAN AS is_enabled,
      json_value:tags::VARIANT AS tags,
      json_value::VARIANT AS raw_json_value,
      true AS is_active,
      _stage_record_id,
      json_value:_loader_run_id::STRING AS _loader_run_id,
      json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
      _staged_at
    FROM raw.json_total_elec_energy_sensors_str
  ) AS src
    ON (tgt.unique_id = src.unique_id)
  WHEN MATCHED THEN
    UPDATE 
    SET 
      tgt.id = src.id,
      tgt.trend_id = src.trend_id,
      tgt.site_id = src.site_id,
      tgt.external_id = src.external_id,
      tgt.name = src.name,
      tgt.description = src.description,
      tgt.type = src.type,
      tgt.unit = src.unit,
      tgt.trend_interval = src.trend_interval,
      tgt.is_enabled = src.is_enabled,
      tgt.tags = src.tags,
      tgt.raw_json_value = src.raw_json_value,
      tgt._last_updated_at = SYSDATE(),
      tgt._is_active = true,
      tgt._stage_record_id = src._stage_record_id,
      tgt._loader_run_id = src._loader_run_id,
      tgt._ingested_at = src._ingested_at,
      tgt._staged_at = src._staged_at
  WHEN NOT MATCHED THEN
    INSERT (
      unique_id, 
      id,
      trend_id,
      site_id, 
      external_id, 
      name,
      description, 
      type, 
      unit,
      trend_interval,
      tags,
      raw_json_value, 
      _is_active,
      _created_at,
      _last_updated_at,
      _stage_record_id, 
      _loader_run_id, 
      _ingested_at, 
      _staged_at) 
    VALUES (
      src.unique_id, 
      src.id,
      src.trend_id,
      src.site_id, 
      src.external_id, 
      src.name,
      src.description, 
      src.type, 
      src.unit,
      src.trend_interval,
      src.tags, 
      src.raw_json_value, 
      true,
      SYSDATE(), 
      SYSDATE(),
      src._stage_record_id, 
      src._loader_run_id, 
      src._ingested_at, 
      src._staged_at 
    );        
    
ALTER TASK raw.merge_total_elec_energy_sensors_stream_tk SUSPEND;