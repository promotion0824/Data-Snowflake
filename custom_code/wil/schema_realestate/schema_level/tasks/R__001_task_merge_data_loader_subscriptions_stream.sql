
-- ------------------------------------------------------------------------------------------------------------------------------
-- Process Stream Task
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK raw.merge_dataloader_subscriptions_stream_tk
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '480 minute'
  USER_TASK_TIMEOUT_MS = 1200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 5
  ERROR_INTEGRATION = error_{{ environment }}_nin
WHEN
  SYSTEM$STREAM_HAS_DATA('raw.json_monitoring_dataloader_subscriptions_str')
AS
   MERGE INTO central_monitoring_db.transformed.dataloader_subscriptions AS tgt 
  USING (
    SELECT
      json_value:source_unique_name::STRING AS source_unique_name,
      json_value:source_type::STRING AS source_type,
      json_value:source_is_active::STRING AS source_is_active,
      json_value:region::STRING AS region,
      json_value:entity_uniquename::STRING AS entity_uniquename,
      json_value:entity_is_active::STRING AS entity_is_active,
      json_value:trigger_name::STRING AS trigger_name,      
      json_value:dataset_settings::VARIANT AS dataset_settings,
      json_value:sink_settings::VARIANT AS sink_settings,
      json_value:_loader_run_id::STRING AS _loader_run_id,
      json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at
    FROM raw.json_monitoring_dataloader_subscriptions_str
	QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_uniquename,sink_settings ORDER BY _ingested_at DESC) = 1
  ) AS src
      ON (tgt.entity_uniquename = src.entity_uniquename)
     AND (tgt.sink_settings = src.sink_settings)
  WHEN MATCHED THEN
    UPDATE 
    SET 
      tgt.source_type = src.source_type,
      tgt.source_is_active = src.source_is_active,
      tgt.region = src.region,
      tgt.entity_uniquename = src.entity_uniquename,
      tgt.entity_is_active = src.entity_is_active,
      tgt.trigger_name = src.trigger_name,
      tgt.dataset_settings = src.dataset_settings,
      tgt.sink_settings = src.sink_settings,
      tgt._loader_run_id = src._loader_run_id,
      tgt._ingested_at = src._ingested_at
  WHEN NOT MATCHED THEN
    INSERT (
      source_unique_name, 
      source_type,
      source_is_active,
      region, 
      entity_uniquename, 
      entity_is_active,
      trigger_name, 
      dataset_settings, 
      sink_settings,
      _loader_run_id, 
      _ingested_at
      ) 
    VALUES (
      src.source_unique_name, 
      src.source_type,
      src.source_is_active,
      src.region, 
      src.entity_uniquename, 
      src.entity_is_active,
      src.trigger_name, 
      src.dataset_settings, 
      src.sink_settings, 
      src._loader_run_id, 
      src._ingested_at
    );
    
ALTER TASK raw.merge_dataloader_subscriptions_stream_tk RESUME;