-- ******************************************************************************************************************************
-- Stored procedure that merges into directory_core_sites
-- This is called via transformed.merge_directory_core_sites_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_directory_core_sites_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_directory_core_sites_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
   MERGE INTO transformed.directory_core_sites AS tgt 
  USING (
    SELECT
      json_value:Id::STRING AS site_id,
      json_value:PortfolioId::STRING AS portfolio_id,
      json_value:CustomerId::STRING AS customer_id,
      json_value:Name::STRING AS name,
      utils.convert_time_zone_name_from_windows_to_tzdata_udf(json_value:TimezoneId::STRING) AS time_zone,
      json_value::VARIANT AS raw_json_value,
      true AS is_active,
      _stage_record_id,
      json_value:_loader_run_id::STRING AS _loader_run_id,
      json_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
      _staged_at  
    FROM raw.json_directory_core_sites_str
    -- Make sure that the joining key is unique (take just the latest batch if there is more)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY _ingested_at DESC) = 1
  ) AS src
    ON (tgt.site_id = src.site_id)
  WHEN MATCHED THEN
    UPDATE 
    SET 
      tgt.portfolio_id = src.portfolio_id,
      tgt.customer_id = src.customer_id,
      tgt.name = src.name,
      tgt.time_zone = src.time_zone,
      tgt.raw_json_value = src.raw_json_value,
      tgt._last_updated_at = SYSDATE(),
      tgt._is_active = true,
      tgt._stage_record_id = src._stage_record_id,
      tgt._loader_run_id = src._loader_run_id,
      tgt._ingested_at = src._ingested_at,
      tgt._staged_at = src._staged_at
  WHEN NOT MATCHED THEN
    INSERT (
      site_id, 
      portfolio_id,
      customer_id,
      name, 
      time_zone, 
      raw_json_value, 
      _is_active,
      _created_at,
      _last_updated_at,
      _stage_record_id, 
      _loader_run_id, 
      _ingested_at, 
      _staged_at) 
    VALUES (
      src.site_id, 
      src.portfolio_id,
      src.customer_id,
      src.name,
      src.time_zone,
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
