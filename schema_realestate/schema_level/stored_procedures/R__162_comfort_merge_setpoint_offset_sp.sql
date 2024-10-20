-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the comfort_setpoints table
-- ******************************************************************************************************************************CREATE OR REPLACE PROCEDURE transformed.comfort_merge_setpoint_offset_sp(task_name VARCHAR)
CREATE OR REPLACE PROCEDURE transformed.comfort_merge_setpoint_offset_sp(task_name VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
  AS
  $$
    BEGIN

        CREATE OR REPLACE TEMPORARY TABLE transformed.new_offset_values AS 
          WITH cte_value_changes AS (
            SELECT
                assets.asset_id,
                new_offset.trend_id,
                new_offset.external_id,
                new_offset.timestamp_utc AS captured_at,
                assets.offset_model_id,
                assets.unit AS unit,
                LAG(new_offset.telemetry_value) OVER (PARTITION BY assets.asset_id, new_offset.trend_id ORDER BY new_offset.timestamp_utc) AS previous_temp,
                IFF(previous_temp IS NULL, TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'), new_offset.timestamp_utc) AS _valid_from,
                new_offset.telemetry_value AS offset_value
            FROM transformed.comfort_transient_telemetry new_offset
              JOIN transformed.comfort_assets_unique assets 
                ON new_offset.trend_id = assets.offset_trend_id
            QUALIFY IFF((offset_value <> previous_temp OR previous_temp IS NULL), true, false) = true
            )
            SELECT 
              asset_id,
              trend_id,
              external_id,
              captured_at,
              offset_model_id,
              unit,
              _valid_from,
              IFNULL(DATEADD(ms, -1, LEAD(_valid_from) OVER (PARTITION BY asset_id,trend_id ORDER BY captured_at)), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')) AS _valid_to,
              offset_value,
            FROM cte_value_changes
            ;

        -- If the first of new records has the same setpoint value as the last of the existing records set valid_to to valid_to of the new record 
        -- Update valid_to of the last existing record. If the offset_value is the same use valid_to of the new record, if it's different use valid_from of the new record - 1ms.
        UPDATE transformed.comfort_setpoint_offsets AS tgt
        SET 
          tgt.offset_model_id = src.offset_model_id,
          tgt.unit = src.unit,
          tgt._valid_to = CASE WHEN tgt.offset_value = src.offset_value THEN src._valid_to ELSE DATEADD(ms, -1, src.captured_at) END,
          tgt.external_id = src.external_id,
          tgt._last_updated_at = SYSDATE(),
          tgt._last_updated_by_task = ''--:task_name
        FROM transformed.new_offset_values AS src
        WHERE 
			        tgt.asset_id  = src.asset_id 
          AND tgt.trend_id = src.trend_id
          AND src.captured_at > tgt.first_captured_at
          AND tgt._valid_to = '9999-12-31 23:59:59.999' 
          AND src._valid_from = '0000-01-01 00:00:00.000'
          ;

        -- Insert the first new record from the new batch (if the value has changed)
        INSERT INTO transformed.comfort_setpoint_offsets (asset_id, trend_id, external_id, first_captured_at, offset_model_id, unit, _valid_from, _valid_to, offset_value, _created_by_task, _last_updated_by_task) 
          SELECT 
            src.asset_id,
            src.trend_id,
            src.external_id,
            src.captured_at AS first_captured_at,
            src.offset_model_id,
            src.unit,
            src.captured_at AS _valid_from,
            src._valid_to,
            src.offset_value,
            '', --:task_name,
            '' --:task_name
          FROM transformed.new_offset_values AS src
           JOIN transformed.comfort_setpoint_offsets AS tgt 
		         ON (tgt.asset_id  = src.asset_id AND tgt.trend_id = src.trend_id)  
          WHERE 
                tgt.asset_id  = src.asset_id
            AND tgt.trend_id = src.trend_id
            AND src.captured_at > tgt.first_captured_at
            AND src._valid_from = '0000-01-01 00:00:00.000'
            AND src.captured_at = DATEADD(ms, 1, tgt._valid_to)
            AND IFNULL(tgt.offset_value,0) <> IFNULL(src.offset_value,0)
          ;

        -- Insert all subsequent new setpoints and completely new records (asset_id + trend_id combination that has no previous setpoints) 
        INSERT INTO transformed.comfort_setpoint_offsets (asset_id, trend_id, external_id, first_captured_at, offset_model_id, unit, _valid_from, _valid_to, offset_value, _created_by_task, _last_updated_by_task) 
          WITH cte_latest_current AS (
            SELECT 
              asset_id,
              external_id,
              trend_id,
              _valid_to
            FROM transformed.comfort_setpoint_offsets
            QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id, trend_id,external_id ORDER BY _valid_to DESC) = 1

          )
          SELECT DISTINCT
            src.asset_id,
            src.trend_id,
            src.external_id,
            src.captured_at,
            src.offset_model_id,
            src.unit,
            src._valid_from,
            src._valid_to,
            src.offset_value,
            '', --:task_name,
            '' --:task_name
          FROM transformed.new_offset_values AS src
            LEFT JOIN cte_latest_current AS tgt 
                  ON (tgt.asset_id  = src.asset_id)
                 AND (tgt.trend_id = src.trend_id)
          WHERE (tgt.asset_id IS NULL OR src._valid_from > tgt._valid_to)
          ;

          -- there is a flaw somewhere in the above logic.  this update is to correct for that.
          UPDATE transformed.comfort_setpoint_offsets AS tgt
          SET 
            tgt._valid_to = '9999-12-31 23:59:59.999'
          FROM transformed.new_offset_values AS src
          WHERE 
              tgt.asset_id  = src.asset_id 
          AND tgt.trend_id = src.trend_id
          AND tgt._valid_from = '0000-01-01 00:00:00.000'
          AND tgt._valid_to = '0000-01-01 00:00:00.001'
          ;
      
      END;
  $$
  ;