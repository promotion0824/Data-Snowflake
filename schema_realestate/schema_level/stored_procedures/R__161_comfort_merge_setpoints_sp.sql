-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the comfort_setpoints table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.comfort_merge_setpoints_sp(task_name VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
  AS
  $$
    BEGIN
      
        CREATE OR REPLACE TABLE transformed.comfort_assets AS 
        SELECT * FROM transformed.comfort_assets_v
        ;

        CREATE OR REPLACE TABLE transformed.comfort_assets_unique AS
        SELECT DISTINCT
            asset_id,
            setpoint_trend_id,
            setpoint_model_id,
            offset_trend_id,
            offset_model_id,
            unit,
            sensor_trend_id
        FROM transformed.comfort_assets;

        CREATE OR REPLACE TRANSIENT TABLE transformed.comfort_transient_telemetry AS
        SELECT 
            ca.capability_id,
            ca.building_id,
            ca.asset_id,
            ca.site_id,
            ca.time_zone,
            ca.trend_id,
            ca.external_id,
            ts.timestamp_utc,
            ca.unit, 
            ts.telemetry_value,
            ts.enqueued_at
        FROM transformed.comfort_telemetry_str ts
        JOIN transformed.capabilities_assets ca
          ON (ts.dt_id = ca.capability_id)
        WHERE ca.asset_id IN (SELECT asset_id FROM transformed.terminal_units_assets)
        ;

        CREATE OR REPLACE TEMPORARY TABLE new_setpoint_values AS 
          WITH cte_value_changes AS (
            SELECT
                assets.asset_id,
                new_setpoints.trend_id,
                new_setpoints.external_id,
                new_setpoints.timestamp_utc AS captured_at,
                setpoint_model_id,
                assets.unit AS setpoint_unit,
                LAG(new_setpoints.telemetry_value) OVER (PARTITION BY assets.asset_id, new_setpoints.trend_id ORDER BY new_setpoints.timestamp_utc) AS previous_temp,
                IFF(previous_temp IS NULL, TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'), new_setpoints.timestamp_utc) AS _valid_from,
                new_setpoints.telemetry_value AS setpoint_value
            FROM transformed.comfort_transient_telemetry new_setpoints
              JOIN transformed.comfort_assets_unique assets 
                ON new_setpoints.trend_id = assets.setpoint_trend_id
            WHERE  (assets.unit = 'degC' AND new_setpoints.telemetry_value >= 2 AND new_setpoints.telemetry_value <= 50
               OR  assets.unit = 'degF' AND new_setpoints.telemetry_value >= 32 AND new_setpoints.telemetry_value <= 90)
            QUALIFY IFF((setpoint_value <> previous_temp OR previous_temp IS NULL), true, false) = true
            )
            SELECT 
              asset_id,
              trend_id,
              external_id,
              captured_at,
              setpoint_model_id,
              setpoint_unit,
              _valid_from,
              IFNULL(DATEADD(ms, -1, LEAD(_valid_from) OVER (PARTITION BY asset_id,trend_id ORDER BY captured_at)), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')) AS _valid_to,
              setpoint_value,
            FROM cte_value_changes
            ;

        -- If the first of new records has the same setpoint value as the last of the existing records set valid_to to valid_to of the new record 
        -- Update valid_to of the last existing record. If the setpoint_value is the same use valid_to of the new record, if it's different use valid_from of the new record - 1ms.
        UPDATE transformed.comfort_setpoints AS tgt
        SET 
          tgt.setpoint_model_id = src.setpoint_model_id,
          tgt.setpoint_unit = src.setpoint_unit,
          tgt._valid_to = CASE WHEN tgt.setpoint_value = src.setpoint_value THEN src._valid_to ELSE DATEADD(ms, -1, src.captured_at) END,
          tgt.external_id = src.external_id,
          tgt._last_updated_at = SYSDATE(),
          tgt._last_updated_by_task = ''--:task_name
        FROM new_setpoint_values AS src
        WHERE 
			        tgt.asset_id  = src.asset_id 
          AND tgt.trend_id = src.trend_id
          AND src.captured_at > tgt.first_captured_at
          AND tgt._valid_to = '9999-12-31 23:59:59.999' 
          AND src._valid_from = '0000-01-01 00:00:00.000'
          ;

        -- Insert the first new record from the new batch (if the value has changed)
        INSERT INTO transformed.comfort_setpoints (asset_id, trend_id, external_id, first_captured_at, setpoint_model_id, setpoint_unit, _valid_from, _valid_to, setpoint_value, _created_by_task, _last_updated_by_task) 
          SELECT 
            src.asset_id,
            src.trend_id,
            src.external_id,
            src.captured_at AS first_captured_at,
            src.setpoint_model_id,
            src.setpoint_unit,
            src.captured_at AS _valid_from,
            src._valid_to,
            src.setpoint_value,
            '', --:task_name,
            '' --:task_name
          FROM new_setpoint_values AS src
           JOIN transformed.comfort_setpoints AS tgt 
		         ON (tgt.asset_id  = src.asset_id AND tgt.trend_id = src.trend_id)  
          WHERE 
                tgt.asset_id  = src.asset_id
            AND tgt.trend_id = src.trend_id
            AND src.captured_at > tgt.first_captured_at
            AND src._valid_from = '0000-01-01 00:00:00.000'
            AND src.captured_at = DATEADD(ms, 1, tgt._valid_to)
            AND IFNULL(tgt.setpoint_value,0) <> IFNULL(src.setpoint_value,0)
          ;

        -- Insert all subsequent new setpoints and completely new records (asset_id + trend_id combination that has no previous setpoints) 
        INSERT INTO transformed.comfort_setpoints (asset_id, trend_id, external_id, first_captured_at, setpoint_model_id, setpoint_unit, _valid_from, _valid_to, setpoint_value, _created_by_task, _last_updated_by_task) 
          WITH cte_latest_current AS (
            SELECT 
              asset_id,
              external_id,
              trend_id,
              _valid_to
            FROM transformed.comfort_setpoints
            QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id, trend_id,external_id ORDER BY _valid_to DESC) = 1

          )
          SELECT DISTINCT
            src.asset_id,
            src.trend_id,
            src.external_id,
            src.captured_at,
            src.setpoint_model_id,
            src.setpoint_unit,
            src._valid_from,
            src._valid_to,
            src.setpoint_value,
            '', --:task_name,
            '' --:task_name
          FROM new_setpoint_values AS src
            LEFT JOIN cte_latest_current AS tgt 
                  ON (tgt.asset_id  = src.asset_id)
                 AND (tgt.trend_id = src.trend_id)
          WHERE (tgt.asset_id IS NULL OR src._valid_from > tgt._valid_to)
          ;

          -- there is a flaw somewhere in the above logic.  this update is to correct for that.
          UPDATE transformed.comfort_setpoints AS tgt
          SET 
            tgt._valid_to = '9999-12-31 23:59:59.999'
          FROM new_setpoint_values AS src
          WHERE 
              tgt.asset_id  = src.asset_id 
          AND tgt.trend_id = src.trend_id
          AND tgt._valid_from = '0000-01-01 00:00:00.000'
          AND tgt._valid_to = '0000-01-01 00:00:00.001'
          ;

      COMMIT;
      
      END;
  $$
  ;