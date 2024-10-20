-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the return_air_temperature_setpoints table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_return_air_temperature_setpoints_sp(task_name VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
  AS
  $$
    BEGIN
      
        LET watermark TIMESTAMP_NTZ := (SELECT MAX(first_captured_at) FROM transformed.return_air_temperature_setpoints);

        CREATE OR REPLACE TEMPORARY TABLE new_setpoint_values AS 
          WITH cte_value_changes AS (

            SELECT
                assets.asset_id,
                new_setpoints.trend_id,
                new_setpoints.timestamp_utc AS captured_at,
                LAG(new_setpoints.telemetry_value) OVER (PARTITION BY assets.asset_id ORDER BY new_setpoints.timestamp_utc) AS previous_temp,
                IFF(previous_temp IS NULL, TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'), new_setpoints.timestamp_utc) AS _valid_from,
                CASE WHEN ca.site_id = '24695d9d-269c-4763-966c-b3ab5992dc52' OR ca.capability_name IN ('Return Air Temperature Setpoint', 'Return Air Temp Sp', 'Return Air Temp Specific Sp', 'return air temp sp') THEN  new_setpoints.telemetry_value ELSE 72 END  AS return_air_temperature_sp,
                assets.site_id
            FROM transformed.time_series_enriched new_setpoints
              JOIN transformed.return_air_temperature_assets assets 
			          ON (new_setpoints.trend_id = assets.setpoint_trend_id)
              JOIN transformed.capabilities_assets ca 
                ON (assets.setpoint_trend_id = ca.trend_id)
            WHERE 
                   new_setpoints.timestamp_utc > IFNULL(:watermark, TO_TIMESTAMP('0000-01-01'))
              AND ((assets.unit = 'degC' AND new_setpoints.telemetry_value >= -5 AND new_setpoints.telemetry_value <= 50)
               OR  (assets.unit = 'degF' AND new_setpoints.telemetry_value >= 48 AND new_setpoints.telemetry_value <= 90)) 
            QUALIFY IFF((return_air_temperature_sp <> previous_temp OR previous_temp IS NULL), true, false) = true

            )

            SELECT 
              asset_id,
              trend_id,
              captured_at,
              _valid_from,
              IFNULL(DATEADD(ms, -1, LEAD(_valid_from) OVER (PARTITION BY asset_id ORDER BY captured_at)), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')) AS _valid_to,
              return_air_temperature_sp,
			  site_id
            FROM cte_value_changes;

        -- If the first of new records has the same setpoint value as the last of the existing records set valid_to to valid_to of the new record 
        -- Update valid_to of the last existing record. If the return_air_temperature_sp is the same use valid_to of the new record, if it's different use valid_from of the new record - 1ms.
        UPDATE transformed.return_air_temperature_setpoints AS tgt
        SET 
          tgt._valid_to = CASE WHEN tgt.return_air_temperature_sp = src.return_air_temperature_sp THEN src._valid_to ELSE DATEADD(ms, -1, src.captured_at) END,
          tgt.site_id = src.site_id,
          tgt._last_updated_at = SYSDATE(),
          tgt._last_updated_by_task = :task_name
        FROM new_setpoint_values AS src
        WHERE 
			        tgt.asset_twin_id  = src.asset_id 
          AND tgt.trend_id = src.trend_id
          AND src.captured_at > tgt.first_captured_at 
          AND tgt._valid_to = '9999-12-31 23:59:59.999' 
          AND src._valid_from = '0000-01-01 00:00:00.000';

        -- Insert the first new record from the new batch (if the value has changed)
        INSERT INTO transformed.return_air_temperature_setpoints (asset_twin_id, trend_id, first_captured_at, _valid_from, _valid_to, return_air_temperature_sp, site_id,_created_by_task, _last_updated_by_task) 
          SELECT 
            src.asset_id,
            src.trend_id,
            src.captured_at AS first_captured_at,
            src.captured_at AS _valid_from,
            src._valid_to,
            src.return_air_temperature_sp,
            src.site_id,
            :task_name,
            :task_name
          FROM new_setpoint_values AS src
           JOIN transformed.return_air_temperature_setpoints AS tgt 
		         ON (tgt.asset_twin_id  = src.asset_id AND tgt.trend_id = src.trend_id)  
          WHERE 
                tgt.asset_twin_id  = src.asset_id 
            AND tgt.trend_id = src.trend_id
            AND src.captured_at > tgt.first_captured_at 
            AND src._valid_from = '0000-01-01 00:00:00.000' 
            AND src.captured_at = DATEADD(ms, 1, tgt._valid_to)
            AND IFNULL(tgt.return_air_temperature_sp,0) <> IFNULL(src.return_air_temperature_sp,0);

        -- Insert all subsequent new setpoints and completely new records (asset_id + trend_id combination that has no previous setpoints) 
        INSERT INTO transformed.return_air_temperature_setpoints (asset_twin_id, trend_id, first_captured_at, _valid_from, _valid_to, return_air_temperature_sp, site_id, _created_by_task, _last_updated_by_task) 
          WITH cte_latest_current AS (
            SELECT 
              asset_twin_id,
              trend_id,
              _valid_to
            FROM transformed.return_air_temperature_setpoints
            QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_twin_id, trend_id ORDER BY _valid_to DESC) = 1

          )
          SELECT DISTINCT
            src.asset_id, 
            src.trend_id, 
            src.captured_at, 
            src._valid_from, 
            src._valid_to, 
            src.return_air_temperature_sp,
            src.site_id,
            :task_name, 
            :task_name
          FROM new_setpoint_values AS src
            LEFT JOIN cte_latest_current AS tgt ON tgt.asset_twin_id  = src.asset_id AND tgt.trend_id = src.trend_id  
          WHERE tgt.asset_twin_id IS NULL OR src._valid_from > tgt._valid_to;

      COMMIT;
      
      END;
  $$
  ;   