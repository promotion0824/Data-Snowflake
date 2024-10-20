-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the zone_air run_sensor table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_chiller_run_sensor_sp(task_name VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
  AS
  $$
    BEGIN
      
        LET watermark TIMESTAMP_NTZ := (SELECT MAX(first_captured_at) FROM transformed.chiller_run_sensor);

        CREATE OR REPLACE TEMPORARY TABLE new_run_sensor_values AS
          WITH cte_value_changes AS (

            SELECT
                assets.asset_id,
                new_run_sensor.trend_id,
                new_run_sensor.timestamp_utc AS captured_at,
                assets.run_sensor_model_id,
                assets.unit AS run_sensor_unit,
                LAG(new_run_sensor.telemetry_value) OVER (PARTITION BY assets.asset_id, new_run_sensor.trend_id ORDER BY new_run_sensor.timestamp_utc) AS previous_temp,
                IFF(previous_temp IS NULL, TO_TIMESTAMP_NTZ('0000-01-01 00:00:00.000'), new_run_sensor.timestamp_utc) AS _valid_from,
                new_run_sensor.telemetry_value AS run_sensor_value,
                assets.site_id
            FROM transformed.time_series_enriched new_run_sensor
            JOIN transformed.chiller_assets assets 
              ON (new_run_sensor.trend_id = assets.run_sensor_trend_id)
            WHERE new_run_sensor.timestamp_utc > IFNULL(:watermark, TO_TIMESTAMP('0000-01-01'))
            QUALIFY IFF((run_sensor_value <> previous_temp OR previous_temp IS NULL), true, false) = true
            )

            SELECT 
              asset_id,
              trend_id,
              captured_at,
              run_sensor_model_id,
              run_sensor_unit,
              _valid_from,
              IFNULL(DATEADD(ms, -1, LEAD(_valid_from) OVER (PARTITION BY asset_id,trend_id ORDER BY captured_at)), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')) AS _valid_to,
              run_sensor_value,
              site_id
            FROM cte_value_changes
            ;
        -- If the first of new records has the same run_sensor value as the last of the existing records set valid_to to valid_to of the new record 
        -- Update valid_to of the last existing record. If the run_sensor_value is the same use valid_to of the new record, if it's different use valid_from of the new record - 1ms.
        UPDATE transformed.chiller_run_sensor AS tgt
        SET 
          tgt.run_sensor_model_id = src.run_sensor_model_id,
          tgt.run_sensor_unit = src.run_sensor_unit,
          tgt._valid_to = CASE WHEN tgt.run_sensor_value = src.run_sensor_value THEN src._valid_to ELSE DATEADD(ms, -1, src.captured_at) END,
          tgt.site_id = src.site_id,
          tgt._last_updated_at = SYSDATE(),
          tgt._last_updated_by_task = :task_name
        FROM new_run_sensor_values AS src
        WHERE 
			  tgt.asset_id  = src.asset_id 
          AND tgt.trend_id = src.trend_id
          AND src.captured_at > tgt.first_captured_at
          AND tgt._valid_to = '9999-12-31 23:59:59.999' 
          AND src._valid_from = '0000-01-01 00:00:00.000'
          ;

        -- Insert the first new record from the new batch (if the value has changed)
        INSERT INTO transformed.chiller_run_sensor (asset_id, trend_id, first_captured_at, run_sensor_model_id, run_sensor_unit, _valid_from, _valid_to, run_sensor_value, site_id,_created_by_task, _last_updated_by_task) 
          SELECT 
            src.asset_id,
            src.trend_id,
            src.captured_at AS first_captured_at,
            src.run_sensor_model_id,
            src.run_sensor_unit,
            src.captured_at AS _valid_from,
            src._valid_to,
            src.run_sensor_value,
            src.site_id,
            :task_name,
            :task_name
          FROM new_run_sensor_values AS src
           JOIN transformed.chiller_run_sensor AS tgt 
		     ON (tgt.asset_id  = src.asset_id AND tgt.trend_id = src.trend_id)  
          WHERE 
                tgt.asset_id  = src.asset_id
            AND tgt.trend_id = src.trend_id
            AND src.captured_at > tgt.first_captured_at
            AND src._valid_from = '0000-01-01 00:00:00.000'
            AND src.captured_at = DATEADD(ms, 1, tgt._valid_to)
            AND IFNULL(tgt.run_sensor_value,0) <> IFNULL(src.run_sensor_value,0)
          ;

        -- Insert all subsequent new run_sensors and completely new records (asset_id + trend_id combination that has no previous run_sensors) 
        INSERT INTO transformed.chiller_run_sensor (asset_id, trend_id, first_captured_at, run_sensor_model_id, run_sensor_unit, _valid_from, _valid_to, run_sensor_value, site_id, _created_by_task, _last_updated_by_task) 
          WITH cte_latest_current AS (
            SELECT 
              asset_id,
              trend_id,
              _valid_to
            FROM transformed.chiller_run_sensor
            QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id, trend_id ORDER BY _valid_to DESC) = 1

          )
          SELECT DISTINCT
            src.asset_id,
            src.trend_id,
            src.captured_at,
            src.run_sensor_model_id,
            src.run_sensor_unit,
            src._valid_from,
            src._valid_to,
            src.run_sensor_value,
            src.site_id,
            :task_name,
            :task_name
          FROM new_run_sensor_values AS src
            LEFT JOIN cte_latest_current AS tgt ON tgt.asset_id  = src.asset_id AND tgt.trend_id = src.trend_id  
          WHERE tgt.asset_id IS NULL OR src._valid_from > tgt._valid_to
          ;

          -- there is a flaw somewhere in the above logic.  this update is to correct for that.
          UPDATE transformed.chiller_run_sensor AS tgt
          SET 
            tgt._valid_to = '9999-12-31 23:59:59.999'
          FROM new_run_sensor_values AS src
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