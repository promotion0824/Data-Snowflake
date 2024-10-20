-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the zone_air_temp_hourly_metrics table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.comfort_merge_comfort_hourly_metrics_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN

        MERGE INTO transformed.comfort_hourly_metrics AS tgt 
          USING (
            WITH cte_offsets AS (
              SELECT 
                so.asset_id, 
                'OffsetSetpoint' AS offset_type,
                MIN(CASE WHEN capability_name ilike '%heat%' THEN offset_value else null end) AS heating_offset_value,
                MAX(CASE WHEN capability_name ilike '%cool%' THEN offset_value else null end) AS cooling_offset_value,
                AVG(CASE WHEN NOT (capability_name ILIKE ANY ('%heat%', '%cool%')) THEN offset_value else null end) AS offset_value,
                _valid_from, _valid_to
                FROM transformed.comfort_setpoint_offsets so
                JOIN transformed.capabilities_assets ca on so.trend_id = ca.trend_id
                GROUP By so.asset_id, _valid_from, _valid_to
            )
            ,cte_scores AS (
              SELECT DISTINCT
                ts.asset_id,
                ca.site_id,
                ts.captured_at,
                DATE(CONVERT_TIMEZONE('UTC', time_zone, ts.captured_at)) AS date,
                CONVERT_TIMEZONE('UTC', time_zone, TIME_SLICE(captured_at, 1, 'HOUR')) AS date_hour_start,
                IFF(dates.is_weekday = TRUE, 'Weekday', 'Weekend') AS day_of_week_type,
                IFF(HOUR(date_hour_start) >= working_hours.default_value:hourStart AND HOUR(date_hour_start) < working_hours.default_value:hourEnd, true, false) AS is_working_hour,
                ts.unit,
                CASE ts.unit WHEN 'degC' THEN 4 WHEN 'degF' THEN 40 ELSE NULL END AS default_min,
                CASE ts.unit WHEN 'degC' THEN 38 WHEN 'degF' THEN 100 ELSE NULL END AS default_max,
                ts.setpoint_type,
                zone_air_temp,
                min_setpoint_value,
                max_setpoint_value,
                cso.heating_offset_value,
                cso.cooling_offset_value,
                cso.offset_value,
                deadband_offset.default_value:value::FLOAT AS offset_site_default,
                IFF(ts.unit = 'degC', 1, 1.8) AS offset_default,
                COALESCE(cso.offset_type,CASE WHEN offset_site_default IS NOT NULL THEN 'Site Default Offset' ELSE NULL END,'Default Offset') AS offset_type_used,
                COALESCE(cso.heating_offset_value,cso.offset_value,offset_site_default,offset_default) AS heating_offset_used,
                COALESCE(cso.cooling_offset_value,cso.offset_value,offset_site_default,offset_default) AS cooling_offset_used,
                CASE WHEN ts.setpoint_type = 'Effective heating/cooling setpoints' THEN IFNULL(min_setpoint_value,default_min) ELSE min_setpoint_value - heating_offset_used END AS min_setpoint,
                CASE WHEN ts.setpoint_type = 'Effective heating/cooling setpoints' THEN IFNULL(max_setpoint_value,default_max) ELSE max_setpoint_value + cooling_offset_used END AS max_setpoint,
                enqueued_at_utc AS last_enqueued_at_utc,
                CONVERT_TIMEZONE('UTC', time_zone, ts.captured_at) AS last_captured_at_local,
                :task_name AS _last_updated_by_task
              FROM transformed.transient_setpoints ts -- use this one for better performance on incremental runs;
              -- FROM transformed.comfort_measurements ts --use this one for full re-process;
              LEFT JOIN transformed.capabilities_assets ca
                ON ts.sensor_trend_id = ca.trend_id   
              JOIN utils.dates dates
                ON dates.date = DATE(CONVERT_TIMEZONE('UTC', time_zone, ts.captured_at))
              LEFT JOIN cte_offsets cso
                ON (ts.asset_id = cso.asset_id)
               AND (cso._valid_from <= CONVERT_TIMEZONE('UTC', time_zone, ts.captured_at) AND cso._valid_to >= CONVERT_TIMEZONE('UTC', time_zone, ts.captured_at))
              LEFT JOIN transformed.site_defaults deadband_offset
                ON (ca.site_id = deadband_offset.site_id)
               AND (deadband_offset.type = 'ZoneAirTemperatureDeadbandOffset') 
               AND (ts.unit = default_value:unit::STRING)
              LEFT JOIN transformed.site_defaults working_hours
                ON (ca.site_id = working_hours.site_id)
               AND (working_hours.type = 'WorkingHours')
               AND (working_hours._valid_from <= CONVERT_TIMEZONE('UTC', time_zone, TIME_SLICE(captured_at, 1, 'HOUR')) AND working_hours._valid_to >= CONVERT_TIMEZONE('UTC', time_zone, TIME_SLICE(captured_at, 1, 'HOUR')))
             )
            SELECT
              scores.asset_id,
              scores.site_id,
              scores.date,
              scores.date_hour_start,
              scores.unit,
              scores.setpoint_type,
              scores.offset_type_used,
              MIN(scores.heating_offset_used) AS heating_offset_used,
              MAX(scores.cooling_offset_used) AS cooling_offset_used,
              AVG(scores.zone_air_temp) AS avg_zone_air_temp,
              MIN(min_setpoint) AS min_setpoint_used,
              MAX(max_setpoint) AS max_setpoint_used,
              COUNT(*) AS sample_count,
              SUM(CASE WHEN scores.zone_air_temp BETWEEN scores.min_setpoint AND scores.max_setpoint THEN 1 ELSE 0 END) AS count_optimum,
              CAST((count_optimum / sample_count) * 100 AS NUMERIC(5,2)) AS comfort_score,
              scores.is_working_hour,
              day_of_week_type,
              MAX(last_captured_at_local) AS last_captured_at_local,
              MAX(scores.captured_at) AS last_captured_at_utc,
              MAX(scores.last_enqueued_at_utc) AS last_enqueued_at_utc,
              _last_updated_by_task
            FROM cte_scores scores
            GROUP BY 
              scores.asset_id, 
              scores.site_id,
              scores.date,
              scores.date_hour_start,
              scores.unit,
              scores.setpoint_type,
              scores.offset_type_used,
              scores.is_working_hour, 
              scores.day_of_week_type,
              _last_updated_by_task
            QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id, date_hour_start ORDER BY setpoint_type) = 1
          ) AS src
            ON (tgt.asset_id = src.asset_id AND tgt.date_hour_start = src.date_hour_start)
            
          WHEN MATCHED THEN        
            UPDATE 
            SET
              tgt.unit = src.unit,
              tgt.avg_zone_air_temp = src.avg_zone_air_temp,
              tgt.min_setpoint_used = src.min_setpoint_used,
              tgt.max_setpoint_used = src.max_setpoint_used,
              tgt.setpoint_type = src.setpoint_type,
              tgt.offset_type_used = src.offset_type_used,
              tgt.heating_offset_used = src.heating_offset_used,
              tgt.cooling_offset_used = src.cooling_offset_used,
              tgt.sample_count = src.sample_count,
              tgt.count_optimum = src.count_optimum,
              tgt.comfort_score = src.comfort_score,
              tgt.last_captured_at_local = src.last_captured_at_local,
              tgt.last_captured_at_utc = src.last_captured_at_utc,
              tgt.last_enqueued_at_utc = src.last_enqueued_at_utc,
              tgt._last_updated_at = SYSDATE(),
              tgt._last_updated_by_task = src._last_updated_by_task   
          WHEN NOT MATCHED THEN
            INSERT (
              asset_id, 
              date, 
              date_hour_start,
              unit,
              avg_zone_air_temp,
              min_setpoint_used,
              max_setpoint_used,
              setpoint_type,
              offset_type_used,
              heating_offset_used,
              cooling_offset_used,            
              sample_count,
              count_optimum,
              comfort_score,
              is_working_hour,
              day_of_week_type, 
              last_captured_at_local, 
              last_captured_at_utc,
              last_enqueued_at_utc,
              _created_by_task,
              _last_updated_by_task) 
            VALUES (
              src.asset_id, 
              src.date,
              src.date_hour_start, 
              src.unit,
              src.avg_zone_air_temp,
              src.min_setpoint_used,
              src.max_setpoint_used,
              src.setpoint_type,
              src.offset_type_used,
              src.heating_offset_used,
              src.cooling_offset_used,
              src.sample_count,
              src.count_optimum,
              src.comfort_score,
              src.is_working_hour, 
              src.day_of_week_type,
              src.last_captured_at_local, 
              src.last_captured_at_utc,
              src.last_enqueued_at_utc,
              _last_updated_by_task,
              _last_updated_by_task
            );
      END;		
    $$
;