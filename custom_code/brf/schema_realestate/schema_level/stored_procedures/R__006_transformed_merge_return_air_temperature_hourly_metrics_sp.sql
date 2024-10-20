-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the return_air_temperature_hourly_metrics table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_return_air_temperature_hourly_metrics_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
        LET watermark TIMESTAMP_NTZ := (SELECT TIME_SLICE(MAX(last_captured_at_utc), 1, 'HOUR') FROM transformed.return_air_temperature_hourly_metrics);
        
        MERGE INTO transformed.return_air_temperature_hourly_metrics AS tgt 
          
          USING (

            WITH cte_scores AS (

              SELECT
                temp_measurements.asset_twin_id,
				        assets.floor_id,
                assets.site_id,
                TIME_SLICE(captured_at, 1, 'HOUR') AS date_hour_start,
                MAX(captured_at) AS last_captured_at,
                AVG(CASE WHEN return_air_temperature between -20 and 35 THEN (return_air_temperature * 9/5) + 32 ELSE return_air_temperature END) AS avg_return_air_temperature,
                AVG(return_air_temperature_sp) AS avg_return_air_temperature_sp,
                AVG(return_air_humidity) AS avg_return_air_humidity,
                COUNT(*) AS sample_count
              FROM transformed.return_air_temperature_measurements temp_measurements
                JOIN transformed.return_air_temperature_assets assets
                  ON (temp_measurements.asset_twin_id = assets.asset_id)     
              WHERE 
                captured_at >= IFNULL(:watermark, TO_TIMESTAMP('0000-01-01')) 
              GROUP BY 
                temp_measurements.asset_twin_id,
				        assets.floor_id,
                assets.site_id,
                date_hour_start

            )
            SELECT
              scores.asset_twin_id,
              scores.site_id,
			        scores.floor_id,
              DATE(CONVERT_TIMEZONE('UTC', sites.time_zone, scores.date_hour_start)) AS date,
              CONVERT_TIMEZONE('UTC', sites.time_zone, scores.date_hour_start) AS date_hour_start,
              MAX(CONVERT_TIMEZONE('UTC', sites.time_zone, scores.last_captured_at)) AS last_captured_at_local,
              MAX(scores.last_captured_at) AS last_captured_at_utc,
              IFF(HOUR(CONVERT_TIMEZONE('UTC', sites.time_zone, scores.date_hour_start)) >= working_hours.default_value:hourStart AND HOUR(CONVERT_TIMEZONE('UTC', sites.time_zone, scores.date_hour_start)) < working_hours.default_value:hourEnd, true, false) AS is_working_hour,
              IFF(dates.is_weekday = TRUE, 'Weekday', 'Weekend') AS day_of_week_type,
              AVG(scores.avg_return_air_temperature) AS avg_return_air_temperature,
              AVG(avg_return_air_temperature_sp) AS avg_return_air_temperature_sp,
			        AVG(avg_return_air_temperature_sp - avg_return_air_temperature) AS deviation,
              AVG(avg_return_air_humidity) AS avg_return_air_humidity,
              SUM(sample_count) AS sample_count
            FROM cte_scores scores
              JOIN transformed.sites sites 
                ON (scores.site_id = sites.site_id)   
              JOIN utils.dates dates
                ON (DATE(CONVERT_TIMEZONE('UTC', sites.time_zone, scores.date_hour_start)) = dates.date)                
              JOIN transformed.site_defaults working_hours
                ON (scores.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours' AND working_hours._valid_from <= scores.date_hour_start AND working_hours._valid_to >= scores.date_hour_start)
            GROUP BY 
              scores.asset_twin_id, 
			  scores.floor_id,
              scores.site_id,
              date, 
              CONVERT_TIMEZONE('UTC', sites.time_zone, scores.date_hour_start), 
              is_working_hour, 
              day_of_week_type, 
              sites.time_zone 
          ) AS src
            ON (tgt.asset_twin_id = src.asset_twin_id AND tgt.date_hour_start = src.date_hour_start)
            
          WHEN MATCHED THEN
          
            UPDATE 
            SET
			        tgt.floor_id = src.floor_id,
              tgt.last_captured_at_local = src.last_captured_at_local,
              tgt.last_captured_at_utc = src.last_captured_at_utc,
              tgt.avg_return_air_temperature = src.avg_return_air_temperature,
              tgt.avg_return_air_temperature_sp = src.avg_return_air_temperature_sp,
			        tgt.deviation = src.deviation,
              tgt.avg_return_air_humidity = src.avg_return_air_humidity,
              tgt.sample_count = src.sample_count,
              tgt._last_updated_at = SYSDATE(),
              tgt._last_updated_by_task = :task_name
              
          WHEN NOT MATCHED THEN
          
            INSERT (
              asset_twin_id,
			        floor_id,
              site_id,
              date, 
              date_hour_start,
              last_captured_at_local, 
              last_captured_at_utc,
              is_working_hour,
              day_of_week_type, 
              avg_return_air_temperature, 
              avg_return_air_temperature_sp,
			        deviation,
              avg_return_air_humidity,
              sample_count, 
              _created_by_task,
              _last_updated_by_task) 
            VALUES (
              src.asset_twin_id,
			        src.floor_id,
              src.site_id,
              src.date,
              src.date_hour_start,
              src.last_captured_at_local,
              src.last_captured_at_utc,
              src.is_working_hour,
              src.day_of_week_type,
              src.avg_return_air_temperature,
              src.avg_return_air_temperature_sp,
			        src.deviation,
              src.avg_return_air_humidity,
              src.sample_count,  
              :task_name,
              :task_name
            );
			
			CREATE OR REPLACE TABLE transformed.return_air_temperature_daily_metrics AS
				SELECT * FROM transformed.return_air_temperature_daily_metrics_v
;
      END;
    $$
;