-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the aggregate table
-- This is called via transformed.merge_agg_electrical_metering_hourly_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_agg_electrical_metering_hourly_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_agg_electrical_metering_hourly_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	BEGIN
	  MERGE INTO transformed.agg_electrical_metering_hourly AS tgt
		USING ( 
			  WITH watermark AS 
				(
				  SELECT site_id,
					IFNULL(MAX(date_local)-15,'2019-01-01') AS max_date --go back 15 days to handle any gaps in data
				  FROM transformed.agg_electrical_metering_hourly
				  GROUP BY site_id
				)
			  ,cte_agg_day AS (
			  SELECT 
			    ema.capability_id,
				ema.building_id,
				ts.trend_id,
				ts.site_id,
				ts.date_local,
				date_trunc('hour',ts.timestamp_local) as date_time_local_hour,
				ts.telemetry_value,
				LAG(ts.telemetry_value, 1, 0) OVER (PARTITION BY ts.trend_id ORDER BY ts.trend_id, ts.timestamp_utc) AS previous_telemetry_value,
				LAST_VALUE( ts.telemetry_value ) OVER ( PARTITION BY ts.trend_id, date_time_local_hour ORDER BY ts.trend_id, ts.timestamp_utc ) AS last_value_hour,
				ema.unit,
				ema.sensor_type,
				IFNULL(d.default_value:EnergyMinThreshold,400) AS energy_min_threshold,
				ts.timestamp_utc,
                ts.timestamp_local,
				ts.enqueued_at AS enqueued_at_utc
			  FROM transformed.telemetry ts
			    LEFT JOIN watermark wm
				  ON (ts.site_id = wm.site_id)
				JOIN transformed.electrical_metering_hierarchy_v ema 
				  ON (ts.trend_id = ema.trend_id)
                LEFT JOIN transformed.site_defaults d
			      ON (ema.site_id = d.site_id)
                 AND (d.type ='EnergyMinThreshold')
			  WHERE ts.date_local >= IFNULL(wm.max_date,'2020-01-01')
			  QUALIFY ((ema.sensor_type = 'Energy'
                       AND ts.telemetry_value > IFNULL(d.default_value:EnergyMinThreshold,400)
                       AND previous_telemetry_value > IFNULL(d.default_value:EnergyMinThreshold,400)
                       AND ts.telemetry_value > previous_telemetry_value)
                       AND ABS(ts.telemetry_value - previous_telemetry_value) / previous_telemetry_value < .90)
						OR ema.sensor_type = 'Power'
			  )
			  SELECT 
			    ts.capability_id,
				ts.building_id,
				ts.trend_id,
				ts.site_id,
				ts.date_local,
                ts.date_time_local_hour,
				AVG(ts.telemetry_value) AS avg_value_hour,
				MIN(ts.telemetry_value) AS min_value_hour,
				MAX(ts.telemetry_value) AS max_value_hour,
				COUNT(ts.telemetry_value) AS values_count,
				MAX(ts.last_value_hour) AS end_of_hour_value_prelim,
				MAX(ts.energy_min_threshold) AS min_threshold_kwh,
				LAG(end_of_hour_value_prelim, 1, 0) OVER (PARTITION BY ts.trend_id ORDER BY ts.trend_id, ts.date_time_local_hour,max_value_hour desc) AS end_of_prev_hour_prelim,
				LAG(max_value_hour, 1, 0) OVER (PARTITION BY ts.trend_id ORDER BY ts.trend_id, ts.date_time_local_hour,max_value_hour desc) AS max_prev_hour,
				CASE 
					WHEN end_of_prev_hour_prelim <= 0 
					THEN end_of_hour_value_prelim 
					ELSE end_of_prev_hour_prelim 
				END AS end_of_prev_hour_value,
				CASE WHEN 
						(
						     max_value_hour > 100000000 
						 AND end_of_hour_value_prelim < 100000000 
						 AND end_of_hour_value_prelim < end_of_prev_hour_value
						 ) 
						OR (
							max_prev_hour > 100000000 
							AND end_of_hour_value_prelim < end_of_prev_hour_value
							)
					 THEN end_of_hour_value_prelim + 429496729 
					 ELSE end_of_hour_value_prelim 
				END as end_of_hour_value,
				end_of_hour_value - end_of_prev_hour_value AS hourly_usage,
				ts.unit,
				ts.sensor_type,
				MAX(timestamp_utc) AS last_captured_at_utc,
				MAX(timestamp_local) AS last_captured_at_local,
				MAX(enqueued_at_utc) AS last_enqueued_at_utc
			  FROM cte_agg_day ts
			  GROUP BY
			    ts.capability_id,
				ts.building_id,
				ts.trend_id,
				ts.site_id,
				ts.date_local,
				ts.date_time_local_hour,
				ts.unit,
				ts.sensor_type
              HAVING (sensor_type = 'Power' 
                  OR  end_of_hour_value_prelim > min_threshold_kwh)
		)
	 AS src
			  ON (    
				      tgt.trend_id = src.trend_id
				  AND tgt.date_time_local_hour = src.date_time_local_hour
				 )
	  WHEN MATCHED THEN
		UPDATE 
		SET
				tgt.building_id = src.building_id,
				tgt.capability_id = src.capability_id,
				tgt.site_id = src.site_id,
				tgt.avg_value_hour = src.avg_value_hour,
				tgt.min_value_hour = src.min_value_hour,
				tgt.max_value_hour = src.max_value_hour,
				tgt.values_count = src.values_count,
				tgt.end_of_hour_value = src.end_of_hour_value,
				tgt.end_of_prev_hour_value = src.end_of_prev_hour_value,
				tgt.hourly_usage = src.hourly_usage,
				tgt.sensor_type = src.sensor_type,
				tgt.unit = src.unit,
				tgt._last_updated_by_task = :task_name,
                tgt.last_captured_at_local = src.last_captured_at_local,
	            tgt.last_captured_at_utc = src.last_captured_at_utc,
                tgt.last_refreshed_at_utc = SYSDATE(),
				tgt.last_enqueued_at_utc = src.last_enqueued_at_utc
	  WHEN NOT MATCHED THEN
		INSERT (
				capability_id,
				building_id,
				site_id,
				trend_id,
				date_local,
				date_time_local_hour,
				avg_value_hour,
				min_value_hour,
				max_value_hour,
				values_count,
				end_of_hour_value,
				end_of_prev_hour_value,
				hourly_usage,
				sensor_type,
				unit,
				_created_at,
				_created_by_task,
				_last_updated_by_task,
                last_captured_at_local,
                last_captured_at_utc,
                last_refreshed_at_utc,
				last_enqueued_at_utc
		)
		VALUES (
				src.capability_id,
				src.building_id,
				src.site_id,
				src.trend_id,
				src.date_local,
				src.date_time_local_hour,
				src.avg_value_hour,
				src.min_value_hour,
				src.max_value_hour,
				src.values_count,
				src.end_of_hour_value,
				src.end_of_prev_hour_value,
				src.hourly_usage,	
				src.sensor_type,
				src.unit, 
				SYSDATE(),
				:task_name,
                :task_name,
                last_captured_at_local,
                last_captured_at_utc,
                SYSDATE(),
				last_enqueued_at_utc
		);
		create or replace temporary table transformed.dummy AS SELECT TOP 1 date_local FROM transformed.telemetry_str;
	END;
    $$
;