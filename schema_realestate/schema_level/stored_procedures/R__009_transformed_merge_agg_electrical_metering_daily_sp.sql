-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the aggregate table
-- This is called via transformed.merge_agg_electrical_metering_daily_tk which is dependent on merge_agg_electrical_metering_hourly_tk
-- USAGE:  CALL transformed.merge_agg_electrical_metering_daily_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.merge_agg_electrical_metering_daily_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	  BEGIN
		  MERGE INTO transformed.agg_electrical_metering_daily AS tgt
		  USING (
				  WITH watermark AS 
					(
				  SELECT
					IFNULL(MAX(date_local),'2019-01-01') AS max_date,
					site_id
				  FROM transformed.agg_electrical_metering_daily
				  WHERE date_local <= DATEADD('d',-2,SYSDATE())
				  GROUP BY site_id
					)
				,cte_agg_day AS (
				  SELECT 
				    ts.capability_id,
					ts.building_id,
					ts.trend_id,
					ts.site_id,
					ts.date_local,
					ts.sensor_type,
					MAX(CASE ts.unit 
					  WHEN 'Wh'  THEN ts.end_of_day_value / 1000.0 
					  WHEN 'kWh' THEN ts.end_of_day_value
					  ELSE NULL
					END) AS end_of_day_kwh,
					SUM(CASE ts.unit 
					  WHEN 'Wh'  THEN ts.hourly_usage / 1000.0 
					  WHEN 'kWh' THEN ts.hourly_usage
					  ELSE NULL
					END) AS daily_usage_kwh,
					SUM(CASE ts.unit 
					  WHEN 'W'  THEN ts.avg_value_hour / 1000.0 
					  WHEN 'kW' THEN ts.avg_value_hour
					  ELSE NULL
					END) AS virtual_daily_usage_kwh,
					FIRST_VALUE( MIN(CASE ts.unit 
					  WHEN 'Wh'  THEN end_of_prev_hour_value / 1000.0 
					  WHEN 'kWh' THEN end_of_prev_hour_value
					  ELSE NULL
					END)) OVER ( PARTITION BY ts.trend_id, ts.date_local ORDER BY ts.trend_id ) AS first_value_day_kwh,
					MAX(ts.last_captured_at_local) AS last_captured_at_local,
					MAX(ts.last_captured_at_utc) AS last_captured_at_utc,
					MAX(ts.last_enqueued_at_utc) AS last_enqueued_at_utc,
					MAX(ts.last_refreshed_at_utc) AS last_refreshed_at_utc
				FROM transformed.agg_electrical_metering_hourly_smoothed  ts
				LEFT JOIN watermark
				  ON (ts.site_id = watermark.site_id)
				WHERE ts.date_local >= DATEADD('d',-1,COALESCE(max_date,'2018-01-01'))
				GROUP BY
					ts.capability_id,
					ts.building_id,
					ts.trend_id,
					ts.site_id,
					ts.date_local,
					ts.sensor_type
			)
				SELECT
						capability_id,
						building_id,
						trend_id,
						site_id,
						date_local,
						sensor_type,
						virtual_daily_usage_kwh,
						end_of_day_kwh,
						COALESCE(LAG(end_of_day_kwh) IGNORE nulls OVER (PARTITION BY trend_id ORDER BY trend_id, date_local),first_value_day_kwh) AS end_of_prev_day_value_kwh,
						daily_usage_kwh,
						end_of_day_kwh - end_of_prev_day_value_kwh AS daily_usage_kwh_EOD_calc,
						last_captured_at_local,
                        last_captured_at_utc,
                        last_refreshed_at_utc,
						last_enqueued_at_utc
				FROM cte_agg_day
					)
		 AS src
				  ON (    
					      tgt.date_local = src.date_local
					  AND tgt.capability_id = src.capability_id
					 )
		  WHEN MATCHED THEN
			UPDATE 
			SET
					tgt.trend_id = src.trend_id,
					tgt.building_id = src.building_id,
					tgt.site_id = src.site_id,						
					tgt.daily_usage_kwh = src.daily_usage_kwh,
					tgt.virtual_daily_usage_kwh = src.virtual_daily_usage_kwh,
					tgt.sensor_type = src.sensor_type,
					tgt.end_of_day_kwh = src.end_of_day_kwh,
					tgt.end_of_prev_day_value_kwh = src.end_of_prev_day_value_kwh,
					tgt.daily_usage_kwh_EOD_calc = src.daily_usage_kwh_EOD_calc,
					tgt._last_updated_by_task = :task_name,
					tgt.last_captured_at_local = src.last_captured_at_local,
					tgt.last_captured_at_utc = src.last_captured_at_utc,
					tgt.last_refreshed_at_utc = COALESCE(src.last_refreshed_at_utc,SYSDATE()),
					tgt.last_enqueued_at_utc = src.last_enqueued_at_utc
		  WHEN NOT MATCHED THEN
			INSERT (
				    capability_id,
					building_id,
					site_id,
					trend_id,
					date_local,
					daily_usage_kwh,
					virtual_daily_usage_kwh,
					sensor_type,
					end_of_day_kwh,
					end_of_prev_day_value_kwh,
					daily_usage_kwh_EOD_calc,
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
					src.daily_usage_kwh,
					src.virtual_daily_usage_kwh,
					src.sensor_type,
					src.end_of_day_kwh,
					src.end_of_prev_day_value_kwh,
					src.daily_usage_kwh_EOD_calc,
					SYSDATE(), 
					:task_name,
                    :task_name,
					src.last_captured_at_local,
					src.last_captured_at_utc,
					SYSDATE(),
					src.last_enqueued_at_utc
			);
	  CREATE OR REPLACE TABLE transformed.electrical_metering_detail AS SELECT * FROM transformed.electrical_metering_detail_v;
	END	
    $$
;