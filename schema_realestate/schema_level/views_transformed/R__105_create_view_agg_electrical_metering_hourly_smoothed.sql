-- ******************************************************************************************************************************
-- Create view
-- This view is called from merge_agg_electrical_metering_daily_sp to populate daily aggregation from hourly
-- The data is retrieved from agg_electrical_metering_daily which is populated by merge_agg_electrical_metering_hourly_sp
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.agg_electrical_metering_hourly_smoothed AS
		-- Build table of all possible trend_id/dates for use as base of outer join
	    WITH watermark AS 
		(
		  SELECT
				IFNULL(MAX(last_refreshed_at_utc),'2019-01-01') AS max_updated
		  FROM transformed.agg_electrical_metering_daily
		)
		-- limit dates to only those in the data set to be processed
		,
		  cte_trendid_days_hours AS (
            SELECT 
			 ts.capability_id,
			 ts.building_id,
			 ts.site_id,
			 ts.trend_id,
             ts.sensor_type,
			 ts.unit,
             d.date AS expected_date,
             d.date_time_hour AS expected_date_hour
			FROM (SELECT 
					capability_id,
					building_id,
					site_id,
					trend_id,
					unit,
					sensor_type,
					MIN(agg.date_local) AS min_date,
					MAX(agg.date_local) AS max_date
				  FROM transformed.agg_electrical_metering_hourly agg
				  -- need to go 90 days back for the averaging out of missing values after a good record comes in
				  WHERE (last_refreshed_at_utc >= (SELECT DATEADD('d',-90,max_updated) FROM watermark)
					 OR last_refreshed_at_utc IS NULL)
				  GROUP BY 
					capability_id,
					building_id,
					site_id, 
					trend_id,
					unit,
					sensor_type
				 ) ts
			JOIN transformed.date_hour d ON (d.date BETWEEN ts.min_date AND ts.max_date)
            ) 
		-- all possible left joined with actual
		,cte_all_leftjoin_actual AS (
				SELECT 
				 dt.building_id   AS expected_building_id,
				 dt.capability_id AS expected_capability_id,
				 dt.site_id  AS expected_site_id,
				 dt.trend_id AS expected_trend_id,
				 dt.expected_date,
				 dt.expected_date_hour,
                 m.date_time_local_hour,
				 m.trend_id,
				 m.site_id,
				 m.date_local,
				 m.end_of_hour_value,
				 -- grouping_level: NULL values in date_time_local_hour get assigned the same grouping level as the subsequent populated value
                 COUNT(date_time_local_hour) OVER (ORDER BY expected_trend_id,expected_date_hour ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as grouping_level,
				 COALESCE(m.avg_value_hour,LAG(m.avg_value_hour) IGNORE nulls OVER (PARTITION BY dt.trend_id ORDER BY dt.trend_id, dt.expected_date_hour)) AS avg_value_hour,
				 -- find next non-null value to use for averaging the missing values
				 COALESCE(m.hourly_usage,LEAD(m.hourly_usage) IGNORE nulls OVER (PARTITION BY dt.trend_id ORDER BY dt.trend_id, dt.expected_date_hour)) AS hourly_usage_prelim,
				 LAG(m.end_of_hour_value) IGNORE nulls OVER (PARTITION BY dt.trend_id ORDER BY dt.trend_id, dt.expected_date) AS end_of_prev_hour_value,
				 dt.unit,
				 dt.sensor_type,
				 m.last_captured_at_local,
				 m.last_captured_at_utc,
				 m.last_refreshed_at_utc,
				 m.last_enqueued_at_utc
				FROM cte_trendid_days_hours dt
					LEFT JOIN transformed.agg_electrical_metering_hourly m 
						   ON (dt.expected_date_hour = m.date_time_local_hour)
						  AND (dt.capability_id = m.capability_id)
				WHERE m.last_refreshed_at_utc >= (SELECT DATEADD('d',-90,max_updated) FROM watermark)
				   OR m.last_refreshed_at_utc IS NULL
				)
		-- identify the missing trend/hours across nulls
		,cte_avg_missing_values AS (
				SELECT 
					avg_value_hour,
					-- for non-missing values, count will be one since date_time_local_hour will have a value.  for missing values it will be the count of rows missing because date_time_local_hour is null
					COUNT(grouping_level) OVER (PARTITION BY expected_capability_id,grouping_level) AS divisor,
					LAST_VALUE( end_of_hour_value ) OVER ( PARTITION BY expected_capability_id, date_local ORDER BY expected_capability_id,expected_date_hour) AS end_of_day_value,
					COALESCE(end_of_prev_hour_value,end_of_hour_value) AS end_of_prev_hour_value,
					CASE WHEN (a.unit ilike 'kwh' AND a.hourly_usage_prelim > 20000) OR (a.unit ilike 'wh' AND a.hourly_usage_prelim > 20000000)THEN NULL ELSE hourly_usage_prelim END AS hourly_usage_prelim,
					a.expected_capability_id,
					a.expected_building_id,
					a.expected_trend_id,
					a.expected_site_id,
					a.expected_date_hour,
					a.date_time_local_hour,
					a.grouping_level,
					a.expected_date,
					a.date_local,
					CASE WHEN a.sensor_type = 'Energy' AND a.end_of_hour_value < end_of_prev_hour_value AND a.end_of_hour_value IS NOT NULL THEN end_of_prev_hour_value ELSE a.end_of_hour_value END AS end_of_hour_value,
					a.unit,
					a.sensor_type,
					a.last_captured_at_local,
					a.last_captured_at_utc,
					a.last_refreshed_at_utc,
					a.last_enqueued_at_utc
					FROM cte_all_leftjoin_actual a
				)
        -- final_results
		  SELECT
                expected_capability_id AS capability_id,
				expected_building_id AS building_id,
				expected_site_id AS site_id,
                expected_trend_id AS trend_id,
                expected_date AS date_local,
                expected_date_hour,
                sensor_type,
				unit,
                divisor,
       			CASE WHEN hourly_usage_prelim > 0 THEN hourly_usage_prelim/divisor ELSE NULL END AS hourly_usage_actual,
				CASE WHEN hourly_usage_prelim = 0 AND end_of_hour_value > 0 AND end_of_hour_value = end_of_prev_hour_value THEN .0000000001 ELSE NULL END AS hourly_usage_zero_handling,
				COALESCE(hourly_usage_zero_handling,hourly_usage_actual) AS hourly_usage,
                CASE WHEN divisor > 1 THEN 1 ELSE 0 END AS is_averaged_across_missing_values,
                avg_value_hour,
                end_of_hour_value,
                end_of_prev_hour_value,
                end_of_day_value,
                hourly_usage_prelim,
				last_captured_at_local,
				last_captured_at_utc,
				last_refreshed_at_utc,
				last_enqueued_at_utc
		  FROM cte_avg_missing_values
;