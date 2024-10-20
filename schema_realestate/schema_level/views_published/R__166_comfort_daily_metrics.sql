-- --------------------------------------------------------------------------------------------
-- Create view that provides data for enterprise comfort dashboard
-- --------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.comfort_daily_metrics AS
	WITH cte_assets AS (
		SELECT DISTINCT
			a.asset_id,
			a.asset_name,
			a.excludedFromComfortAnalytics,
			a.building_id,
			a.building_name,
			a.zone_id,
			a.zone_name,
			a.room_id,
			a.room_name,
			a.level_name,
			a.floor_sort_order,
			a.unit,
			a.site_id,
			a.time_zone
		FROM transformed.comfort_assets a
	)
      , cte_scores AS (
         SELECT 
			scores.asset_id,
			scores.date,
			scores.is_working_hour,
			CASE WHEN scores.day_of_week_type = 'Weekday' THEN 1 ELSE 0 END AS is_weekday,
			AVG(scores.avg_zone_air_temp) AS avg_zone_air_temp,
			MIN(scores.min_setpoint_used) AS heating_setpoint_used,
			MAX(scores.max_setpoint_used) AS cooling_setpoint_used,
			scores.setpoint_type,
			scores.offset_type_used,
			SUM(scores.sample_count) AS sum_sample_count,
			SUM(scores.count_optimum) AS sum_count_optimum,
			ROUND(sum_count_optimum / sum_sample_count, 10) * 100 AS comfort_score,
			MAX(scores.heating_offset_used)	AS heating_offset_used,
			MAX(scores.cooling_offset_used)	AS cooling_offset_used,
            MAX(scores.last_captured_at_local) AS last_captured_at_local,
            MAX(scores.last_captured_at_utc) AS last_captured_at_utc,
            MAX(scores._last_updated_at) AS last_updated_at_utc,
		  FROM transformed.comfort_hourly_metrics  scores
          GROUP BY asset_id, scores.date, scores.is_working_hour, is_weekday, setpoint_type, offset_type_used
	  )
		  SELECT 
			scores.asset_id,
			a.asset_name,
			a.excludedFromComfortAnalytics,
			a.building_id,
			a.building_name,
			a.site_id,
			a.zone_id,
			a.zone_name,
			a.room_id,
			a.room_name,
			a.level_name,
			a.floor_sort_order,
			scores.date,
			a.unit,
			scores.last_captured_at_local,
			scores.is_working_hour,
			scores.is_weekday,
			scores.avg_zone_air_temp,
			scores.sum_sample_count,
			scores.sum_count_optimum,
			scores.comfort_score,
			scores.heating_setpoint_used,
			scores.cooling_setpoint_used,
			scores.setpoint_type,
			scores.offset_type_used,
			scores.heating_offset_used,
			scores.cooling_offset_used,
			scores_month_ago.comfort_score AS comfort_score_1_month_ago,
            MAX(scores.last_updated_at_utc) OVER () AS last_refreshed_at_utc,
			CONVERT_TIMEZONE( 'UTC',a.time_zone, last_refreshed_at_utc) AS last_refreshed_at_local
		  FROM cte_scores  scores
			JOIN cte_assets a 
			  ON (scores.asset_id = a.asset_id) 
		  LEFT JOIN cte_scores scores_month_ago
				   ON (scores.asset_id = scores_month_ago.asset_id)
				  AND (DATEADD('month',-1,scores.date) = scores_month_ago.date)
                  AND scores.is_working_hour = scores_month_ago.is_working_hour
          ;