---------------------------------------------------------------------------------------
-- Create view that provides data for enterprise comfort dashboard
-- ------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.return_air_temperature_hourly_metrics_score AS
WITH cte_assets AS (
      SELECT DISTINCT 
        assets.asset_id,
        assets.asset_name,
        assets.floor_id,
		assets.building_id,
		assets.building_name,
        assets.site_id, 
        sites.name AS site_name,
		sites.time_zone,
        floors.level_name AS floor_name,
        floors.level_number,
        floors.floor_sort_order,
        assets.category_name,
		assets.excludedFromComfortAnalytics
        FROM transformed.return_air_temperature_assets assets 
        JOIN transformed.sites
          ON (assets.site_id = sites.site_id)
        LEFT JOIN transformed.levels_buildings floors
          ON (assets.floor_id = floors.floor_id)
  )
  SELECT 
	assets.asset_id,
	assets.asset_name,
	assets.building_id,
	assets.building_name,
	assets.site_id, 
	assets.site_name,
	assets.time_zone,
	assets.floor_id,
	assets.floor_name,
	assets.level_number,
	assets.floor_sort_order,
	assets.category_name,
	assets.excludedFromComfortAnalytics,
	scores.date,
	scores.date_hour_start,
	scores.last_captured_at_local,
	scores.is_working_hour,
	scores.day_of_week_type,
	scores.avg_return_air_temperature,
	scores.avg_return_air_temperature_sp,
	(CASE WHEN ABS(scores.deviation) <= 4.1 THEN 1 ELSE 0 END) AS count_optimum_temp,
	scores.avg_return_air_humidity, 
	scores.avg_return_air_temperature - (9/25)*(100 - scores.avg_return_air_humidity) AS avg_dew_point,
	(CASE WHEN IFNULL(avg_dew_point,1) <= 62 THEN 1 ELSE 0 END) AS count_optimum_humidity,
	-- only count humidity for 1MW;
	(CASE WHEN scores.site_id = '4e5fc229-ffd9-462a-882b-16b4a63b2a8a' THEN count_optimum_humidity ELSE NULL END) AS adj_humidity_count,
	scores.sample_count,
	scores.last_captured_at_utc AS last_captured_at_utc,
	scores._last_updated_at AS last_refreshed_at_utc
  FROM transformed.return_air_temperature_hourly_metrics scores 
	JOIN cte_assets assets
	  ON (scores.asset_twin_id = assets.asset_id)
	LEFT JOIN transformed.site_defaults default_setpoints
	  ON (
			assets.site_id = default_setpoints.site_id 
		AND default_setpoints.type = 'ComfortDataStartDate' 
		AND default_setpoints._valid_from <= scores.date 
		AND default_setpoints._valid_to >= scores.date
	  )
  WHERE scores.date >= IFNULL(default_setpoints.default_value:SiteStartDate,'2019-01-01');