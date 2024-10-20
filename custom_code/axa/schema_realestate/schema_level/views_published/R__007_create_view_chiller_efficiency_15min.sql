---------------------------------------------------------------------------------------
-- Create view that provides data for enterprise comfort dashboard
-- ------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.chiller_efficiency_15min AS
	WITH cte_assets AS (
	  SELECT DISTINCT
        assets.asset_id,
        assets.asset_name,
        assets.site_id, 
        sites.name AS site_name
	  FROM transformed.chiller_efficiency_sensors assets
			JOIN transformed.sites
			  ON (assets.site_id = sites.site_id)
			LEFT JOIN transformed.levels_buildings floors
			  ON (assets.floor_id = floors.floor_id)
    )
    ,cte_agg AS (
	  SELECT
            scores.asset_id,
			scores.date_local,
			scores.date_time_local_15min,
			TRUNC(scores.date_time_local_15min,'HOUR') AS date_time_local_hour,
			scores.sensor_type,
			scores.unit,
			AVG(scores.avg_sensor_value) AS avg_sensor_value,
			MAX(CASE WHEN scores.count_run_sensor_on >= 1 THEN 1 ELSE 0 END) AS compressor_run_sensor
      FROM transformed.chiller_efficiency_15min  scores
    GROUP BY    scores.asset_id,
    			scores.date_local,
    			scores.date_time_local_15min,
				scores.sensor_type,
    			scores.unit

	  )
		  SELECT 
			assets.asset_id,
			assets.asset_name,
			assets.site_id,
			assets.site_name,
			agg.date_local,
			agg.date_time_local_15min,
			agg.sensor_type,
			agg.unit,
			agg.avg_sensor_value,
			agg.compressor_run_sensor
		  FROM cte_agg agg
			JOIN cte_assets assets 
			  ON (agg.asset_id = assets.asset_id) 
;