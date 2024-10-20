---------------------------------------------------------------------------------------
-- Create view that provides data for enterprise comfort dashboard
-- ------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.chiller_delta_temp_15min AS
		WITH cte_assets AS (
	  SELECT DISTINCT
        assets.asset_id,
        assets.asset_name,
        assets.site_id, 
        sites.name AS site_name
	  FROM transformed.chiller_assets assets
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
			scores.unit,
			AVG(scores.avg_chiller_delta_temp) AS avg_delta_chilled_water_temp,
			MAX(CASE WHEN scores.count_run_sensor_on >= 1 THEN 1 ELSE 0 END) AS compressor_run_sensor
      FROM transformed.chiller_15mins  scores
    GROUP BY    scores.asset_id,
    			scores.date_local,
    			scores.date_time_local_15min,
    			scores.unit

	  )
		  SELECT 
			assets.asset_id,
			assets.asset_name,
			assets.site_id,
			assets.site_name,
			agg.date_local,
			agg.date_time_local_15min,
			agg.unit,
			agg.avg_delta_chilled_water_temp,
			agg.compressor_run_sensor
		  FROM cte_agg agg
			JOIN cte_assets assets 
			  ON (agg.asset_id = assets.asset_id) 
;