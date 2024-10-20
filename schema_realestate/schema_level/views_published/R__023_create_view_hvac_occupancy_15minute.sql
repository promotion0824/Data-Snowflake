-- ******************************************************************************************************************************
-- Create view published.hvac_occupancy_15minute 
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.hvac_occupancy_15minute AS
	SELECT 
		ts.trend_id,
		o.site_id,
		ts.date_local,
		ts.time_local_15min,
		ts.date_time_local_15min,
		ts.avg_value_15minute,
		ts.min_value_15minute,
		ts.max_value_15minute,
		ts.last_value_15minute,
		ts.last_value_15minute - LAG(ts.last_value_15minute, 1, 0) OVER (PARTITION BY ts.trend_id,date_local ORDER BY ts.trend_id, ts.time_local_15min,ts.last_value_15minute) AS diff_to_prev,
		o.capability_name,
		o.model_id,
		o.capability_type,
		o.capablity_tags,
		o.equipment_id,
		o.equipment_name,
		o.equipment_model_id,
		o.equipment_tags,
		o.space_id,
		o.space_name,
		o.space_type,
		o.capacity,
		o.usable_area,
		o.level_name,
		o.level_id,
		o.building_id,
		o.space_detail,
		o.building_detail,
		o.model_id AS model_id_sensor  --legacy FOR 1MW Rigado existing dashboard
	FROM transformed.agg_hvac_occupancy_15minute ts
		JOIN transformed.capabilities_hvac_occupancy o 
			ON (ts.site_id = o.site_id AND ts.trend_id = o.trend_id)
;
