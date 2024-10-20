-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE View published.hvac_sensor_reading AS
	WITH CTE AS (
		SELECT 
		ts.timestamp_utc,
		ts.timestamp_local,
		ts.telemetry_value,
		 -- to make this dynamic LAG/Lead, compare the timediff FROM previous record;   add a config param to siteConfig whether the site uses LAG OR Lead.
		CASE WHEN c.model_id LIKE '%sensor%' THEN LAG(ts.telemetry_value, 1) OVER (PARTITION BY c.asset_id ORDER BY ts.timestamp_utc,c.model_id) ELSE null END AS last_setpoint,
		CASE WHEN c.model_id LIKE '%sensor%' THEN ts.telemetry_value - LAG(ts.telemetry_value, 1) OVER (PARTITION BY c.asset_id ORDER BY ts.timestamp_utc,c.model_id) ELSE null END AS diff_from_setpoint,
		c.id AS capability_id,
		c.capability_name,
		c.trend_id,
		c.trend_interval,
		c.model_id,
		c.enabled,
		c.capability_type,
		c.description,
		c.asset_id,
		c.asset_detail
		FROM transformed.capabilities_assets c
		JOIN transformed.time_series_enriched ts ON c.trend_id = ts.trend_id
		WHERE (c.model_id ILIKE '%TemperatureSetpoint%' OR c.model_id ILIKE '%TemperatureSensor%')
	) 
	SELECT
		 c.timestamp_utc,
		 c.timestamp_local,
		 c.telemetry_value,
		 c.last_setpoint,
		 c.diff_from_setpoint,
		 c.capability_id,
		 c.capability_name,
		 c.trend_id,
		 c.trend_interval,
		 c.model_id,
		 c.enabled,
		 c.capability_type,
		 c.description,
		 c.asset_id,
		 a.equipment_name,
		 s.space_name,
		 l.level_name,
		 l.level_code,
		 l.site_id
	FROM CTE c
		JOIN transformed.hvac_equipment a 
			ON (c.asset_id = a.id)
		LEFT JOIN transformed.spaces_levels s 
			ON (a.space_id = s.id)
		LEFT JOIN transformed.levels_buildings l 
			ON (s.level_id = l.id)
	WHERE c.model_id ILIKE '%Sensor%' 
	ORDER BY c.asset_id, c.timestamp_utc
;