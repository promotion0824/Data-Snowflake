-- ------------------------------------------------------------------------------------------------------------------------------
-- Create daily aggregate
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.sites_daily_temperature AS
SELECT 
	date,
	is_weekday,
	AVG(temperature) AS avg_daily_temperature,
	site_id,
	site_name,
	temperature_unit,
    building_id,
    building_name
FROM published.sites_hourly_temperature
GROUP BY 
	date,
	is_weekday,
	site_id,
	site_name,
	temperature_unit,
    building_id,
    building_name
;