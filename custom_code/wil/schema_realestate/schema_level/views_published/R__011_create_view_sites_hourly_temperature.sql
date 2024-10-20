-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View 
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.site_station_hourly_temperature AS
SELECT 
	h.station_id,
	h.date_hour,
	DATE(h.date_hour) AS date,
	h.temperature,
	s.site_id,
	s.site_name,
	s.longitude,
	s.latitude,
	h.temperature_unit,
	s.customer_id,
	h._last_updated_at
FROM transformed.sites_stations s
	JOIN transformed.hourly_temperature h 
	  ON (s.station_id = h.station_id)
;