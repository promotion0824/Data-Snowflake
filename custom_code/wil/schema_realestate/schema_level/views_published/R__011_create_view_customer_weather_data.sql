

CREATE OR REPLACE VIEW published.customer_weather_data AS
SELECT 
 w.station_id,
 w.date,
 w.cdd,
 w.hdd,
 s.site_id,
 s.site_name,
 s.longitude,
 s.latitude,
 s.temperature_unit,
 s.temperature_threshold,
 s.customer_id,
 w._last_updated_at
FROM transformed.sites_stations s
JOIN transformed.weather_data w on s.station_id = w.station_id
;