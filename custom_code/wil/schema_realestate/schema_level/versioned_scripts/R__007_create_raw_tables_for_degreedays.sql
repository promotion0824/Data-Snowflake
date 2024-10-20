-- ******************************************************************************************************************************
-- Create raw tables
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.json_weather_data(
  json_value 		VARIANT,
  _last_updated_at 	TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS raw.json_sites_stations(
  json_value 		VARIANT,
  _last_updated_at 	TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP()
);