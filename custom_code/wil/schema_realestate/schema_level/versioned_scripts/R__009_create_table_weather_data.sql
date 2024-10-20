-- ******************************************************************************************************************************
-- Create table in transformed
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS transformed.weather_data (
	station_id			VARCHAR(50),
	date				TIMESTAMP_NTZ,
	cdd					DECIMAL(8,1),
	hdd					DECIMAL(8,1),
    _last_updated_at	TIMESTAMP_NTZ
);
