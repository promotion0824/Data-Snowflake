-- ******************************************************************************************************************************
-- Create raw table
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.sites_weather_data (
    station_id 			VARCHAR(50),
    date 				DATE,
    cdd 				DECIMAL(12,1),
    hdd 				DECIMAL(12,1),
    site_id 			VARCHAR(36),
    site_name 			VARCHAR(255),
    longitude 			DECIMAL(9,6),
    latitude 			DECIMAL(9,6),
    temperature_unit	VARCHAR(50),
    temperature_threshold DECIMAL(12,1),
	_last_updated_at	TIMESTAMP_NTZ DEFAULT SYSDATE()
);
