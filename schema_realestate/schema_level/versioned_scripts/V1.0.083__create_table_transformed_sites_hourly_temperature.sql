-- ******************************************************************************************************************************
-- Create transformed table
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS transformed.sites_hourly_temperature (
    station_id 			VARCHAR(50),
	date_hour 			TIMESTAMP_NTZ,
    date 				DATE,
    temperature 		DECIMAL(12,1),
    site_id 			VARCHAR(36),
    site_name 			VARCHAR(255),
    longitude 			DECIMAL(9,6),
    latitude 			DECIMAL(9,6),
    temperature_unit	VARCHAR(50),
    customer_id 		VARCHAR(50),
	_last_updated_at	TIMESTAMP_NTZ
);
