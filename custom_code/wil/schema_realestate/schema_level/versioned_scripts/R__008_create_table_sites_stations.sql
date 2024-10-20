-- ******************************************************************************************************************************
-- Create table
-- ******************************************************************************************************************************

CREATE OR REPLACE TABLE transformed.sites_stations (
	customer_id				VARCHAR(36),
	site_id					VARCHAR(36),
	site_name				VARCHAR(255),
	latitude				DECIMAL(9,6),
	longitude				DECIMAL(9,6),
	station_id				VARCHAR(50),
	temperature_unit		VARCHAR(50),
	temperature_threshold	VARCHAR(20),
    _last_updated_at		TIMESTAMP_NTZ
);