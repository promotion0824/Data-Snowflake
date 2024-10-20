-- ******************************************************************************************************************************
-- Create transformed table
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS transformed.hourly_temperature (
    station_id varchar(50),
	date_hour TIMESTAMP_NTZ,
    temperature DECIMAL(12,1),
    temperature_unit varchar(50),
	_created_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at TIMESTAMP_NTZ DEFAULT SYSDATE()
);
