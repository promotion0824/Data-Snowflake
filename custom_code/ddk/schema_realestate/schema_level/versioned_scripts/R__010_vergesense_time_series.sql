-- ******************************************************************************************************************************
-- create table to persist vergesense time series data
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS transformed.vergesense_time_series (
	 date_local 		DATE,
	 timestamp_local 	TIMESTAMP_NTZ, 
	 timestamp_utc 		TIMESTAMP_NTZ, 
	 trend_id 			VARCHAR(36), 
	 telemetry_value 	FLOAT, 
	 site_id 			VARCHAR(36), 
	 capability_name	VARCHAR(255), 
	 capability_id 		VARCHAR(255), 
	 start_of_hour 		TIMESTAMP_NTZ, 
	 analog_value 		FLOAT, 
	 on_count 			NUMBER(1,0), 
	 off_count 			NUMBER(1,0), 
	 model_id 			VARCHAR(255), 
	 asset_id 			VARCHAR(255), 
	_last_updated_at	TIMESTAMP_NTZ DEFAULT SYSDATE()
 );