-- ******************************************************************************************************************************
-- create table to persist occupancy time series data
-- ******************************************************************************************************************************
CREATE TABLE IF NOT EXISTS transformed.occupancy_time_series (
	 date_local 			DATE,
	 date_time_local_hour	TIMESTAMP_NTZ,
	 date_time_local_15min	TIMESTAMP_NTZ,
	 timestamp_local 	TIMESTAMP_NTZ,
	 enqueued_at_utc	TIMESTAMP_NTZ,
	 trend_id 			VARCHAR(36),
	 telemetry_value 	FLOAT,
	 last_value_hour 	FLOAT,
	 site_id 			VARCHAR(36),
	 capability_name	VARCHAR(255),
	 capability_id 		VARCHAR(255),
	 start_of_hour 		TIMESTAMP_NTZ,
	 model_id 			VARCHAR(255),
	 asset_id 			VARCHAR(255),
	_last_updated_at	TIMESTAMP_NTZ DEFAULT SYSDATE()
 );