-- ******************************************************************************************************************************
-- create aggregate table
-- ******************************************************************************************************************************
CREATE TABLE IF NOT EXISTS transformed.vergesense_hourly_summary (
	date_local 			DATE, 
	date_time_local_hour TIMESTAMP_NTZ, 
	trend_id 			VARCHAR(36), 
	capability_name 	VARCHAR(255), 
	sum 				FLOAT,
	average 			FLOAT, 
	minimum 			FLOAT, 
	maximum 			FLOAT, 
	last_value_hour		NUMBER(13,0), 
	count 				NUMBER(13,0), 
	type 				VARCHAR(7), 
	model_id 			VARCHAR(255), 
	asset_id 			VARCHAR(255), 
	site_id				VARCHAR(36), 
	_last_updated_at	TIMESTAMP_NTZ DEFAULT SYSDATE()
);