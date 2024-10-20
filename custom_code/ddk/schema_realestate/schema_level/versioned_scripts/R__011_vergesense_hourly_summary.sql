-- ******************************************************************************************************************************
-- create aggregate table
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS transformed.vergesense_hourly_summary (
	date_local 			DATE, 
	start_of_hour 		TIMESTAMP_NTZ, 
	trend_id 			VARCHAR(36), 
	capability_name 	VARCHAR(255), 
	average 			FLOAT, 
	minimum 			FLOAT, 
	maximum 			FLOAT, 
	type 				VARCHAR(7), 
	off_count 			NUMBER(13,0), 
	on_count 			NUMBER(13,0), 
	model_id 			VARCHAR(255), 
	asset_id 			VARCHAR(255), 
	site_id				VARCHAR(36), 
	_last_updated_at	TIMESTAMP_NTZ DEFAULT SYSDATE()
);