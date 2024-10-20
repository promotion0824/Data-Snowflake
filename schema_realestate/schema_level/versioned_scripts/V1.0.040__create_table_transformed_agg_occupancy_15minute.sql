-- ******************************************************************************************************************************
-- Create table transformed.agg_occupancy_15minute 
-- ******************************************************************************************************************************

CREATE OR REPLACE TABLE transformed.agg_occupancy_15minute (
	trend_id 				VARCHAR(36),
	site_id 				VARCHAR(36),
	date_local 				DATE,
	time_local_15min 		CHAR(5),
	date_time_local_15min 	TIMESTAMP_NTZ(9),
	avg_value_15minute 		FLOAT,
	min_value_15minute 		FLOAT,
	max_value_15minute 		FLOAT,
	last_value_15minute 	FLOAT,
	_created_at           TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
	_created_by_task      VARCHAR(255)        NULL,
	_last_updated_at      TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
	_last_updated_by_task VARCHAR(255)        NULL
);
