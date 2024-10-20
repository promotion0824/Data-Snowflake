----------------------------------------------------------------------------------
-- Create table for storing aggregates at hour level
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.agg_electrical_metering_hourly (
	capability_id 			VARCHAR(100),
	building_id 			VARCHAR(36),
	trend_id 				VARCHAR(36),
	site_id 				VARCHAR(36),
	date_local 				DATE,
	date_time_local_hour 	TIMESTAMP_NTZ,
    avg_value_hour 		    FLOAT,
    min_value_hour 		    FLOAT,
    max_value_hour 		    FLOAT,
    values_count            INTEGER,
    end_of_hour_value 		FLOAT,
    end_of_prev_hour_value	FLOAT,
    hourly_usage			FLOAT,
	unit					VARCHAR(100),
	sensor_type				varchar(36),
	_created_at           	TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
	_created_by_task      	VARCHAR(255),
	_last_updated_by_task 	VARCHAR(255),
	last_captured_at_local	TIMESTAMP_NTZ,
	last_captured_at_utc	TIMESTAMP_NTZ,
    last_refreshed_at_utc	TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
	last_enqueued_at_utc	TIMESTAMP_NTZ
);