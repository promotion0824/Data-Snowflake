----------------------------------------------------------------------------------
-- Create table for storing aggregates at hour level
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.agg_electrical_metering_daily (
	capability_id 		    VARCHAR(100),
	building_id 		    VARCHAR(36),
	trend_id 		      	VARCHAR(36),
	site_id 		      	VARCHAR(36),
	date_local 		      	DATE,
    sensor_type           	VARCHAR(36),
    daily_usage_kwh         FLOAT,
	virtual_daily_usage_kwh	FLOAT,
	end_of_day_kwh			FLOAT,
	end_of_prev_day_value_kwh FLOAT,
	daily_usage_kwh_EOD_calc FLOAT,
	_created_at           	TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
	_created_by_task      	VARCHAR(255)        NULL,
	_last_updated_by_task 	VARCHAR(255)        NULL,
	last_captured_at_local	TIMESTAMP_NTZ,
	last_captured_at_utc	TIMESTAMP_NTZ,
    last_refreshed_at_utc	TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
	last_enqueued_at_utc	TIMESTAMP_NTZ
);
