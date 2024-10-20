----------------------------------------------------------------------------------
-- Create table for storing aggregates at hour level
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.agg_electrical_demand_hourly (
	site_id 				    VARCHAR(36),
	asset_id 				    VARCHAR(100),
	date_local 				    DATE,
	date_time_local_hour 	    TIMESTAMP_NTZ,
    daily_peak_demand_building 	FLOAT,
    is_peak_hour 		        BOOLEAN,
    building_peak_hour 		    TIMESTAMP_NTZ,
    hourly_power_consumption 	FLOAT,
    values_count                INTEGER,
	_created_at           	    TIMESTAMP_NTZ   NOT NULL    DEFAULT SYSDATE(),
	_created_by_task      	    VARCHAR(255),
	_last_updated_at	  	    TIMESTAMP_NTZ   NOT NULL    DEFAULT SYSDATE(),
	_last_updated_by_task 	    VARCHAR(255),
	last_captured_at_local	    TIMESTAMP_NTZ,
	last_captured_at_utc	    TIMESTAMP_NTZ,
    last_refreshed_at_utc	    TIMESTAMP_NTZ   NOT NULL    DEFAULT SYSDATE(),
	last_refreshed_at_local		TIMESTAMP_NTZ,
	last_enqueued_at_utc		TIMESTAMP_NTZ
);
