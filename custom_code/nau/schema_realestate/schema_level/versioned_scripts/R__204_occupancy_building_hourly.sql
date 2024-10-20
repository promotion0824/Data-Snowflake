-- ------------------------------------------------------------------------------------------------------------------------------
-- create table
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transformed.occupancy_building_hourly (
	date_local DATE,
	datetime_local_hour TIMESTAMP_NTZ(9),
	day_name VARCHAR(16777216),
	day_of_week NUMBER(2,0),
	hour_num NUMBER(2,0),
	trend_id VARCHAR(50),
	external_id VARCHAR(255),
	end_of_hour_value INTEGER,
	previous_hour_value INTEGER,
	hourly_incremental INTEGER,
	is_weekday BOOLEAN,
	last_captured_at_local TIMESTAMP_NTZ(9),
	last_refreshed_at_utc TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);