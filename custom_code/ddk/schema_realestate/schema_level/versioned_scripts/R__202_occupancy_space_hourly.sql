-- ------------------------------------------------------------------------------------------------------------------------------
-- create table
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transformed.occupancy_space_hourly (
	date_local DATE,
	datetime_local_hour TIMESTAMP_NTZ(9),
	day_name VARCHAR(16777216),
	day_of_week NUMBER(2,0),
	hour_num NUMBER(2,0),
	external_id VARCHAR(255),
	trend_id VARCHAR(50),
	occupancy_count FLOAT,
	is_weekday BOOLEAN,
	last_captured_at_ut TIMESTAMP_NTZ(9),
	last_refreshed_at_utc TIMESTAMP_NTZ(9)
);