-- ------------------------------------------------------------------------------------------------------------------------------
-- create View
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transformed.occupancy_space_telemetry (
	date_local DATE,
	valid_from TIMESTAMP_NTZ(9),
	valid_to TIMESTAMP_NTZ(9),
	timestamp_utc TIMESTAMP_NTZ(9),
	enqueued_at TIMESTAMP_NTZ(9),
	trend_id VARCHAR(100),
	external_id VARCHAR(255),
	telemetry_value FLOAT,
	last_refreshed_at_utc TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);