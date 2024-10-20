-- ------------------------------------------------------------------------------------------------------------------------------
-- Create aggregate table
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transformed.occupancy_divided_openings_hourly (
	date_local 			 DATE,
	date_time_local_hour TIMESTAMP_NTZ(9),
	trend_id 			 VARCHAR(36),
	telemetry_value 	 FLOAT,
	enqueued_at 		 TIMESTAMP_NTZ(9)
);
