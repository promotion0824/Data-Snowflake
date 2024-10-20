-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables required for ccure aggregates
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transformed.time_series_occupancy (
	site_id 				VARCHAR(36),
	trend_id 				VARCHAR(36),
	date_local 				DATE,
	date_time_local_hour	TIMESTAMP_NTZ(9),
	sum_telemetry_value		INTEGER,
	_created_at				TIMESTAMP_NTZ	 NOT NULL		DEFAULT SYSDATE(),
	_created_by_task		VARCHAR(255)		 NULL,
	_last_updated_at		TIMESTAMP_NTZ	 NOT NULL		DEFAULT SYSDATE(),
	_last_updated_by_task VARCHAR(255)		     NULL
);

