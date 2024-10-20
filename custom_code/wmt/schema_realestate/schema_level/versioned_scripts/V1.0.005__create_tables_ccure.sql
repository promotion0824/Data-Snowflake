-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables required for ccure aggregates
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transformed.ccure_time_series_15minute (
	site_id 				VARCHAR(36),
	trend_id 				VARCHAR(36),
	date_local 				DATE,
	time_local_15min 		VARCHAR(5),
	date_time_local_15min	TIMESTAMP_NTZ(9),
	last_value_15min 		NUMBER(18,6),
	_created_at				TIMESTAMP_NTZ	 NOT NULL		DEFAULT SYSDATE(),
	_created_by_task		VARCHAR(255)		 NULL,
	_last_updated_at		TIMESTAMP_NTZ	 NOT NULL		DEFAULT SYSDATE(),
	_last_updated_by_task VARCHAR(255)		     NULL
);

CREATE TABLE IF NOT EXISTS transformed.ccure_trend_id_15minute (
	site_id 				VARCHAR(36),
	trend_id 				VARCHAR(36),
	date_local 				DATE,
	time_local_15min 		VARCHAR(5),
	date_time_local_15min	TIMESTAMP_NTZ(9),
	_created_at				TIMESTAMP_NTZ	 NOT NULL		DEFAULT SYSDATE(),
	_created_by_task		VARCHAR(255)		 NULL,
	_last_updated_at		TIMESTAMP_NTZ	 NOT NULL		DEFAULT SYSDATE(),
	_last_updated_by_task VARCHAR(255)		     NULL
);

CREATE TABLE IF NOT EXISTS transformed.ccure_15minute (
	site_id 				VARCHAR(36),
	trend_id 				VARCHAR(36),
	date_local 				DATE,
	time_local_15min 		VARCHAR(5),
	date_time_local_15min	TIMESTAMP_NTZ(9),
	is_business_hours		VARCHAR(36),
	last_15min_value 		NUMBER(18,6),
	prev_15min_value 		NUMBER(18,6),
	diff_to_prev 			NUMBER(18,6),
	_created_at				TIMESTAMP_NTZ	 NOT NULL		DEFAULT SYSDATE(),
	_created_by_task		VARCHAR(255)		 NULL,
	_last_updated_at		TIMESTAMP_NTZ	 NOT NULL		DEFAULT SYSDATE(),
	_last_updated_by_task VARCHAR(255)		     NULL
);

