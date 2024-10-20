-- ******************************************************************************************************************************
-- Create site_daily_scores
-- ******************************************************************************************************************************

CREATE OR REPLACE TABLE transformed.site_daily_scores (
	site_id VARCHAR(36) NOT NULL,
	site_name VARCHAR(100),
	date DATE NOT NULL,
	is_weekday BOOLEAN,
	comfort_score NUMBER(5,2),
	energy_score NUMBER(5,2),
	overall_score NUMBER(5,2),
	_created_at TIMESTAMP_NTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)),
	_created_by_task VARCHAR(255),
	_last_updated_by_task VARCHAR(255),
	last_captured_at_local	TIMESTAMP_NTZ,
	last_captured_at_utc	TIMESTAMP_NTZ,
    last_refreshed_at_utc	TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE()
)
;


