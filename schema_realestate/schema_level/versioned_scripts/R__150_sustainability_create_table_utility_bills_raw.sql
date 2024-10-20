-- ******************************************************************************************************************************
-- Create table
-- ******************************************************************************************************************************

CREATE OR REPLACE TRANSIENT TABLE transformed.sustainability_utility_bills_raw (
	billing_period_start TIMESTAMP_NTZ(9),
	billing_period_end TIMESTAMP_NTZ(9),
	days_in_period NUMBER(9,0),
	telemetry_value FLOAT,
	avg_daily_value FLOAT,
	trend_id VARCHAR(36),
	external_id  VARCHAR(500),
	dt_id VARCHAR(500),
	last_captured_at_utc TIMESTAMP_NTZ(9),
	last_refreshed_at_utc TIMESTAMP_NTZ(9)
);