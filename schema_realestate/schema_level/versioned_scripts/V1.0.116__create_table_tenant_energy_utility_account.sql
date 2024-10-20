----------------------------------------------------------------------------------
-- Create table for storing aggregates at hour level
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.tenant_energy_utility_account (
	date_local DATE,
	date_time_local_hour TIMESTAMP_NTZ(9),
	date_time_local_15min TIMESTAMP_NTZ(9),
	day_of_week NUMBER(2,0),
	is_weekday BOOLEAN,
	day_of_week_type VARCHAR(7),
	power_consumption FLOAT,
	asset_id VARCHAR(255),
	asset_name VARCHAR(255),
	tenant_id VARCHAR(255),
	tenant_name VARCHAR(255),
	tenant_unit_id VARCHAR(3),
	tenant_unit_name VARCHAR(3),
	tenant_unit_rentable_area FLOAT,
	site_id VARCHAR(100),
	site_name VARCHAR(100),
	customer_id VARCHAR(36),
	portfolio_id VARCHAR(36),
	building_id VARCHAR(255),
	building_name VARCHAR(255),
	building_gross_area NUMBER(36,2),
	building_rentable_area NUMBER(36,2),
	building_type VARCHAR(100),
	last_captured_at_local TIMESTAMP_NTZ(9),
	last_captured_at_utc TIMESTAMP_NTZ(9),
	last_refreshed_at_utc TIMESTAMP_NTZ(9),
	last_refreshed_at_local TIMESTAMP_NTZ(9)
);