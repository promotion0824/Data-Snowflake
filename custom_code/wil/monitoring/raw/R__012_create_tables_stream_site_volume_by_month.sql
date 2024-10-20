----------------------------------------------------------------------------------
-- Create table for storing aggregates for finance
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE central_monitoring_db.raw.site_volume_by_month (
    month_start_date DATE,
    site_id VARCHAR(36),
    site_name VARCHAR(255),
    connector_id VARCHAR(36),
    count_of_telemetry_points NUMBER(12,0),
    count_of_rows NUMBER(12,0),
    building_id VARCHAR(100),
    building_name VARCHAR(255),
    type VARCHAR(100),
    gross_area NUMBER(15,2),
    rentable_area NUMBER(15,2),
    model_id VARCHAR(255),
    customer_abbreviation VARCHAR(50),
    snowflake_created_date TIMESTAMP_NTZ(9),
    _last_updated TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TABLE central_monitoring_db.transformed.site_volume_by_month (
    month_start_date DATE,
    site_id VARCHAR(36),
    site_name VARCHAR(255),
    connector_id VARCHAR(36),
    count_of_telemetry_points NUMBER(12,0),
    count_of_rows NUMBER(12,0),
    building_id VARCHAR(100),
    building_name VARCHAR(255),
    type VARCHAR(100),
    gross_area NUMBER(15,2),
    rentable_area NUMBER(15,2),
    model_id VARCHAR(255),
    customer_abbreviation VARCHAR(50),
    snowflake_created_date TIMESTAMP_NTZ(9),
    _last_updated TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE STREAM central_monitoring_db.raw.site_volume_by_month_str
    ON TABLE central_monitoring_db.raw.site_volume_by_month 
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;