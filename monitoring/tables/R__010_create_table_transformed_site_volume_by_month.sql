----------------------------------------------------------------------------------
-- Create table for storing aggregates for finance
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS monitoring_db.transformed.site_volume_by_month (
    month_start_date DATE,
    site_id VARCHAR(36),
    connector_id VARCHAR(36),
    count_of_telemetry_points NUMBER(12,0),
    count_of_rows NUMBER(12,0)
);
