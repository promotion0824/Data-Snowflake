-- ------------------------------------------------------------------------------------------------------------------------------
-- Create streams
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE STREAM IF NOT EXISTS central_monitoring_db.raw.site_volume_by_month_str
    ON TABLE central_monitoring_db.raw.site_volume_by_month
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;
