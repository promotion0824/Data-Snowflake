-- ------------------------------------------------------------------------------------------------------------------------------
-- Create streams
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE STREAM IF NOT EXISTS transformed.telemetry_str_occupancy
    ON TABLE transformed.telemetry
    APPEND_ONLY = TRUE
    ;