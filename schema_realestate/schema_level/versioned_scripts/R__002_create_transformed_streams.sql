-- ------------------------------------------------------------------------------------------------------------------------------
-- Create streams
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE STREAM IF NOT EXISTS transformed.telemetry_str
    ON TABLE transformed.telemetry
    APPEND_ONLY = TRUE
    ;