-- ------------------------------------------------------------------------------------------------------------------------------
-- Create rolling 7 day view for performance engineering
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.telemetry_twins_rolling_7_days AS
  SELECT *
  FROM transformed.telemetry_twins_rolling_7_days
;