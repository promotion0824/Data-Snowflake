-- ------------------------------------------------------------------------------------------------------------------------------
-- Create rolling 7 day view for performance engineering
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.telemetry_hostedby_twins_rolling_6_months AS
  SELECT *
  FROM transformed.telemetry_hostedby_twins_rolling_6_months
;