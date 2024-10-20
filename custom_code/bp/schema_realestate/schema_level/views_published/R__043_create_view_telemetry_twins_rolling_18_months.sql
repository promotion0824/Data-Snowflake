-- ------------------------------------------------------------------------------------------------------------------------------
-- Create rolling 7 day view for performance engineering
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.telemetry_twins_rolling_18_months AS
SELECT 
    ts.date_local,
    ts.timestamp_local,
    ts.timestamp_utc,
    ts.trend_id,
    ts.site_id,
    ts.telemetry_value,
    ts.date_time_local_15min,
    ts.date_time_local_hour,
    ca.asset_id,
    ca.asset_name,
    ca.capability_name,
    ca.model_id as model_id_capability,
    ca.model_id_asset
FROM transformed.time_series_enriched ts
JOIN transformed.capabilities_assets ca 
  ON (ts.trend_id = ca.trend_id)
WHERE ts.date_local >= DATEADD(MONTH,-18,CURRENT_DATE)
;