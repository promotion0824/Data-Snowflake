-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.billed_electricity AS
SELECT 
        ts.date_local,
        ts.timestamp_local,
        ts.telemetry_value,
        ts.capability_id,
        ca.capability_name,
        ts.trend_id,
        ca.model_id,
        ca.unit,
        ca.asset_id,
        ca.asset_name,
        ca.model_id_asset,
        ca.site_id,
        ca.site_name,
        ca.time_zone,
        ca.building_id,
        ca.building_name,
        CONVERT_TIMEZONE( 'UTC',ca.time_zone, ts._last_updated_at) AS last_refreshed_at_local
FROM transformed.billed_electricity ts
JOIN transformed.capabilities_assets ca ON ts.capability_id = ca.capability_id;