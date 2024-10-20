-- ******************************************************************************************************************************
-- Create view for Overall Occupancy dashboard
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.chiller_telemetry AS
SELECT DISTINCT
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
  ON (ts.site_id = ca.site_id)
 AND (ts.trend_id = ca.trend_id)
WHERE
	 (ca.model_level_4 = 'Chiller' 
   OR ca.model_id_asset = 'dtmi:com:willowinc:WaterCooledChiller;1'
   OR ca.model_id_asset = 'dtmi:com:willowinc:Chiller;1')
;