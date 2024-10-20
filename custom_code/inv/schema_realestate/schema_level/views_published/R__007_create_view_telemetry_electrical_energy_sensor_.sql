-- ******************************************************************************************************************************
-- Create view for Overall Occupancy dashboard
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.telemetry_electrical_energy_sensor AS
SELECT
    ts.date_local,
    ts.timestamp_local,
    ts.timestamp_utc,
    ts.trend_id,
    ts.site_id,
    ca.site_name,
    ts.telemetry_value,
    ts.date_time_local_15min,
    ts.date_time_local_hour,
    ca.asset_id,
    ca.asset_name,
    ca.capability_name,
    ca.model_id as model_id_capability,
    ca.model_id_asset,
    tr.relationship_name, 
    t.model_id, 
    t.twin_id as meter_id,
    t.name as meter_name
FROM transformed.time_series_enriched ts
JOIN transformed.capabilities_assets ca 
  ON (ts.site_id = ca.site_id)
 AND (ts.trend_id = ca.trend_id)
JOIN transformed.twins_relationships_deduped tr
  ON (ca.id = tr.source_twin_id)
JOIN transformed.twins t
  ON tr.target_twin_id = t.twin_id
WHERE date_local >= '2020-03-23'
  AND ca.model_id = 'dtmi:com:willowinc:ActiveElectricalEnergySensor;1'
  AND ca.model_id_asset != 'dtmi:com:willowinc:Building;1'
  AND tr.relationship_name = 'hostedBy'
  AND t.model_id = 'dtmi:com:willowinc:ElectricalMeter;1'
  AND IFNULL(tr.is_deleted,false) = false
  AND IFNULL(t.is_deleted,false) = false
;