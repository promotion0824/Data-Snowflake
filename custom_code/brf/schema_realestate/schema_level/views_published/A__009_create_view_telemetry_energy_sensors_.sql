-- ******************************************************************************************************************************
-- Create view for Overall Occupancy dashboard
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.telemetry_energy_sensors AS
SELECT  top 100
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
    tenant.tenant_name,
    tenant.tenant_id,
    tenant.tenant_unit_id,
    tenant.tenant_unit_name,
    tenant.relationship_name
FROM transformed.time_series_enriched ts
JOIN transformed.capabilities_assets ca
  ON (ts.trend_id = ca.trend_id)
LEFT JOIN transformed.tenant_served_by_twin tenant
  ON (ca.asset_id = tenant.asset_id)
WHERE
      ca.model_id_asset IN ('dtmi:com:willowinc:ElectricalPanelboard;1',
        'dtmi:com:willowinc:ElectricalPanelboardMCB;1',
        'dtmi:com:willowinc:ElectricalPanelboardMLO;1')
  AND ca.model_id IN ('dtmi:com:willowinc:CurrentSensor;1','dtmi:com:willowinc:VoltageSensor;1','dtmi:com:willowinc:FrequencySensor;1','dtmi:com:willowinc:StatusSensor;1')
  AND ts.telemetry_value IS NOT NULL
;
