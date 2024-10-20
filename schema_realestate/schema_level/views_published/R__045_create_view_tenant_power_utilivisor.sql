-- ------------------------------------------------------------------------------------------------------------------------------
-- Scenario 1 for DemAND calculation
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.tenant_power_utilivisor AS 
    WITH cte_tenant_account AS (
        SELECT DISTINCT
            tr.source_twin_id AS tenant_account_id,
            tr.relationship_name,
            tt.twin_id AS tenant_id,
            tt.name AS tenant_name,
			      t.site_id
        FROM transformed.twins t
        JOIN transformed.twins_relationships tr ON t.twin_id = tr.source_twin_id
        JOIN transformed.twins tt ON tr.target_twin_id = tt.twin_id
        WHERE (tr.relationship_name IN ('isHeldBy'))
          AND (tt.model_id = 'dtmi:com:willowinc:Company;1')
		  AND IFNULL(t.is_deleted, false) = false
		  AND IFNULL(tt.is_deleted, false) = false
		  AND IFNULL(tr.is_deleted, false) = false
          )
SELECT 
    ts.date_local,
    ts.date_time_local_15min,
    ts.date_time_local_hour,
    SUM(ts.telemetry_value) AS power_consumption,
    ca.asset_id,
    tnt.tenant_id,
    tenant_name,
    ca.site_name,
    ca.site_id,
    ca.building_id,
    ca.building_name
FROM transformed.time_series_enriched ts
JOIN transformed.capabilities_assets ca ON ts.dt_id = ca.capability_id
JOIN cte_tenant_account tnt ON ca.asset_id = tnt.tenant_account_id
WHERE date_local >= '2022-12-01'
  AND ca.model_id IN ('dtmi:com:willowinc:ActiveElectricalPowerSensor;1','dtmi:com:willowinc:BilledActiveElectricaPower;1')
  AND ca.model_id_asset = 'dtmi:com:willowinc:UtilityAccount;1'
  AND ca.capability_id NOT ILIKE '%PWROUT%'
  AND ca.capability_id NOT ILIKE '%-SUPP-%'
GROUP BY ts.date_local, ts.date_time_local_15min, ts.date_time_local_hour, ca.asset_id, tnt.tenant_id, tenant_name, ca.site_name, ca.site_id, ca.building_id, ca.building_name
;
