-- ------------------------------------------------------------------------------------------------------------------------------
-- Create building view
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE SECURE VIEW published.telemetry_30034 AS
WITH cte_lucernix AS (
	SELECT site_id, building_id, building_name, custom_properties:externalIds.Real_Estate_ID::STRING as LucernixId
	FROM transformed.buildings
	WHERE LucernixId = '030034'
)
SELECT 
	l.LucernixId AS lucernix_id,
	ts.date_local,
	ts.timestamp_local,
	ts.timestamp_utc,
	ts.enqueued_at,
	ts.trend_id,
	ca.site_id,
	ca.site_name,
	ca.building_id,
	ca.building_name,
	ts.telemetry_value,
	ts.date_time_local_15min,
	ts.date_time_local_hour,
	ca.asset_id,
    ca.asset_name,
	ca.capability_id,
    ca.capability_name,
    ca.model_id as model_id_capability,
    ca.model_id_asset
FROM transformed.time_series_enriched ts
JOIN transformed.capabilities_assets ca 
  ON (ts.trend_id = ca.trend_id)
JOIN cte_lucernix l
  ON (ca.building_id = l.building_id)
;

EXECUTE IMMEDIATE
$$
DECLARE 
	currentdb STRING;
BEGIN
	SELECT CURRENT_DATABASE() INTO :currentdb From dual;
	IF (currentdb='PRD_DB') THEN
		GRANT SELECT ON VIEW published.telemetry_30034 TO SHARE external_share;
	END IF;
END;
$$