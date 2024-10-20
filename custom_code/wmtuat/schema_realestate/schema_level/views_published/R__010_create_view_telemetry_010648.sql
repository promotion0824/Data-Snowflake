-- ------------------------------------------------------------------------------------------------------------------------------
-- Create rolling 7 day view for performance engineering
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE SECURE VIEW published.telemetry_010648 AS
WITH cte_lucernix AS (
	SELECT site_id, building_id, building_name, custom_properties:externalIds.Real_Estate_ID::STRING as LucernixId
	FROM transformed.buildings
	WHERE LucernixId = 010648
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
  ON (ca.site_id = l.site_id)
WHERE 
     model_id_asset in (
	'dtmi:com:willowinc:ComputerRoomAirConditioningUnit;1',
	'dtmi:com:willowinc:ComputerRoomAirHandlingUnit;1',
	'dtmi:com:willowinc:RooftopUnit;1',
	'dtmi:com:willowinc:VAVBoxReheat;1',
	'dtmi:com:willowinc:VAVBox;1',
	'dtmi:com:willowinc:Door;1'
)
--;  GRANT SELECT ON VIEW published.telemetry_010648 TO SHARE external_share 
;
