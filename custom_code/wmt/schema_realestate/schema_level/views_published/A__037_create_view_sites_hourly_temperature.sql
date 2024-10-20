-- ------------------------------------------------------------------------------------------------------------------------------
-- Create view - sourcedfrom weatherBit
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW published.sites_hourly_temperature AS

SELECT 
    ts.date_local AS date,
	DATE_TRUNC('HOUR',ts.timestamp_local) AS date_hour,
    d.is_weekday,
    IFF(HOUR(date_hour) >= COALESCE(working_hours.default_value:hourStart,8) 
    AND HOUR(date_hour) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
    AVG(telemetry_value) AS temperature,
    ca.unit AS temperature_unit,
    COALESCE(b.site_id,ca.site_id) AS site_id,
    COALESCE(b.site_name,ca.site_name) AS site_name,
    COALESCE(b.building_id,ca.building_id) AS building_id,
    COALESCE(b.building_name,ca.building_name) AS building_name
FROM transformed.capabilities_assets ca
JOIN transformed.telemetry ts ON ca.trend_id = ts.trend_id
JOIN transformed.dates d ON ts.date_local = d.date
LEFT JOIN transformed.twins_relationships_deduped tr 
  ON ca.asset_id = tr.target_twin_id
 AND tr.relationship_name = 'servedBy'
LEFT JOIN transformed.buildings b
  ON tr.source_twin_id = b.building_id
LEFT JOIN transformed.site_defaults working_hours
     ON (ca.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
    AND (working_hours._valid_from <= ts.timestamp_local AND working_hours._valid_to >= ts.timestamp_local)
WHERE 
    ts.date_local >= '2024-01-01'
AND ca.model_id IN ('dtmi:com:willowinc:AirTemperatureSensor;1')
AND IFNULL(tr.is_deleted,false) = false
GROUP BY
	ts.date_local,
    date_hour,
    d.is_weekday,
    ca.unit,
    COALESCE(b.site_id,ca.site_id),
    COALESCE(b.site_name,ca.site_name),
    COALESCE(b.building_id,ca.building_id),
    COALESCE(b.building_name,ca.building_name),
	working_hours.default_value:hourStart,
	working_hours.default_value:hourEnd
;