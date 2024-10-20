-- ******************************************************************************************************************************
-- Create view for Overall Occupancy dashboard
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.hvac_systems AS
	SELECT
		ts.timestamp_utc,
		ts.timestamp_local,
		ts.time_local_15min,
		ts.date_time_local_15min,
		DAYOFWEEKISO(ts.date_local) AS day_of_week,
		CASE WHEN day_of_week > 5 THEN false ELSE true END AS is_weekday,
		IFF(is_weekday = TRUE, 'Weekday', 'Weekend') AS day_of_week_type,
		ts.telemetry_value,
		c.capability_name,
		c.capability_id,
		c.adjusted_capability_name,
		c.unit,
		c.site_id,
		c.trend_id,
		c.trend_interval,
		c.model_id_capability,
		c.model_name_capability,
        c.tags_capability,
		c.unique_id,
		c.enabled,
		c.asset_id,
		c.asset_name,
		c.model_id_asset,
		c.model_name_asset,
		c.asset_detail,
        c.tags_asset,
		c.ontology_model_level_4,
		c.space_id,
		-- c.space_code,
		c.space_name,
		c.space_type,
		c.capacity,
		c.level_name,
		c.level_id,
		c.building_id
	FROM transformed.hvac_adjusted_capabilities c 
		JOIN transformed.time_series_enriched ts 
			ON (c.site_id = ts.site_id AND c.trend_id = ts.trend_id)
	WHERE 
        ts.site_id = 'e57539e0-d938-400a-b6b4-e9cb21dcc535'
    AND ts.trend_id IN (SELECT trend_id FROM transformed.hvac_adjusted_capabilities 
                        WHERE model_id_asset = 'dtmi:com:willowinc:FanCoilUnit;1'
                      AND  model_id_capability IN ('dtmi:com:willowinc:ModeSensor;1','dtmi:com:willowinc:ModeState;1')
                      AND capability_name ilike 'Occupation Synthèse%')
AND timestamp_utc >= DATEADD(month, -13, CURRENT_DATE())
;
