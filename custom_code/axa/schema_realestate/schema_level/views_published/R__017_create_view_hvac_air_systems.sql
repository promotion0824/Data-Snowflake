-- **********************************************************************************************************************************
-- Create view
-- May need to add new "pivoted" metrics when new adjusted_capability_name values are added to transformed.hvac_adjusted_capabilities
-- **********************************************************************************************************************************
  
CREATE OR REPLACE View published.hvac_air_systems AS 
	SELECT
		ts.timestamp_utc,
		ts.timestamp_local,
		ts.time_local_15min,
		ts.date_time_local_15min,
		CASE WHEN adjusted_capability_name = 'Outside Air Flow' THEN ts.telemetry_value ELSE NULL END AS outside_air_flow,
		CASE WHEN adjusted_capability_name = 'Unit Outside Air Flow' THEN ts.telemetry_value ELSE NULL END AS unit_outside_air_flow,
		CASE WHEN adjusted_capability_name = 'Actual Zone Temperature' THEN ts.telemetry_value ELSE NULL END AS actual_zone_temperature,
		CASE WHEN adjusted_capability_name = 'Return Air Temperature' THEN ts.telemetry_value ELSE NULL END AS return_air_temperature,
		CASE WHEN adjusted_capability_name = 'Outside CO2' THEN ts.telemetry_value ELSE NULL END AS outside_co2,
		CASE WHEN adjusted_capability_name = 'Return Air CO2' THEN ts.telemetry_value ELSE NULL END AS return_air_co2,
		CASE WHEN adjusted_capability_name = 'Supply Air Static Pressure Control' THEN ts.telemetry_value ELSE NULL END AS supply_air_static_pressue_control,
		CASE WHEN adjusted_capability_name = 'Supply Air Temperature' THEN ts.telemetry_value ELSE NULL END AS supply_air_temperature,
		CASE WHEN adjusted_capability_name = 'Zone CO2' THEN ts.telemetry_value ELSE NULL END AS zone_co2,
		c.unit,
		ts.site_id,
		ts.trend_id,
		c.trend_interval,
		c.model_name_capability,
		c.unique_id,
		c.enabled,
		c.capability_type,
		c.asset_id,
		c.asset_name,
		c.asset_name AS equipment_name,
		c.model_name_asset,
		c.asset_detail,
		c.space_id,
		c.space_name,
		c.space_type,
		c.level_name,
		c.level_id,
		c.level_code,
		c.ontology_model_level_4
	FROM transformed.time_series_enriched ts 
		JOIN transformed.hvac_adjusted_capabilities c 
		  ON (ts.site_id = c.site_id AND ts.trend_id = c.trend_id)
	WHERE c.adjusted_capability_name is not null
	  AND ts.timestamp_utc > DATEADD('d',-180,GETDATE())
;
