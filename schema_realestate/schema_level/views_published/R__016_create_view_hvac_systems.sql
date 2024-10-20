-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
  
CREATE OR REPLACE View published.hvac_systems AS 
	SELECT
		ts.timestamp_utc,
		ts.timestamp_local,
		ts.time_local_15min,
		ts.date_time_local_15min,
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
		c.unique_id,
		c.enabled,
		c.capability_type,
		c.description,
		c.tags_capability,
		c.asset_id,
		c.asset_name,
		c.tags_asset,
		c.model_id_asset,
		c.model_name_asset,
		c.asset_detail,
		c.space_id,
		c.space_name,
		c.space_type,
		c.capacity,
		c.usable_area,
		c.level_name,
		c.level_id,
		c.building_id,
		c.space_detail,
		c.building_detail,
		c.ontology_model_level_4,
		c.ontology_model_level_5,
		c.ontology_model_level_6
	FROM transformed.hvac_adjusted_capabilities c 
		JOIN transformed.time_series_enriched ts 
			ON (c.site_id = ts.site_id AND c.trend_id = ts.trend_id)
;
