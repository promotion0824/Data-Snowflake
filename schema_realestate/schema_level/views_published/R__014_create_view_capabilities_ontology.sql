-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.capabilities_ontology AS
	SELECT 
		ca.capability_name,
		ca.model_id_capability,
		ca.capability_type,
		ca.trend_id,
		ca.trend_interval,
		ca.unit,
		ca.unique_id,
		ca.enabled,
		ca.description,
		ca.site_id,
		ca.id,
		ca.tags,
		space_id,
		space_name,
		space_type,
		ca.level_name,
		ca.level_id,
		ca.building_id,
		ca.zone_detail,
		ca.asset_id,
		ca.asset_name,
		ca.model_id_asset,
		ca.asset_detail,
		o.model_level_1 AS ontology_model_level_1,
		o.model_level_2 AS ontology_model_level_2,
		o.model_level_3 AS ontology_model_level_3,
		o.model_level_4 AS ontology_model_level_4,
		o.model_level_5 AS ontology_model_level_5,
		o.model_level_6 AS ontology_model_level_6,
		o.model_level_7 AS ontology_model_level_7,
		o.model_level_8 AS ontology_model_level_8,
		o.model_level_9 AS ontology_model_level_9,
		o.model_level_10 AS ontology_model_level_10
	FROM transformed.capabilities_assets_details ca
		LEFT JOIN transformed.ontology_buildings o 
			ON (ca.model_id_asset = o.id)
;
