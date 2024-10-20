-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.capabilities_assets_details_v AS
WITH
	asset_level AS (
				SELECT  DISTINCT
					 id AS asset_id,
					 h.space_detail:id::string AS level_id,
					 h.space_detail:customProperties.name::string AS level_name,
					 h.space_detail
				FROM transformed.hvac_equipment h
				WHERE h.space_detail:modelId::string ILIKE '%Level%'
				QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY level_id desc) = 1
				),
	asset_space AS (
				SELECT DISTINCT 
					 id AS asset_id,
					 h.space_detail:id::string AS space_id,
					 h.space_detail:customProperties.name::string AS space_name,
					 SPLIT_PART(REPLACE(h.space_detail:modelId::string, ';',':'),':',4) AS space_type,
					 h.space_detail
				FROM transformed.hvac_equipment h
				WHERE h.space_detail:modelId::string ILIKE ANY ('%Room%','%Zone%')
				QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY space_type) = 1
				)
  SELECT
		ca.id AS capability_id,
		ca.capability_name,
		ca.model_id AS model_id_capability,
		SPLIT_PART(REPLACE(model_id_capability, ';',':'),':',4) AS model_name_capability,
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
		ca.tags_string,
		COALESCE(a.space_id, cz.zone_id) AS space_id,
		COALESCE(a.space_name,cz.zone_name,s.space_name,s2.space_name) AS space_name,
		COALESCE(a.space_type,split_part(replace(cz.zone_detail:modelId::string, ';',':'),':',4)) AS space_type,
		s.space_capacity AS capacity,
		s.usable_area_space AS usable_area,
		ca.asset_id,
		ca.asset_name,
		ca.asset_detail:modelId::STRING AS model_id_asset,
		SPLIT_PART(REPLACE(model_id_asset, ';',':'),':',4) AS model_name_asset,
		ca.asset_detail,
		COALESCE(al.level_name,l.level_name) AS level_name,
		COALESCE(l.id,s.level_id,s2.level_id,al.level_id) AS level_id,
		COALESCE(a.space_detail,al.space_detail) AS space_detail,
		l.level_code,
		l.building_id,
		cz.zone_detail,
		l.building_detail
	FROM transformed.capabilities_assets ca
		LEFT JOIN transformed.capabilities_zones cz 
		       ON (ca.unique_id = cz.unique_id)
		LEFT JOIN transformed.spaces_levels s 
			   ON (cz.zone_id = s.id)
		LEFT JOIN asset_space a 
			   ON (ca.asset_id = a.asset_id)
		LEFT JOIN transformed.spaces_levels s2 
			   ON (a.space_id = s2.id)
		LEFT JOIN asset_level al 
			   ON (ca.asset_id = al.asset_id)
		LEFT JOIN transformed.levels_buildings l 
			   ON (l.id = COALESCE(al.level_id,s.level_id,s2.level_id))
;

CREATE OR REPLACE TABLE transformed.capabilities_assets_details AS SELECT * FROM transformed.capabilities_assets_details_v;